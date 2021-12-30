"""Grab flood maps from Calgary open data and import to PostGIS."""
import json
from pathlib import Path
from typing import Dict

import requests

from wheretolive.logconf import get_logger
from wheretolive.postgis import PostGIS

logger = get_logger("floodzones")


def download_flood_map(pct: int = 100) -> Path:
    """Download the 1 in 100 or 1 in 20 year flood map."""
    if pct == 100:
        url = "https://data.calgary.ca/api/geospatial/w8wn-kuii?method=export&format=GeoJSON"
    elif pct == 20:
        url = "https://data.calgary.ca/api/geospatial/iyqi-dvvj?method=export&format=GeoJSON"
    else:
        raise ValueError(f"Download not implemented for 1 in {pct} chance flood.")
    logger.info(f"Downloading {pct} flood map.")
    response = requests.get(url, stream=True, headers={"user-agent": None})
    outdir = Path(__file__).resolve().parents[2] / "data" / "calgary_open"
    outdir.mkdir(exist_ok=True)
    outfile = outdir / f"calgary_flood_1_in_{pct}.json"
    with open(outfile, "wb") as handle:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                handle.write(chunk)
    return outfile


def get_map(pct: int) -> Dict:
    """Download map if missing, get the path to it either way."""
    outdir = Path(__file__).resolve().parents[2] / "data" / "calgary_open"
    outfile = outdir / f"calgary_flood_1_in_{pct}.json"
    if not outfile.exists():
        download_flood_map(pct=pct)
    with open(outfile, "r") as f:
        floodmap = json.load(f)
    logger.info(f"Loaded {pct} flood map.")
    return floodmap


def _insertion_syntax(feature, scenario):
    """Turn a geoJSON polygon feature into an insertion statement for staging."""
    geo_shape = feature["geometry"]
    geo_str = f"ST_GeomFromGeoJSON('{json.dumps(geo_shape)}')"
    sql = f"""
        INSERT INTO public.flood_staging (scenario, geom)
        VALUES ('{scenario}', {geo_str})
    """
    return sql


def stage_map(pct: int) -> None:
    """Push a map into PostGIS and do the necessary transformations."""
    floodmap = get_map(pct)
    scenario = f"1 in {pct} chance flood"
    flood_staging = """
    DROP TABLE IF EXISTS public.flood_staging;

    CREATE TABLE public.flood_staging (
        scenario VARCHAR(50),
        geom geometry(Polygon, 4326)
    );
    """

    with PostGIS().connection.begin() as conn:
        conn.execute(flood_staging)
        logger.info(f"Starting record insertion for {pct}")
        for feature in floodmap["features"]:
            sql = _insertion_syntax(feature, scenario)
            conn.execute(sql)
        logger.info(f"Finished record insertion for {pct}")


def union_map(year: int, start_fresh: bool = False) -> None:
    """Union all the polygons into one giant multipolygon.

    Note, this takes a few minutes to run.
    """
    dropsql = "DROP TABLE IF EXISTS public.floodmap;"
    createsql = """
    CREATE TABLE IF NOT EXISTS public.floodmap (
        scenario VARCHAR(50),
        geom geometry(MultiPolygon, 4326)
    );
    """
    insertsql = """
    INSERT INTO public.floodmap (scenario, geom)
    SELECT
        scenario,
        ST_Union(geom) as geom
    FROM public.flood_staging
    GROUP BY scenario;
    """
    with PostGIS().connection.begin() as conn:
        logger.info("Starting aggregation for staged data.")
        if start_fresh:
            conn.execute(dropsql)
        conn.execute(createsql)
        conn.execute(insertsql)
        logger.info("Finished aggregation for staged data.")


def main():
    """Load the flood maps."""
    pct = 100
    stage_map(pct)
    union_map(pct, start_fresh=True)
    pct = 20
    stage_map(pct)
    union_map(pct, start_fresh=False)


if __name__ == "__main__":
    main()
