"""Put scraped isochrone data into PostGIS.

Why is this in the library and the download part is in scripts? Because I'm not well
organized.
"""
import json
from pathlib import Path

from wheretolive.logconf import get_logger
from wheretolive.postgis import PostGIS

logger = get_logger(__name__)


def _get_isochrone_file() -> Path:
    """Find the isochrone file."""
    iso_file = (
        Path(__file__).resolve().parents[2] / "data" / "isochrone" / "isochrones.json"
    )
    if not iso_file.exists():
        raise FileNotFoundError(f"Can't find isochrone file at {iso_file}")
    return iso_file


def _get_isochrones():
    """Get all the travel times from json."""
    iso_file = _get_isochrone_file()
    with open(iso_file, "r") as f:
        isochrones = json.load(f)
    logger.info("Loaded isochrones from JSON file")
    return isochrones


def exec_sql(query_file: str):
    """Run a sql script saved in this folder."""
    pg_db = PostGIS().connection
    sql_dir = Path(__file__).resolve().parent
    with open(sql_dir / query_file, "r") as f:
        sql = f.read()
    with pg_db.begin() as conn:
        conn.execute(sql)


def setup_isochrone_table():
    """Drop the existing table and make a fresh one."""
    exec_sql("setup_isochrone.sql")
    logger.info("Created fresh isochrone table.")


def _insertion_syntax(iso_entry):
    """Turn an isochrone dictionary entry to an insertion statement."""
    # (ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}')) As wkt;
    geo_shape = iso_entry["shape"]
    geo_str = f"ST_GeomFromGeoJSON('{json.dumps(geo_shape)}')"
    sql = f"""
        INSERT INTO public.isochrones (place_name, commute_mode, cutoff_time, geom)
        VALUES ('{iso_entry["place_name"]}', '{iso_entry["mode"]}', {iso_entry["cutoff_time"]}, {geo_str})
    """
    return sql


def insert_isochrones():
    """Add isochrone entries to the table."""
    pg_db = PostGIS().connection
    iso_entries = _get_isochrones()
    with pg_db.begin() as conn:
        for iso_entry in iso_entries:
            sql = _insertion_syntax(iso_entry)
            conn.execute(sql)
            msg = f"Inserted isochrone for {iso_entry['place_name']}, {iso_entry['mode']}, {iso_entry['cutoff_time']}"
            logger.info(msg)


if __name__ == "__main__":
    setup_isochrone_table()
    insert_isochrones()
    print("hurray!")
