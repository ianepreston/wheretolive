"""Ingest latest scraped rentfaster listings into PostGIS for future analysis."""
import datetime as dt

import pandas as pd

from wheretolive.logconf import get_logger
from wheretolive.postgis import PostGIS
from wheretolive.rfaster.common import _find_scrapes_dir

logger = get_logger(__name__)


def get_rfaster_scrape_df(scrape_date: dt.date = None) -> pd.DataFrame:
    """Read a clean scraped rentfaster date to a dataframe."""
    scrape_file = _find_scrapes_dir(scrape_date) / "clean.pq"
    rfaster_df = pd.read_parquet(scrape_file)
    return rfaster_df


def update_rfaster_gis(scrape_date: dt.date = None):
    """Update rfaster data to the scraped date."""
    db = PostGIS().connection
    rfaster_df = get_rfaster_scrape_df(scrape_date)
    logger.info(f"Uploading rfaster data for {scrape_date}")
    with db.begin() as conn:
        rfaster_df.to_sql("rfaster_staging", conn, if_exists="replace", index=False)
        add_geom = "SELECT AddGeometryColumn('public', 'rfaster_staging','geom',4326,'POINT',2);"
        conn.execute(add_geom)
        update_latlong = 'UPDATE public.rfaster_staging SET geom = ST_SetSRID(ST_MakePoint("longitude"::double precision, "latitude"::double precision), 4326);'
        conn.execute(update_latlong)
        first_seen = """
        UPDATE rfaster_staging
        SET first_seen_dt = subquery.min_seen
        FROM (
            SELECT
                rfaster_staging.id,
                LEAST(rfaster_staging.scrape_dt, rfaster.first_seen_dt) AS min_seen
            FROM
                rfaster_staging
                LEFT JOIN rfaster
                ON rfaster_staging.id = rfaster.rfaster_id
        ) AS subquery
        WHERE rfaster_staging.id = subquery.id;
        """
        logger.info("Updating rentfaster staging with first seen date.")
        conn.execute(first_seen)
        logger.info("Truncating old rfaster table.")
        conn.execute("TRUNCATE rfaster;")
        insertion = """
        INSERT INTO rfaster (
            rfaster_id,
            price,
            listing_description,
            sq_feet_in,
            avdate,
            link,
            rented,
            smoking,
            lease_term,
            garage_size,
            bedrooms,
            den,
            bathrooms,
            cats,
            dogs,
            electricity,
            water,
            heat,
            internet,
            cable,
            util_check_listing,
            scrape_dt,
            first_seen_dt,
            geom
        )
        SELECT
            id AS rfaster_id,
            price,
            title AS listing_description,
            sq_feet AS sq_feet_in,
            avdate,
            link,
            rented,
            smoking,
            lease_term,
            garage_size,
            bedrooms,
            den,
            baths AS bathrooms,
            cats,
            dogs,
            electricity,
            water,
            heat,
            internet,
            cable,
            util_check_listing,
            scrape_dt,
            first_seen_dt,
            geom
        FROM rfaster_staging;
        """
        logger.info("Updating rfaster from rfaster_staging.")
        conn.execute(insertion)
        refresh_wide = "REFRESH MATERIALIZED VIEW rfaster_wide;"
        conn.execute(refresh_wide)
        logger.info("Refreshed wide materialized view of rfaster listings.")


if __name__ == "__main__":
    update_rfaster_gis()
