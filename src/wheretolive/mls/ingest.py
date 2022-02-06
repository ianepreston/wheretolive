"""Ingest latest scraped MLS listings into PostGIS for future analysis."""
import datetime as dt

import pandas as pd
from wheretolive.logconf import get_logger
from wheretolive.mls.common import _find_scrapes_dir
from wheretolive.postgis import PostGIS

logger = get_logger(__name__)


def get_mls_scrape_df(scrape_date: dt.date = None) -> pd.DataFrame:
    """Grab a scraped data set."""
    scrape_file = _find_scrapes_dir(scrape_date) / "clean.pq"
    mls_df = pd.read_parquet(scrape_file)
    return mls_df


def update_mls_postgis(scrape_date: dt.date = None):
    """Update the data set in MLS to the scraped date."""
    db = PostGIS().connection
    mls_df = get_mls_scrape_df(scrape_date)
    with db.begin() as conn:
        # Remove old entries
        logger.info("Truncating old MLS records")
        conn.execute("TRUNCATE mls;")
        mls_df.to_sql(name="mls", con=conn, index=False, if_exists="append")
        logger.info(f"Inserted {len(mls_df):,.0f} records into MLS table")
        update_latlong = 'UPDATE public.mls SET geom = ST_SetSRID(ST_MakePoint("longitude"::double precision, "latitude"::double precision), 4326);'
        conn.execute(update_latlong)
        logger.info("Parsed latlong location from latitude and longitude columns")
    # Try breaking this into chunks
    with db.begin() as conn:
        logger.info("Starting materialized view refresh")
        refresh_wide = "REFRESH MATERIALIZED VIEW mls_wide;"
        conn.execute(refresh_wide)
        logger.info("Refreshed wide materialized view of MLS listings.")

if __name__ == "__main__":
    update_mls_postgis()
