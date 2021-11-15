"""Ingest downloaded parquet files into a database."""
import datetime as dt
import re
from pathlib import Path
from typing import List

import pandas as pd

from wheretolive.postgis import PostGIS


def get_data_dir() -> Path:
    """Get the base folder where ingest data lives.

    Right now this is kind of overkill but might get more complicated later

    Returns
    -------
    Path: The base download folder
    """
    return Path(__file__).resolve().parents[2] / "data"


def _find_daily_scrapes(subdir: str, scrape_date: dt.date) -> List[Path]:
    """Get all the daily scraped data files for a particular day."""
    scrape_folder = get_data_dir() / subdir
    date_str = scrape_date.strftime("%Y-%m-%d")
    scrapes = scrape_folder.glob(f"*{date_str}*.pq")
    return list(scrapes)


def _get_scrape_df(subdir: str, scrape_date: dt.date) -> pd.DataFrame:
    """Concatenate all the scraped listings for a day."""
    return pd.concat(
        [pd.read_parquet(scrape) for scrape in _find_daily_scrapes(subdir, scrape_date)]
    ).drop_duplicates(ignore_index=True)


def get_mls_scrape_df(scrape_date: dt.date) -> pd.DataFrame:
    """Get a DataFrame of MLS scrapes ready for ingestion."""
    return _get_scrape_df("mls", scrape_date=scrape_date).rename(
        columns={
            "description": "listing_description",
            "address": "listing_address",
            "extract_date": "scrape_date",
        }
    )


def _stage_mls(scrape_date: dt.date) -> None:
    """Load a dataframe into staging."""
    # Delete what was in staging originally
    sql = "TRUNCATE mls_staging;"
    db = PostGIS().connection
    with db.begin() as conn:
        conn.execute(sql)
    stage_df = get_mls_scrape_df(scrape_date=scrape_date)
    stage_df.to_sql(name="mls_staging", con=db, index=False, if_exists="append")


def _truncate_mls():
    """Delete everything in MLS so I can restart."""
    sql = "TRUNCATE mls;"
    db = PostGIS().connection
    with db.begin() as conn:
        conn.execute(sql)


def _update_mls_from_stage() -> None:
    """Update the MLS table with what's in staging."""
    db = PostGIS().connection
    mls_update_count_sql = "SELECT COUNT(mls_id) FROM MLS;"
    staging_count_sql = "SELECT COUNT(mls_id) FROM mls_staging;"
    staging_insert_count_sql = "SELECT COUNT(mls_id) FROM mls_insert_stage_view;"
    staging_update_count_sql = "SELECT COUNT(mls_id) FROM mls_update_stage_view;"
    staging_backfill_update_count_sql = (
        "SELECT COUNT(mls_id) FROM mls_update_backfill_stage_view;"
    )
    with db.begin() as conn:
        pre_update_count = conn.execute(mls_update_count_sql).fetchone()[0]
        staging_count = conn.execute(staging_count_sql).fetchone()[0]
        staging_insert_count = conn.execute(staging_insert_count_sql).fetchone()[0]
        staging_update_count = conn.execute(staging_update_count_sql).fetchone()[0]
        staging_backfill_update_count = conn.execute(
            staging_backfill_update_count_sql
        ).fetchone()[0]
    staging_check_count = (
        staging_insert_count + staging_update_count + staging_backfill_update_count
    )
    if staging_check_count != staging_count:
        raise RuntimeError(
            f"staging has {staging_count} records but views total {staging_check_count}"
        )
    print(
        f"{staging_insert_count} {staging_update_count} {staging_backfill_update_count}"
    )
    mls_cols = [
        "mls_id",
        "mls_number",
        "listing_description",
        "bedrooms_above",
        "bedrooms_below",
        "bedrooms",
        "bathrooms",
        "sq_feet_in",
        "listing_type",
        "amenities",
        "current_price",
        "lowest_price",
        "highest_price",
        "property_type",
        "listing_address",
        "latlong",
        "ownership_type",
        "parking",
        "lot_size",
        "postal_code",
        "link",
        "first_scrape_date",
        "latest_scrape_date",
    ]
    mls_cols_str = ", ".join(col for col in mls_cols)
    update_cols = [
        col for col in mls_cols if col not in ("mls_id", "first_scrape_date")
    ]
    update_str = ", ".join(
        f"{col} = mls_update_stage_view.{col}" for col in update_cols
    )
    insert_sql = f"""
        INSERT INTO mls ({mls_cols_str})
        SELECT * FROM mls_insert_stage_view;
    """
    update_sql = f"""
        UPDATE mls SET {update_str}
        FROM mls_update_stage_view
        WHERE mls.mls_id = mls_update_stage_view.mls_id;
    """
    update_backfill_cols = [
        "lowest_price",
        "highest_price",
        "first_scrape_date",
    ]
    update_backfill_str = ", ".join(
        f"{col} = mls_update_backfill_stage_view.{col}" for col in update_backfill_cols
    )
    update_backfill_sql = f"""
        UPDATE mls SET {update_backfill_str}
        FROM mls_update_backfill_stage_view
        WHERE mls.mls_id = mls_update_backfill_stage_view.mls_id
    """
    with db.begin() as conn:
        conn.execute(insert_sql)
        # Only one of these should ever be non zero for results
        if staging_update_count:
            conn.execute(update_sql)
        elif staging_backfill_update_count:
            conn.execute(update_backfill_sql)
        post_update_count = conn.execute(mls_update_count_sql).fetchone()[0]
    print(
        f"pre update: {pre_update_count}, post update: {post_update_count} expected {pre_update_count + staging_insert_count}"
    )


def update_mls(scrape_date: dt.date) -> None:
    """Stage a scraped date then update the mls table with it."""
    _stage_mls(scrape_date=scrape_date)
    _update_mls_from_stage()


def mls_has_data() -> bool:
    """Check if mls table has data.

    Used to determine whether to do a full ingest or just the latest day.
    """
    db = PostGIS().connection
    with db.begin() as conn:
        rowcount = conn.execute("SELECT COUNT(mls_id) FROM mls;").fetchone()[0]
    return rowcount > 0


def get_ingested_dates() -> List[dt.date]:
    """See what dates we've downloaded."""
    scrape_dir = get_data_dir() / "mls"
    filenames = [path.name for path in scrape_dir.glob("*.pq")]
    rgx = re.compile(r"mls_(\d{4}-\d{2}-\d{2})_minprice_\d+\.pq")
    filedates = set(rgx.match(filename).groups()[0] for filename in filenames)
    clean_dates = sorted(
        [dt.datetime.strptime(datestr, "%Y-%m-%d").date() for datestr in filedates]
    )
    return clean_dates


def ingest_mls():
    """Load data on mls into the database.

    If the mls table is empty this will load in everything that's downloaded.
    If it's not it will only load in today's data
    """
    if mls_has_data():
        update_mls(dt.date.today())
    else:
        for ingest_date in get_ingested_dates():
            update_mls(ingest_date)


if __name__ == "__main__":
    ingest_mls()
