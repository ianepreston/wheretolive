"""Upload MLS listing candidates to dropbox."""
import datetime as dt
from pathlib import Path

import pandas as pd

from wheretolive.dropbox_uploader import dropbox_save
from wheretolive.logconf import get_logger
from wheretolive.postgis import PostGIS

logger = get_logger(__name__)


def upload_candidate(requestor_name: str):
    """Upload candidate results for a specific person."""
    sql = f"SELECT * FROM {requestor_name}_candidates;"
    df = pd.read_sql(sql, PostGIS().connection)
    logger.info(f"Extracted candidates for {requestor_name}")
    csv_file = Path(__file__).resolve().parent / f"{requestor_name}_candidates.csv"
    df.to_csv(csv_file)
    logger.info("Saved candidates to local csv")
    dropbox_path = f"/wheretolive/{requestor_name}/mls_{dt.date.today():%Y-%m-%d}.csv"
    dropbox_save(csv_file, dropbox_path)
    csv_file.unlink()
    logger.info("Deleted locally saved extract.")


if __name__ == "__main__":
    upload_candidate("ian")
