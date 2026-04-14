"""Upload rfaster listing candidates to dropbox."""
from pathlib import Path

import pandas as pd

from wheretolive.dropbox_uploader import dropbox_save
from wheretolive.logconf import get_logger
from wheretolive.postgis import PostGIS

logger = get_logger(__name__)


def upload_candidate(requestor_name: str):
    """Upload candidate results for a specific person."""
    sql = f"SELECT * from {requestor_name}_candidates_rfaster;"
    df = pd.read_sql(sql, PostGIS().connection, parse_dates=["first_seen_dt"])
    logger.info(f"Extracted rfaster candidates for {requestor_name}")
    dump_path = Path(__file__).resolve().parent
    full_file = dump_path / f"{requestor_name}_rfaster_candidates.html"
    df.to_html(full_file, render_links=True)
    new_mask = df["first_seen_dt"] == df["first_seen_dt"].max()
    new_file = dump_path / f"{requestor_name}_rfaster_candidates_newest.html"
    df.loc[new_mask].to_html(new_file, render_links=True)
    logger.info("Saved candidates to local html.")
    dropbox_full_path = f"/wheretolive/{requestor_name}/rfaster_full.html"
    dropbox_new_path = f"/wheretolive/{requestor_name}/rfaster_new.html"
    dropbox_save(full_file, dropbox_full_path)
    dropbox_save(new_file, dropbox_new_path)
    logger.info("Uploaded candidates to dropbox.")
    full_file.unlink()
    new_file.unlink()
    logger.info("Deleted locally saved extracts.")


if __name__ == "__main__":
    upload_candidate("ian")
