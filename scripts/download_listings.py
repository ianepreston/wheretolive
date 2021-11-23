"""Download daily rental and sales listings.

Just saves them to the data folder. Separate logic will need to be used to provision
to a database or cloud data store.

Ok nevermind, for now I'm staging MLS stuff here too. Will refactor this later.
"""
import datetime as dt
import time
from pathlib import Path

import pandas as pd

from wheretolive.rfaster import get_listings_page

today = f"{dt.date.today():%Y-%m-%d}"
data_dir = Path(__file__).resolve().parents[1] / "data"
rfaster_dir = data_dir / "rfaster"
rfaster_dir.mkdir(exist_ok=True)

rfaster_page = 0
page_list = None
while page_list or rfaster_page == 0:
    # be polite, don't hammer the server
    filename = f"rfaster_{today}_page_{str(rfaster_page).zfill(3)}.pq"
    filepath = rfaster_dir / filename
    if filepath.exists():
        rfaster_page += 1
        continue
    page_list = get_listings_page(city_id=1, page=rfaster_page)
    if page_list:
        page_df = (
            pd.DataFrame([listing.__dict__ for listing in page_list])
            .assign(avdate=lambda df: pd.to_datetime(df["avdate"], format="%Y-%m-%d"))
            .assign(extract_date=dt.date.today())
            .assign(
                extract_date=lambda df: pd.to_datetime(
                    df["extract_date"], format="%Y-%m-%d"
                )
            )
            .assign(extract_page=rfaster_page)
        )
        page_df.to_parquet(filepath, engine="fastparquet")
        rfaster_page += 1
        time.sleep(1)
