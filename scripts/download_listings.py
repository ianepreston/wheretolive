"""Download daily rental and sales listings.

Just saves them to the data folder. Separate logic will need to be used to provision
to a database or cloud data store.
"""
import datetime as dt
import time
from pathlib import Path

import pandas as pd

from wheretolive.mls import _mls_scrape_page
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

mls_dir = data_dir / "mls"
mls_dir.mkdir(exist_ok=True)
price_min = 0
listings = None
while listings or price_min == 0:
    filename = f"mls_{today}_minprice_{str(price_min).zfill(8)}.pq"
    filepath = mls_dir / filename
    listings = _mls_scrape_page(price_min=price_min)
    # Don't think I can easily skip existing files since I don't know my increment
    if listings:
        listings_df = (
            pd.DataFrame([listing.__dict__ for listing in listings])
            .assign(
                price_change_dt=lambda df: pd.to_datetime(
                    df["price_change_dt"], format="%Y-%m-%d"
                )
            )
            .assign(extract_date=dt.date.today())
            .assign(
                extract_date=lambda df: pd.to_datetime(
                    df["extract_date"], format="%Y-%m-%d"
                )
            )
        )
        listings_df.to_parquet(filepath, engine="fastparquet")
        top_price = listings_df["price"].max()
        # Break out of my loop if I'm just grabbing one listing over and over
        if top_price - 1_000 == price_min:
            break
        # Could probably do the exact price, but just to be safe
        price_min = int(top_price - 1_000)
        time.sleep(1)
