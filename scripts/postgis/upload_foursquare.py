"""Add in scraped foursquare grocery store data."""
from pathlib import Path

import pandas as pd

from wheretolive.postgis import PostGIS


def grocery_df() -> pd.DataFrame:
    """Load the saved csv of grocery stores.

    Can add in filters for stores I don't want in here rather than hitting the API
    every time to get a data set.
    """
    fs_file = (
        Path(__file__).resolve().parents[2] / "data" / "foursquare" / "groceries.csv"
    )
    return pd.read_csv(fs_file)


def push_groceries() -> None:
    """Push the groceries data set into PostGIS."""
    gdf = grocery_df()
    with PostGIS().connection.begin() as conn:
        gdf.to_sql("grocery_stores", conn, index=False, if_exists="replace")
        add_geom = "SELECT AddGeometryColumn('public', 'grocery_stores','geom',4326,'POINT',2);"
        conn.execute(add_geom)
        update_latlong = 'UPDATE public.grocery_stores SET geom = ST_SetSRID(ST_MakePoint("longitude"::double precision, "latitude"::double precision), 4326);'
        conn.execute(update_latlong)


if __name__ == "__main__":
    push_groceries()
