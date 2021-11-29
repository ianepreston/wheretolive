"""Parse and clean up scraped data sets."""
import datetime as dt
import json
import re
import zipfile
from pathlib import Path
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd

from wheretolive.logconf import get_logger
from wheretolive.mls.common import _find_base_dir
from wheretolive.mls.common import _find_scrapes_dir

logger = get_logger(__name__)


def _find_raw_zips(date: dt.date = None) -> List[Path]:
    """Find the raw price grouped scrapes."""
    if date is None:
        date = dt.date.today()
    scrapes_dir = _find_scrapes_dir(date)
    return list(scrapes_dir.glob("mls_*_maxprice_*.zip"))


def _load_raw_zip(zip: Path) -> List:
    """Get a list of listings from a zipped json."""
    zipname = zip.name
    jsonname = zipname.replace(".zip", ".json")
    with zipfile.ZipFile(zip, mode="r") as z:
        with z.open(jsonname) as f:
            listings = json.loads(f.read().decode("utf-8"))
    return listings


def _full_day_listings(date: dt.date = None) -> List:
    """Combine raw zips for a day into a full raw list."""
    if date is None:
        date = dt.date.today()
    all_listings = list()
    zipfiles = _find_raw_zips(date)
    for f in zipfiles:
        all_listings.extend(_load_raw_zip(f))
    return all_listings


class _ListingCleaner:
    def __init__(self, raw_listing) -> None:
        self.raw_listing = raw_listing
        self._id = self.raw_listing.get("MlsNumber")
        self._cleaned = dict()

    def parse(self):
        """Get cleaned listing if valid or None if invalid."""
        logger.info(f"Attempting to parse {self._id}")
        if self._is_vacant_land():
            return None

    def _is_vacant_land(self) -> bool:
        """We don't want vacant land."""
        is_vacant = self.raw_listing.get("Property").get("Type") == "Vacant Land"
        if is_vacant:
            logger.debug(f"{self._id} is vacant, skipping")
        return is_vacant

    def _parse_bedrooms(self, bed_key: str) -> Tuple[int, int]:
        """Parse above and below grade bedrooms.

        Parameters
        ----------
        bed_key: the raw bedrooms key from the listing

        Returns
        -------
        Tuple[int, int]:
            Number of above and below grade bedrooms
        """
        if " + " in bed_key:
            above, below = bed_key.split(" + ")
        else:
            logger.warning(
                f"No + separator for {self._id}, assuming all bedrooms above grade"
            )
            try:
                above = int(bed_key)
                below = 0
            except ValueError:
                logger.warn(f"Can't parse bedrooms for {self._id}")
                above, below = 0, 0
        return above, below

    def _parse_date(self, date_key: Optional[str]) -> Optional[dt.datetime]:
        """Read a text date into a datetime.

        Parameters
        ----------
        date_key: str
            The raw date

        Returns
        -------
        dt.datetime
            Parsed date
        """
        if date_key is None:
            return None
        else:
            return dt.datetime.strptime(date_key, "%Y-%m-%d %H:%M:%S %p")

    def _parse_listing(self) -> None:
        """Clean up and format the raw listing."""
        if self._is_vacant_land():
            self._cleaned = None
            return None
        self._cleaned["mls_id"] = self.raw_listing.get("Id")
        self._cleaned["mls_number"] = self.raw_listing.get("MlsNumber")
        self._cleaned["stories"] = self.raw_listing.get("Building").get("StoriesTotal")
        # Want to be able to parse this as None when converting to integer
        if not self._cleaned["stories"]:
            self._cleaned["stories"] = None
        self._cleaned["listing_description"] = self.raw_listing.get("PublicRemarks")
        raw_bedrooms = self.raw_listing.get("Building").get("Bedrooms")
        if raw_bedrooms is None:
            self._cleaned["bedrooms_above"] = None
            self._cleaned["bedrooms_below"] = None
            self._cleaned["bedrooms"] = None
        else:
            (
                self._cleaned["bedrooms_above"],
                self._cleaned["bedrooms_below"],
            ) = self._parse_bedrooms(raw_bedrooms)
            self._cleaned["bedrooms"] = int(self._cleaned["bedrooms_above"]) + int(
                self._cleaned["bedrooms_below"]
            )
        self._cleaned["bathrooms"] = self.raw_listing.get("Building").get(
            "BathroomTotal"
        )
        raw_sq_feet = self.raw_listing.get("Building").get("SizeInterior")
        if raw_sq_feet is None:
            self._cleaned["sq_feet_in"] = None
        else:
            # Almost everything lists in square feet. Annoying
            if "m2" in raw_sq_feet:
                sqft = float(raw_sq_feet.replace(" m2", "")) * 10.7639
                self._cleaned["sq_feet_in"] = f"{sqft}"
            else:
                self._cleaned["sq_feet_in"] = raw_sq_feet.replace(" sqft", "")

        self._cleaned["listing_type"] = self.raw_listing.get("Building").get("Type")
        self._cleaned["amenities"] = self.raw_listing.get("Building").get("Ammenities")
        self._cleaned["price"] = self.raw_listing.get("Property").get(
            "PriceUnformattedValue"
        )
        self._cleaned["property_type"] = self.raw_listing.get("Property").get("Type")
        self._cleaned["listing_address"] = (
            self.raw_listing.get("Property").get("Address").get("AddressText")
        )
        self._cleaned["longitude"] = (
            self.raw_listing.get("Property").get("Address").get("Longitude")
        )
        self._cleaned["latitude"] = (
            self.raw_listing.get("Property").get("Address").get("Latitude")
        )
        self._cleaned["ownership_type"] = self.raw_listing.get("Property").get(
            "OwnershipType"
        )
        self._cleaned["parking"] = self.raw_listing.get("Property").get("ParkingType")
        self._cleaned["parking_spaces"] = self.raw_listing.get("Property").get(
            "ParkingSpaceTotal"
        )
        self._cleaned["lot_size"] = self.raw_listing.get("Land").get("SizeTotal")
        self._cleaned["postal_code"] = self.raw_listing.get("PostalCode")
        self._cleaned[
            "link"
        ] = f"https://www.realtor.ca{self.raw_listing.get('RelativeDetailsURL')}"
        self._cleaned["price_change_dt"] = self._parse_date(
            self.raw_listing.get("PriceChangeDateUTC")
        )
        raw_timestamp = self.raw_listing.get("InsertedDateUTC")
        if raw_timestamp is not None:
            # Something about the formatting here has the epoch start wrong
            # this corrects it
            clean_timestamp = dt.datetime.fromtimestamp(int(raw_timestamp) / 10_000_000)
            clean_timestamp = clean_timestamp.replace(year=clean_timestamp.year - 1969)
        else:
            clean_timestamp = None
        self._cleaned["mls_insert_dt"] = clean_timestamp
        return None

    @property
    def clean_listing(self):
        if not self._cleaned:
            self._parse_listing()
        return self._cleaned


def _parse_listings(date: dt.date = None) -> pd.DataFrame:
    """Clean up raw listings to a DataFrame."""
    if date is None:
        date = dt.date.today()
    raw = _full_day_listings(date)
    cleaned_listings = list()
    for raw_listing in raw:
        cleaned_listing = _ListingCleaner(raw_listing).clean_listing
        if cleaned_listing is not None:
            cleaned_listings.append(cleaned_listing)
    _ = None
    listings_df = pd.DataFrame(cleaned_listings).assign(scrape_dt=date)
    numeric_cols = [
        "mls_id",
        "bedrooms_above",
        "bedrooms_below",
        "bedrooms",
        "bathrooms",
        "sq_feet_in",
        "price",
    ]
    datetime_cols = [
        "price_change_dt",
        "scrape_dt",
        "mls_insert_dt",
    ]
    for col in numeric_cols:
        listings_df[col] = pd.to_numeric(listings_df[col])
    for col in datetime_cols:
        listings_df[col] = pd.to_datetime(listings_df[col], format="%Y-%m-%d")
    return listings_df


def _raw_to_parquet(date: dt.date = None, force_overwrite: bool = False) -> Path:
    """Take a raw scrape and save a dataframe to parquet."""
    if date is None:
        date = dt.date.today()
    out_dir = _find_scrapes_dir(date)
    out_file = out_dir / "clean.pq"
    if out_file.exists() and (not force_overwrite):
        logger.debug(f"{out_file} exists, skipping")
    else:
        logger.info(f"Saving clean parquet to {out_file}")
        listings_df = _parse_listings(date)
        listings_df.to_parquet(out_file, engine="fastparquet")
    return out_file


def _find_all_raw_scrape_days() -> List[dt.date]:
    """Get all the folders with scraped data in them."""
    base_dir = _find_base_dir()
    rgx = re.compile(r"\d{4}-\d{2}-\d{2}")
    scrape_days = [
        dt.datetime.strptime(scrape_dir.name, "%Y-%m-%d").date()
        for scrape_dir in base_dir.iterdir()
        if re.match(rgx, scrape_dir.name)
    ]
    return scrape_days


def parse_scrapes(force_overwrite: bool = False):
    """Parse all raw scrapes and save them as parquet files."""
    scrape_days = _find_all_raw_scrape_days()
    for scrape_day in scrape_days:
        _raw_to_parquet(scrape_day, force_overwrite)


if __name__ == "__main__":
    parse_scrapes(True)
    print("hurray!")
