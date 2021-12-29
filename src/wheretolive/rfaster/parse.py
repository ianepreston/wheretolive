"""Parse and clean up scraped data sets."""
import datetime as dt
import json
import re
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd

from wheretolive.logconf import get_logger
from wheretolive.rfaster.common import _find_base_dir
from wheretolive.rfaster.common import _find_scrapes_dir

logger = get_logger(__name__)


def _find_raw_zips(date: dt.date = None) -> List[Path]:
    """Find the raw price grouped scrapes."""
    if date is None:
        date = dt.date.today()
    scrapes_dir = _find_scrapes_dir(date)
    return list(scrapes_dir.glob("rfaster_*_page_*.zip"))


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
        self._cleaned = dict()

    @staticmethod
    def _avdate_parser(avdate: str) -> Optional[dt.date]:
        """Parse the availability date to a real date.

        Parameters
        ----------
        avdate: str
            The raw availability date string

        Returns
        -------
        Optional[dt.date]
            Parsed date if possible or None
        """
        clean_date: Optional[dt.date]
        today = dt.date.today()
        if avdate == "Immediate":
            clean_date = today
        elif avdate in ["Negotiable", "Call for Availability"]:
            clean_date = None
        else:
            try:
                datetime = dt.datetime.strptime(avdate, "%B %d").replace(
                    year=today.year
                )
                clean_date = dt.date(datetime.year, datetime.month, datetime.day)
                if clean_date < today:
                    clean_date = clean_date.replace(year=today.year + 1)
            except ValueError:
                clean_date = None
        return clean_date

    @staticmethod
    def _sq_foot_parser(sq_feet: str) -> Optional[int]:
        """Parse square footage.

        Parameters
        ----------
        sq_feet: str
            The raw input of square footage

        Returns
        -------
        Optional[int]
            Square footage as an integer if it can be parsed, or None
        """
        # Some of them do addition, I'm not dealing with that right now
        if "plus" in sq_feet.lower():
            sq_feet = ""
        # Listing seems to treat "0" and none as interchangeable
        sq_feet = re.sub("[A-Za-z~\.\ <>,]", "", sq_feet)  # noqa: W605
        if sq_feet == "0":
            return None
        elif not sq_feet:
            return None
        # Some listings put ranges like 750 - 900. Just drop those for now
        # we'll still have the raw string
        try:
            parsed_sq_feet = int(sq_feet)
        except ValueError:
            return None
        except TypeError:
            return None
        return parsed_sq_feet

    @staticmethod
    def _parse_listing_id(id_str: str, link: str) -> str:
        """Parse the listing id from its link.

        Multiple listings can share an id, like if a building has different types of
        units. The site generates a _ and a number after the link for these listings.
        It doesn't do anything to the presentation of the page, presumably it's just for
        analytics, but I can use it to make a unique id per listing. If there isn't an
        underscore, that's a single listing, so just make it _0

        Parameters
        ----------
        id: str
            The original id from the JSON
        link: str
            The url for the listing

        Returns
        -------
        str:
            An id that accounts for multiple listings per page
        """
        rgx = re.compile(r"^.*\/([0-9_]*$)")
        match_attempt = rgx.match(link)
        if not match_attempt:
            raise ValueError(f"Couldn't parse link from {link}")
        link_end = match_attempt.groups()[0]
        if "_" in link_end:
            base, decimal = link_end.split("_")
            id_str = f"{base}_{decimal}"
        else:
            id_str = f"{id_str}_0"
        return id_str

    def _is_valid(self) -> bool:
        """Check that this isn't for a parking space or something.

        Parameters
        ----------
        listing: Dict
            The raw JSON of the listing

        Returns
        -------
        bool:
            Whether or not this should be included in the result set
        """
        listing = self.raw_listing
        is_housing = listing["type"] not in [
            "Office Space",
            "Parking Spot",
            "Storage",
            "Shared",
        ]
        is_available = listing["avdate"] != "No Vacancy"
        return is_housing & is_available

    def _parse_listing(self) -> Dict[str, Any]:
        """Clean up a raw listing JSON.

        Returns
        -------
        Dict
            The listing with some post-processing applied
        """
        listing: Dict[str, Any] = deepcopy(self.raw_listing)
        availability: Optional[str] = listing["availability"]
        if availability is None:
            listing["avdate"] = None
        else:
            listing["avdate"] = self._avdate_parser(availability)
        # clean out commentary. For example, one listing was "about 750", I want that
        # to just say 750
        # Keep the raw square footage in case there's a parsing error I need to fix
        listing["raw_sq_feet"] = listing["sq_feet"]
        if listing["sq_feet"] is not None:
            listing["sq_feet"] = self._sq_foot_parser(listing["sq_feet"])

        if (listing["id"] is not None) & (listing["link"] is not None):
            listing["id"] = self._parse_listing_id(listing["id"], listing["link"])
        else:
            raise ValueError(
                f"ID: {listing['id']} Link: {listing['link']} validation error"
            )
        # There doesn't appear to be any consistency between listing bedroom as
        # "1 + Den" and listing bedroom as "1" and Den as "yes", let's consolidate that
        # clean up den first, assume blank is No
        if listing["den"] in ["No", ""]:
            listing["den"] = False
        elif listing["den"] == "Yes":
            listing["den"] = True
        if " + Den" in listing["bedrooms"]:
            listing["den"] = True
            listing["bedrooms"] = listing["bedrooms"].replace(" + Den", "")
        # I'm going to call bachelor 0
        if listing["bedrooms"] in ["bachelor", "none"]:
            listing["bedrooms"] = "0"
        if listing["baths"] == "none":
            listing["baths"] = None
        # Utilities listing is going to be really hard to parse as it is, but
        # There seems to be a pretty consistent way they're entered
        util_keys = ["electricity", "water", "heat", "internet", "cable"]
        for util_key in util_keys:
            listing[util_key] = False
            if not listing["utilities_included"]:
                pass
            elif util_key.title() in listing["utilities_included"]:
                listing[util_key] = True
        if not listing["utilities_included"]:
            listing["util_check_listing"] = False
        elif "See Full Description" in listing["utilities_included"]:
            listing["util_check_listing"] = True
        else:
            listing["util_check_listing"] = False
        listing.pop("utilities_included", None)

        listing["link"] = f"https://www.rentfaster.ca{listing['link']}"
        listing["rented"] = listing["rented"] != "Not-Rented"
        return listing

    @property
    def clean_listing(self):
        if not self._cleaned and self._is_valid():
            self._cleaned = self._parse_listing()
        return self._cleaned


def _parse_listings(date: dt.date = None) -> pd.DataFrame:
    """Clean up raw listings to a DataFrame."""
    if date is None:
        date = dt.date.today()
    raw = _full_day_listings(date)
    cleaned_listings = list()
    for raw_listing in raw:
        cleaned_listing = _ListingCleaner(raw_listing).clean_listing
        if cleaned_listing:
            cleaned_listings.append(cleaned_listing)
    _ = None
    listings_df = (
        pd.DataFrame(cleaned_listings)
        .assign(scrape_dt=date)
        .assign(first_seen_dt=date)
        .rename(
            columns={
                "userId": "user_id",
                "type": "listing_type",
                "location": "neighbourhood",
            }
        )
    )
    numeric_cols = [
        "user_id",
        "bedrooms",
        "baths",
        "sq_feet",
        "price",
    ]
    datetime_cols = [
        "avdate",
        "scrape_dt",
        "first_seen_dt",
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
