# type: ignore
"""Scrape data from MLS.ca.

The format for the JSON that comes in from MLS is annoying to specify, so I'm turning
off type validation for this module :/
"""
import datetime as dt
import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import geocoder
import requests
from pydantic import BaseModel


def _get_city(city_name: str = "Calgary, Canada") -> geocoder.api.OsmQuery:
    """Geocoded location for search.

    Parameters
    ----------
    city_name: str, default "Calgary, Canada"
        The city to query

    Returns
    -------
    geocoder.api.OsmQuery
    """
    return geocoder.osm(city_name)


def is_valid_listing(listing: Dict[str, Dict[str, str]]) -> bool:
    """Check if we want to include this listing.

    Parameters
    ----------
    listing: Dict
        Raw listing from realtor.ca

    Returns
    -------
    bool:
        Do we want to include this listing or not?
    """
    if listing.get("Property").get("Type") == "Vacant Land":
        return False
    else:
        return True


class MLSListing(BaseModel):
    """Summary of an MLS listing from realtor.ca."""

    mls_id: int
    mls_number: str
    description: str
    bedrooms_above: Optional[int]
    bedrooms_below: Optional[int]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    sq_feet_in: Optional[float]
    listing_type: Optional[str]
    amenities: Optional[str]
    price: float
    property_type: Optional[str]
    address: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    ownership_type: Optional[str]
    parking: Optional[str]
    lot_size: Optional[str]
    postal_code: Optional[str]
    link: str
    price_change_dt: Optional[dt.datetime]


def _parse_bedrooms(bed_key: str) -> Tuple[int, int]:
    """Parse above and below grade bedrooms.

    Parameters
    ----------
    bed_key: the raw bedrooms key from the listing

    Returns
    -------
    Tuple[int, int]:
        Number of above and below grade bedrooms
    """
    above, below = bed_key.split(" + ")
    return (int(above), int(below))


def _parse_date(date_key: Optional[str]) -> Optional[dt.datetime]:
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


def _parse_listing(listing: Dict) -> MLSListing:
    """Read JSON into a pydantic model."""
    cleaned = dict()
    cleaned["mls_id"] = listing.get("Id")
    cleaned["mls_number"] = listing.get("MlsNumber")
    cleaned["description"] = listing.get("PublicRemarks")
    raw_bedrooms = listing.get("Building").get("Bedrooms")
    if raw_bedrooms is None:
        cleaned["bedrooms_above"] = None
        cleaned["bedrooms_below"] = None
        cleaned["bedrooms"] = None
    else:
        cleaned["bedrooms_above"], cleaned["bedrooms_below"] = _parse_bedrooms(
            raw_bedrooms
        )
        cleaned["bedrooms"] = cleaned["bedrooms_above"] + cleaned["bedrooms_below"]
    cleaned["bathrooms"] = listing.get("Building").get("BathroomTotal")
    raw_sq_feet = listing.get("Building").get("SizeInterior")
    if raw_sq_feet is None:
        cleaned["sq_feet_in"] = None
    else:
        cleaned["sq_feet_in"] = raw_sq_feet.replace(" sqft", "")

    cleaned["listing_type"] = listing.get("Building").get("Type")
    cleaned["amenities"] = listing.get("Building").get("Ammenities")
    cleaned["price"] = listing.get("Property").get("PriceUnformattedValue")
    cleaned["property_type"] = listing.get("Property").get("Type")
    cleaned["address"] = listing.get("Property").get("Address").get("AddressText")
    cleaned["longitude"] = listing.get("Property").get("Address").get("Longitude")
    cleaned["latitude"] = listing.get("Property").get("Address").get("Latitude")
    cleaned["ownership_type"] = listing.get("Property").get("OwnershipType")
    cleaned["parking"] = listing.get("Property").get("ParkingType")
    cleaned["lot_size"] = listing.get("Land").get("SizeTotal")
    cleaned["postal_code"] = listing.get("PostalCode")
    cleaned["link"] = f"https://www.realtor.ca{listing['RelativeDetailsURL']}"
    cleaned["price_change_dt"] = _parse_date(listing.get("PriceChangeDateUTC"))
    return MLSListing(**cleaned)


def _mls_scrape_page(
    max_results: int = 500,
    price_min: int = 0,
    price_max: int = 10_000_000,
    property_type: int = 1,
    language: str = "English",
) -> List[MLSListing]:
    """Scrape from MLS.

    Parameters
    ----------
    max_results: int, default 10
        Maximum results to return, tops out at 500
    price_min: int,default 0
        Lowest price to search
    price_max: int, default $10M
        Highest price to search
    property_type: int, default 1
        0: No preference
        1: Residential
        2: Recreational
        3: Condo/Strata
        4: Agriculture
        5: Parking
        6: Vacant Land
        8: Multi Family
        Note that there's some overlap between what's in Residential and Condo/Strata
        I also don't know why they skipped 7, maybe it's deprecated
    language: ["English", "French"] default English
        Result language

    """
    languages = {"English": "1", "French": "2"}
    g = _get_city()
    payload = {
        "CultureId": languages[language],
        # Hard code to mobile, seems to allow it to work
        "ApplicationId": "37",
        "RecordsPerPage": max_results,
        "MaximumResults": max_results,
        "PropertySearchTypeId": property_type,
        "PriceMin": price_min,
        "PriceMax": price_max,
        # This only applies to commercial listings
        "LandSizeRange": "0-0",
        # 1: sale or rent, 2: sale, 3: rent
        # only using this for sale
        "TransactionTypeId": "2",
        # going to leave these hard coded for now
        "StoreyRange": "0-0",
        "BedRange": "0-0",
        "BathRange": "0-0",
        # Bounding box of the search
        "LongitudeMin": g.west,
        "LongitudeMax": g.east,
        "LatitudeMin": g.south,
        "LatitudeMax": g.north,
        "SortOrder": "A",
        "SortBy": "1",
        "viewState": "m",
        "Longitude": g.lng,
        "Latitude": g.lat,
        "ZoomLevel": "8",
    }

    uri = "https://api.realtor.ca/Listing.svc/PropertySearch_Post"
    r = requests.post(uri, data=payload)
    if r.ok:
        results = r.json()["Results"]
        return [
            _parse_listing(result) for result in results if is_valid_listing(result)
        ]
    else:
        raise RuntimeError(f"failed for price range {price_min}, {price_max}")


def get_all_listings() -> List[MLSListing]:
    """Scrape all the pages.

    Returns
    -------
    List[MLSListing]
        All the currently available listings
    """
    scraped_ids = set()
    all_listings = list()
    price_min = 0
    listings = _mls_scrape_page(price_min=price_min)
    while listings:
        for listing in listings:
            if listing.mls_id not in scraped_ids:
                scraped_ids.add(listing.mls_id)
                all_listings.append(listing)
        top_price = max(listing.price for listing in listings)
        # Could probably do the exact price, but just to be safe
        price_min = int(top_price - 1_000)
        time.sleep(5.0)
        listings = _mls_scrape_page(price_min=price_min)
        listings = [
            listing for listing in listings if listing.mls_id not in scraped_ids
        ]
    return all_listings
