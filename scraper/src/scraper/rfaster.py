"""Scrape data from rentfaster.ca."""
import datetime as dt
import json
import re
import time
import urllib.request
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


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
            datetime = dt.datetime.strptime(avdate, "%B %d").replace(year=today.year)
            clean_date = dt.date(datetime.year, datetime.month, datetime.day)
            if clean_date < today:
                clean_date = clean_date.replace(year=today.year + 1)
        except ValueError:
            clean_date = None
    return clean_date


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


def _parse_listing(  # noqa: C901
    listing_in: Dict[str, Optional[str]]
) -> Dict[str, Any]:
    """Do some pre-validation and parsing on rentfaster listing.

    Run this before passing to pydantic

    Parameters
    ----------
    listing: Dict
        The raw listing dumped from JSON

    Returns
    -------
    Dict
        The listing with some post-processing applied
    """
    listing: Dict[str, Any] = deepcopy(listing_in)
    availability: Optional[str] = listing["availability"]
    if availability is None:
        listing["avdate"] = None
    else:
        listing["avdate"] = _avdate_parser(availability)
    # clean out commentary. For example, one listing was "about 750", I want that
    # to just say 750
    # Keep the raw square footage in case there's a parsing error I need to fix
    listing["raw_sq_feet"] = listing["sq_feet"]
    if listing["sq_feet"] is not None:
        listing["sq_feet"] = _sq_foot_parser(listing["sq_feet"])

    if (listing["id"] is not None) & (listing["link"] is not None):
        listing["id"] = _parse_listing_id(listing["id"], listing["link"])
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

    listing["link"] = f"https://www.rentfaster.ca{listing['link']}"
    return listing


def _is_valid(listing: Dict[str, str]) -> bool:
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
    is_housing = listing["type"] not in [
        "Office Space",
        "Parking Spot",
        "Storage",
        "Shared",
    ]
    is_available = listing["avdate"] != "No Vacancy"
    return is_housing & is_available


class RFasterListingSummary(BaseModel):
    """Summary of a rentfaster listing."""

    ref_id: int
    user_id: int = Field(alias="userId")
    uid: str = Field(alias="id")
    title: str
    price: int
    listing_type: str = Field(alias="type")
    raw_sq_feet: Optional[str]
    sq_feet: Optional[int]
    availability: str
    avdate: Optional[dt.date]
    neighbourhood: Optional[str] = Field(alias="location")
    rented: Optional[str]
    thumb: str
    link: str
    slide: str
    latitude: float
    longitude: float
    address: Optional[str]
    address_hidden: bool
    city: str
    province: str
    smoking: Optional[str]
    lease_term: Optional[str]
    garage_size: Optional[str]
    bedrooms: int
    den: bool
    baths: Optional[float]
    cats: bool
    dogs: bool
    electricity: bool
    water: bool
    heat: bool
    cable: bool
    internet: bool
    util_check_listing: bool


def get_listings_page(city_id: int = 1, page: int = 0) -> List[RFasterListingSummary]:
    """Retrieve listings for a specific page.

    Code modified from
    https://github.com/furas/python-examples/blob/master/__scraping__/rentfaster.ca%20-%20requests/main.py  # noqa: E510

    The full json returns keys for "listings", "query", "total" and "total2"
    We're mostly concerned with listings
    For completeness here's what the rest do:
    query: has the keys from the query string above
    total: looks like the total number of listings available
    total2: how many listings are returned on this page

    Parameters
    ----------
    city_id: int, default 1
        The city_id to query, default is 1 for Calgary
    page: int, default 1
        Results can be broken over pages, default to the first page

    Returns
    -------
    List[RFasterListingsSummary]
        parsed list of a page of rentfaster listings
    """
    # This is a hard coded url so I don't have to worry about users passing file:/ or
    # other custom schemes
    url = f"https://www.rentfaster.ca/api/search.json?proximity_type=location-city&novacancy=0&cur_page={page}&city_id={city_id}"  # noqa: E510
    r = urllib.request.urlopen(url)  # noqa: S310
    data = json.loads(r.read())
    results = [
        RFasterListingSummary(**_parse_listing(result))
        for result in data["listings"]
        if _is_valid(result)
    ]
    return results


def get_all_listings(city_id: int = 1) -> List[RFasterListingSummary]:
    """Get all available listings from rentfaster.

    Parameters
    ----------
    city_id: int, default 1
        The city_id to query, default is 1 for Calgary
    page: int, default 1
        Results can be broken over pages, default to the first page

    Returns
    -------
    List[RFasterListingsSummary]
        parsed list of a page of rentfaster listings
    """
    # Not completely happy with how I've structured this while loop, but I'll deal
    page = 0
    listings = list()
    page_list = get_listings_page(city_id=city_id, page=page)
    listings.extend(page_list)
    while page_list:
        # be polite, don't hammer the server
        time.sleep(5)
        page += 1
        page_list = get_listings_page(city_id=city_id, page=page)
        listings.extend(page_list)
    return listings
