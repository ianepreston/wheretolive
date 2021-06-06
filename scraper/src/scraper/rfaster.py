"""Scrape data from rentfaster.ca."""
import json
import re
import time
import urllib.request
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


def _parse_listing(listing: Dict) -> Dict:  # noqa: C901
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
    # clean out commentary. For example, one listing was "about 750", I want that
    # to just say 750
    listing["sq_feet"] = re.sub("[^0-9]", "", listing["sq_feet"])
    if not listing["sq_feet"]:
        listing["sq_feet"] = None
    # Multiple listings can share an id, like if a building has different types of
    # units. The site generates a _ and a number after the link for these listings.
    # It doesn't do anything to the presentation of the page, presumably it's just for
    # analytics, but I can use it to make a unique id per listing. If there isn't an
    # underscore, that's a single listing, so just make it _0
    rgx = re.compile(r"^.*\/([0-9_]*$)")
    link_end = rgx.match(listing["link"]).groups()[0]
    if "_" in link_end:
        base, decimal = link_end.split("_")
        listing["id"] = f"{base}_{decimal}"
    else:
        listing["id"] = f"{listing['id']}_0"
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


def _is_housing(listing: Dict) -> bool:
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
    return listing["type"] not in ["Office Space", "Parking Spot", "Storage", "Shared"]


class RFasterListingSummary(BaseModel):
    """Summary of a rentfaster listing."""

    ref_id: int
    user_id: int = Field(alias="userId")
    uid: str = Field(alias="id")
    title: str
    price: int
    listing_type: str = Field(alias="type")
    sq_feet: Optional[int]
    availability: str
    avdate: str
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
        if _is_housing(result)
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
        # Get rid of this later
        if not page % 10:
            print(page)
        page += 1
        page_list = get_listings_page(city_id=city_id, page=page)
        listings.extend(page_list)
    return listings
