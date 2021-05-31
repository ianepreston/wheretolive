"""Scrape data from rentfaster.ca."""
import json
import re
import urllib.request
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field


def _parse_listing(listing: Dict) -> Dict:
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
    listing["sq_feet"] = re.sub("[^0-9]", "", listing["sq_feet"])
    if not listing["sq_feet"]:
        listing["sq_feet"] = None
    rgx = re.compile(r"^.*\/([0-9_]*$)")
    link_end = rgx.match(listing["link"]).groups()[0]
    if "_" in link_end:
        base, decimal = link_end.split("_")
        listing["id"] = f"{base}_{decimal}"
    else:
        listing["id"] = f"{listing['id']}_0"

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
    if listing["type"] in ["Office Space", "Parking Spot", "Storage", "Shared"]:
        return False
    return True


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
    avdate: Optional[str]
    location: Optional[str]
    rented: Optional[str]
    thumb: str
    link: str
    slide: str
    latitude: Optional[float]
    longitude: Optional[float]
    marker: Optional[str]
    address: Optional[str]
    address_hidden: bool
    city: str
    province: str
    smoking: Optional[str]
    lease_term: Optional[str]
    garage_size: Optional[str]
    status: Optional[str]
    bedrooms: Optional[str]
    den: Optional[str]
    baths: Optional[str]
    cats: Optional[bool]
    dogs: Optional[bool]
    utilities_included: Optional[Union[List[str], str]]


def get_listings(city_id: int = 1, page: int = 1) -> List[RFasterListingSummary]:
    """Retrieve listings.

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
    Dict
        Dictionary of results, gotta work out the format for this still.
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
