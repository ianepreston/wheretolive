"""Scrape data from rentfaster.ca."""
import json
import urllib.request
from typing import Dict
from typing import List
from typing import Union

from pydantic import BaseModel
from pydantic import Field


class RFasterListingSummary(BaseModel):
    """Summary of a rentfaster listing."""

    ref_id: int
    user_id: int = Field(alias="userId")
    uid: int = Field(alias="id")
    title: str
    price: int
    type: str
    sq_feet: str
    availability: str
    avdate: str
    location: str
    rented: str
    thumb: str
    link: str
    slide: str
    latitude: float
    longitude: float
    marker: str
    address: str
    address_hidden: bool
    city: str
    province: str
    smoking: str
    lease_term: str
    garage_size: str
    status: str
    bedrooms: str
    den: str
    baths: str
    cats: bool
    dogs: bool
    utilities_included: Union[List[str], str]


def get_listings(city_id: int = 1, page: int = 1) -> Dict:
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
    results = [RFasterListingSummary(**result) for result in data["listings"]]
    return results
