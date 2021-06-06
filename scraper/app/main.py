"""
Minimal FastAPI application taken directly from the tutorial.
https://fastapi.tiangolo.com/
"""
from typing import List

from fastapi import FastAPI

from scraper import rfaster

app = FastAPI()


@app.get("/")
def read_root():
    """Basic test for the webapp.

    Returns
    -------
    Dict[str]
        An example output
    """
    return {"Hello": "scraper test"}


@app.get("/rentfaster/listings/{city_id}/page/{page}")
def get_rentfaster_listings_page(
    city_id: int = 1, page: int = 0
) -> List[rfaster.RFasterListingSummary]:
    return rfaster.get_listings_page(city_id, page)


@app.get("/rentfaster/listings/{city_id}/all")
def get_all_rentfaster_listings(
    city_id: int = 1,
) -> List[rfaster.RFasterListingSummary]:
    return rfaster.get_all_listings(city_id)
