"""
Minimal FastAPI application taken directly from the tutorial.
https://fastapi.tiangolo.com/
"""
from typing import List

import mls
import rfaster
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    """Show hellow world page.

    Returns
    -------
    Dict[str]
        An example output
    """
    return {"Hello": "scraper test"}


@app.get(
    "/rentfaster/listings/{city_id}/page/{page}",
    response_model=List[rfaster.RFasterListingSummary],
)
def get_rentfaster_listings_page(
    city_id: int = 1, page: int = 0
) -> List[rfaster.RFasterListingSummary]:
    return rfaster.get_listings_page(city_id, page)


@app.get(
    "/rentfaster/listings/{city_id}/all",
    response_model=List[rfaster.RFasterListingSummary],
)
def get_all_rentfaster_listings(
    city_id: int = 1,
) -> List[rfaster.RFasterListingSummary]:
    return rfaster.get_all_listings(city_id)


@app.get("/mls/all", response_model=List[mls.MLSListing])
def get_all_mls_listings() -> List[mls.MLSListing]:
    return mls.get_all_listings()
