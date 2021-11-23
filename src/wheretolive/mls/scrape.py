"""Scrape and save raw MLS data."""
import datetime as dt
import json
import time
import zipfile
from pathlib import Path
from typing import List

import geocoder
import requests

from wheretolive.logconf import get_logger
from wheretolive.mls.common import _find_base_dir

logger = get_logger(__name__)

_GEOBOUND = geocoder.osm("Calgary, Canada")


def _mls_scrape_page(
    max_results: int = 500,
    price_min: int = 0,
    price_max: int = 10_000_000,
    property_type: int = 1,
    language: str = "English",
) -> List:
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
    g = _GEOBOUND
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
    logger.info(
        f"Querying MLS listings priced between ${price_min:,.0f} and ${price_max:,.0f}"
    )
    r = requests.post(uri, data=payload)
    if r.ok:
        results = r.json()["Results"]
        return results
    else:
        raise RuntimeError(f"failed for price range {price_min}, {price_max}")


def _max_result_price(results) -> float:
    """Find max price in a scraped list of results.

    Because we have to chunk requests by price range starting at zero, we hvae to find
    the highest price returned in the current chunk to know where to set the floor
    for the next chunk.
    """
    return max(
        float(listing.get("Property").get("PriceUnformattedValue"))
        for listing in results
    )


def _dump_result(results) -> Path:
    """Save results to a zipped json."""
    max_price: str = f"{_max_result_price(results):.0f}".zfill(8)
    today: str = f"{dt.date.today():%Y-%m-%d}"
    base_dir = _find_base_dir()
    dump_dir = base_dir / today
    dump_dir.mkdir(exist_ok=True, parents=True)
    filename = f"mls_{today}_maxprice_{max_price}"
    zipname = f"{filename}.zip"
    jsonname = f"{filename}.json"
    with zipfile.ZipFile(dump_dir / zipname, "w", compression=zipfile.ZIP_LZMA) as z:
        with z.open(jsonname, "w") as d:
            d.write(json.dumps(results).encode("utf-8"))

    return dump_dir / zipname


def scrape_all():
    """Save all currently available listings."""
    price_min: int = 0
    results = None
    while results or price_min == 0:
        results = _mls_scrape_page(price_min=price_min)
        if results:
            _dump_result(results)
        top_price = int(round(_max_result_price(results), 0))
        # Break out of loop if there's only one listing left
        if top_price - 1 == price_min:
            break
        # have to subtract 1 in case there are multiple listings with
        # the same price and my cutoff is partway through them
        price_min = top_price - 1
        # wait a second so I don't hammer the server too hard
        time.sleep(1)


if __name__ == "__main__":
    scrape_all()
    print("Hurray!")
