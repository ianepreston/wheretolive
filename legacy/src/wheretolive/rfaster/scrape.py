"""Scrape and save raw Rentfaster data."""
import datetime as dt
import json
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import List

from wheretolive.logconf import get_logger
from wheretolive.rfaster.common import _find_base_dir

logger = get_logger(__name__)


def get_listings_page(city_id: int = 1, page: int = 0) -> List:
    """Retrieve listings for a specific page.

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
    List:
        Raw JSON of a rentfaster listing page
    """
    # Modified from
    # "https://github.com/furas/python-examples/blob/master/__scraping__/rentfaster.ca%20-%20requests/main.py"  # noqa: E510 B950
    # This is a hard coded url so I don't have to worry about users passing file:/ or
    # other custom schemes
    logger.info(f"Querying Rentfaster listings page {page}.")
    url = f"https://www.rentfaster.ca/api/search.json?proximity_type=location-city&novacancy=0&cur_page={page}&city_id={city_id}"  # noqa: E510 B950
    r = urllib.request.urlopen(url)  # noqa: S310
    data = json.loads(r.read())["listings"]
    return data


def _dump_result(results, page) -> Path:
    """Save results to a zipped json."""
    today: str = f"{dt.date.today():%Y-%m-%d}"
    base_dir = _find_base_dir()
    dump_dir = base_dir / today
    dump_dir.mkdir(exist_ok=True, parents=True)
    filename = f"rfaster_{today}_page_{page}"
    zipname = f"{filename}.zip"
    jsonname = f"{filename}.json"
    logger.info(f"Dumping rentfaster listings page {page}.")
    with zipfile.ZipFile(dump_dir / zipname, "w", compression=zipfile.ZIP_LZMA) as z:
        with z.open(jsonname, "w") as d:
            d.write(json.dumps(results).encode("utf-8"))
    return dump_dir / zipname


def scrape_all(city_id: int = 1) -> None:
    """Get all available listings from rentfaster.

    Parameters
    ----------
    city_id: int, default 1
        The city_id to query, default is 1 for Calgary

    Returns
    -------
    List:
        parsed list of all pages of rentfaster listings
    """
    # Not completely happy with how I've structured this while loop, but I'll deal
    page = 0
    page_list = get_listings_page(city_id=city_id, page=page)
    _dump_result(page_list, page)
    while page_list:
        # be polite, don't hammer the server
        time.sleep(1)
        page += 1
        page_list = get_listings_page(city_id=city_id, page=page)
        _dump_result(page_list, page)


if __name__ == "__main__":
    scrape_all()
    print("hurray!")
