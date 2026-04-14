"""Connect to and grab data from the Foursquare API.

https://developer.foursquare.com/docs/categories
"""
import os
from pathlib import Path
from typing import Dict
from typing import List

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from wheretolive.logconf import get_logger

logger = get_logger(__name__)

load_dotenv()

FS_KEY = os.getenv("FOURSQUARE_KEY")


def _latlong_grid(steps: int = 20) -> List[Dict[str, str]]:
    """Iterate over a grid of equally sized points across calgary."""
    west_bound = -114.3157587
    east_bound = -113.8600018
    north_bound = 51.2125013
    south_bound = 50.842526
    n_to_s = np.linspace(start=north_bound, stop=south_bound, endpoint=True, num=steps)
    e_to_w = np.linspace(start=east_bound, stop=west_bound, endpoint=True, num=steps)
    rectangles = list()
    for ns in range(1, len(n_to_s)):
        for ew in range(1, len(e_to_w)):
            n = n_to_s[ns - 1]
            s = n_to_s[ns]
            e = e_to_w[ew - 1]
            w = e_to_w[ew]
            ne = f"{n},{e}"
            sw = f"{s},{w}"
            rectangles.append({"ne": ne, "sw": sw})
    return rectangles


def _get_fs_path() -> Path:
    """Find where to save foursquare data."""
    fs_path = Path(__file__).resolve().parents[2] / "data" / "foursquare"
    fs_path.mkdir(exist_ok=True)
    return fs_path


def foursquare_groceries():
    """Scrape all grocery store locations off FourSquare."""
    rectangles = _latlong_grid()
    groceries = list()
    for rectangle in rectangles:
        ne = rectangle["ne"]
        sw = rectangle["sw"]
        limit = 50
        category = "17069"
        base_url = "https://api.foursquare.com/v3/places/search?v=20211229"
        headers = {"Accept": "application/json", "Authorization": FS_KEY}
        url = f"{base_url}&ne={ne}&sw={sw}&categories={category}&limit={limit}"
        response = requests.request("GET", url, headers=headers).json()
        stores = response.get("results")
        if stores:
            if len(stores) >= 50:
                logger.warn(
                    f"Rectangle at {ne} has 50 results, should use smaller bounds"
                )
            for store in stores:
                name = store["name"]
                lat = store["geocodes"]["main"]["latitude"]
                long = store["geocodes"]["main"]["longitude"]
                groceries.append({"name": name, "latitude": lat, "longitude": long})
    df = pd.DataFrame(groceries)
    fs_path = _get_fs_path() / "groceries.csv"
    df.to_csv(fs_path, index=False)
    return groceries


if __name__ == "__main__":
    foursquare_groceries()
