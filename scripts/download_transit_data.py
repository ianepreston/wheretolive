"""Grab transit and open streetmap data."""
from pathlib import Path

import requests

GTFS_URL = r"https://data.calgary.ca/download/npk7-z3bj/application%2Fx-zip-compressed"

DATA_DIR = Path("data/isochrone/commute/calgary")


def download_gtfs() -> Path:
    """Grab transit data from city of Calgary."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(GTFS_URL, stream=True, headers={"user-agent": None})
    zip_file = DATA_DIR / "calgary.gtfs.zip"
    with open(zip_file, "wb") as handle:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                handle.write(chunk)
    return zip_file


if __name__ == "__main__":
    download_gtfs()
