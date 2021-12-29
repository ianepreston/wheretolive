"""Helper functions for working in rentfaster."""
import datetime as dt
from pathlib import Path


def _find_base_dir() -> Path:
    """Get the base folder with all the scraped data."""
    return Path(__file__).resolve().parents[3] / "data" / "rfaster"


def _find_scrapes_dir(date: dt.date = None) -> Path:
    """Where to grab scrapes for a day or save."""
    if date is None:
        date = dt.date.today()
    datestr: str = f"{date:%Y-%m-%d}"
    return _find_base_dir() / datestr
