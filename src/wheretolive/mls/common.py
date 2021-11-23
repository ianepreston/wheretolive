"""Helper functions for working in mls."""
from pathlib import Path


def _find_base_dir() -> Path:
    """Get the base folder with all the scraped data."""
    return Path(__file__).resolve().parents[3] / "data" / "mls"
