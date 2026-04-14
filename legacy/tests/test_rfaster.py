"""Test the rentfaster scraper.

Just a basic skeleton test for now so coverage won't complain.
"""
from wheretolive.rfaster import get_all_listings


def test_rfaster_smoke() -> None:
    """Very high level smoke test for rentfaster."""
    # If nothing crashes it must have worked, right?
    _ = get_all_listings()
