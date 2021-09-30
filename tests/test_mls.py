"""Test the MLS scraper.

Just a basic skeleton test for now so coverage won't complain.
"""
from wheretolive.mls import get_all_listings


def test_mls_smoke() -> None:
    """Very high level smoke test for MLS."""
    # If nothing crashes it must have worked, right?
    _ = get_all_listings()
