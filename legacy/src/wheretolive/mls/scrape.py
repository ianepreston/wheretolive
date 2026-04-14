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


def _mls_cookie() -> dict:
    """Construct a cookie to make scraping less flaky."""
    return {
		"_4c_": "{\"_4c_s_\":\"XZJNj9sgEIb/SsQ5JIAxH7mtUrVaaQ9VparHiAB20HqNhUm82yj/vYObD+36wvDOMy8jz5zRdPA92lDBORWSyErVZIle/ceINmeUgivHCW0Ql5Rrowxmta0wV3yPtScOi0YSIhXhjNdoid5nL1krogXThF6WyPU3D+cbc+zyHdOVrlUtJBWAhRtlvuSVYMUmTVfglhBMUfYJnRVA7XBFz8hG58GT6hXlK0pwM4J7/gsS5oxAPKTojjbv8sdQwMnvF6N7hYTzp2D9bgouHyDBakEe6sGH9pCLMeezPKRygWgKvYvT17Kr+ihjSoK8T3EafSn9HpJv4vtCC5Aj/Hz0Zy4p7ULGpzRjh5yHcbNeT9O0St50OaaVNWuAxpBL/w/xqsEoHzKe5RfTt0fT+mvHL7FtvVs8w5hQY7rRg/YzxVPobUGeur1PudRt47HPqdhtTW9ckX75MTjf52C6mLbx7c2nYE03v3jPAOf7MtkhOYh/PO1+P3+DqxSaCyXLbESlYYW0oOhymyhndcUl00zCwDJYKsFJ+YA43ZelocZaI2pccecw99JiZRuCpdW11qqp91Ld9wlWXMF7St8sqfrveLn8Aw==\"}",
		"_dc_gtm_UA-12908513-11": "1",
		"_fbp": "fb.1.1639804962224.878511891",
		"_ga": "GA1.2.769468719.1639804961",
		"_ga_Y07J3B53QP": "GS1.1.1644166383.10.1.1644167069.53",
		"_gali": "homeSearchBtn",
		"_gid": "GA1.2.725039489.1644161449",
		"ASP.NET_SessionId": "bnqpr4kyvw3bsabizlz0oag0",
		"gig_bootstrap_3_mrQiIl6ov44s2X3j6NGWVZ9SDDtplqV7WgdcyEpGYnYxl7ygDWPQHqQqtpSiUfko": "gigya-pr_ver4",
		"incap_ses_235_2269415": "vkXaSpLTllSPFVMFxuNCA17t/2EAAAAAXSF/1oSTWpLp7C7pSAUyAg==",
		"incap_ses_675_2269415": "J0EtadYP2TRowuomjRVeCaXp/2EAAAAAoNGhpKe8UDWDJFH8AZxDUQ==",
		"incap_ses_675_2271082": "xgK8b5DaOy8Ms/UmjRVeCZr//2EAAAAA8/T71t9O4fcg01NOjx6NhQ==",
		"nlbi_2269415": "F65bWc5jCBb+5PAVkG5lugAAAAByYO6YLRkxpt5dyZow3g/8",
		"nlbi_2269415_2147483646": "bMipLNYw5Bj4XTVCkG5lugAAAADN2RqV1Wkonslluy7OQgre",
		"nlbi_2271082": "HUW7ZbfjNEglGk8hcbDG1QAAAACZmYI0H2/RCTmdLHNImL+l",
		"reese84": "3:CJTLNY042IM6pV4E3wjwGg==:r4vnOraIJQjKGKcDjCTok0kAnOFq8EXhfr+e+Cldk6WXsFYimjtvDCin4NSICQ9+KiHLJjem3EXytJgSJfThMfjwVCNjeQNs2ulaPqqyRsMRsFQI5nkybH0TgyNMIYrqwpSHxd02nUBiXDNibMIHIhPC4/5wftlPZZHOwzvk/bt7SvNOWG9UEuf3NsEQfJsKJ75/a4S7vpWL/XQ5GXpMXe/CAIKLZ3NQHrwjcARZgMkhm3NFhdL+lf96fyOk5UPee9OcM7e8DyUBQsPG4iu661KAhsRIZNpfdMJDZNle4Ar6MdeqxdJ6zNuJD8B+3ZoUvpi3qrnCXRNnQ/5rpGZiy1lZ1Gm8tUqMfkiKFRjQWsdXwayJ3wo8h39T7Jjx06grq9WsGLxDTyn8a9uet5Tf1wfH7qs88btpH6+okoLhqjspVUELQxIV++AiT9cSJM8G:wXG821uHVE6kWd3zu3ZR89t9nszTTPhWLsxlk40Ngyw=",
		"visid_incap_2269415": "++/VsymETUW8P6Lf71Ff7h9wvWEAAAAAQUIPAAAAAACTDWs+J5rJxLqH2n5NJ15u",
		"visid_incap_2271082": "BgItCMdJTGSmffBuCHvT6rDp/2EAAAAAQUIPAAAAAAAdHxggMbSOat3nCVhHbRmV"
	}


def _mls_scrape_page(
    max_results: int = 100,
    price_min: int = 0,
    price_max: int = 10_000_000,
    property_type: int = 1,
    language: str = "English",
    max_attempts_page: int = 10,
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
        "ApplicationId": "1",
        "Version": "7.0",
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
        # "StoreyRange": "0-0",
        # "BedRange": "0-0",
        # "BathRange": "0-0",
        # Bounding box of the search
        "LongitudeMin": g.west,
        "LongitudeMax": g.east,
        "LatitudeMin": g.south,
        "LatitudeMax": g.north,
        "Sort": "1-A",
        # "SortOrder": "A",
        # "SortBy": "1",
        "viewState": "m",
        "Longitude": g.lng,
        "Latitude": g.lat,
        "ZoomLevel": "11",
    }
    headers = {
        "accept-encoding": 'gzip, deflate, br',
        "user-agent": "Mozilla/5.0 (Linux; Android 12; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.87 Mobile Safari/537.36",
        "referer": "https://www.realtor.ca",
        "origin": "https://www.realtor.ca",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "Android",
        "sec-fetch-site": "same-site",

    }
    uri = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
    logger.info(
        f"Querying MLS listings priced between ${price_min:,.0f} and ${price_max:,.0f}"
    )
    attempts = 0
    while attempts < max_attempts_page:
        attempts += 1
        session = requests.Session()
        r = session.post(uri, data=payload, headers=headers, cookies=_mls_cookie())
        if r.ok:
            results = r.json()["Results"]
            return results
        else:
            # Give a longer sleep between failed attempts
            logger.warning(f"failed for price range {price_min}, {price_max}, attempt: {attempts}")
            time.sleep(5)
    # Give it some time first
    raise RuntimeError(f"failed for price range {price_min}, {price_max} after {attempts} tries")


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
