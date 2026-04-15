"""Local scraping + upload to Databricks Volume + pipeline trigger.

Scrapes MLS and Rentfaster listings locally (bypassing cloud IP blocks),
uploads gzip-compressed JSON to the ingest Volume via Databricks SDK,
then triggers the medallion pipeline via databricks bundle run.

Usage:
    pip install databricks-sdk playwright
    python scripts/local_ingest.py

Requires a Databricks CLI profile named 'free-edition' (or set
DATABRICKS_CONFIG_PROFILE=free-edition).
"""

import datetime as dt
import gzip
import json
import os
import random
import subprocess
import sys
import time
import urllib.request
from io import BytesIO
from pathlib import Path

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "free-edition")
W = WorkspaceClient(profile=PROFILE)

TODAY = f"{dt.date.today():%Y-%m-%d}"
CITY_ID = 1  # Calgary
MLS_API_URL = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"

# Discover the Volume path dynamically — DAB dev mode prefixes schema names
CATALOG = "workspace"
VOLUME_BASE = None  # resolved at startup


def discover_volume_path():
    """Find the ingest Volume path by discovering the wheretolive schema."""
    global VOLUME_BASE
    # SDK volumes.list requires schema_name — discover schema first
    # (DAB dev mode prefixes schema names, e.g. dev_ian_e_preston_wheretolive)
    schemas = list(W.schemas.list(catalog_name=CATALOG))
    for s in schemas:
        if "wheretolive" in s.name:
            VOLUME_BASE = f"/Volumes/{CATALOG}/{s.name}/ingest"
            print(f"Discovered Volume: {VOLUME_BASE}")
            return
    # Fallback: try common dev-mode pattern
    VOLUME_BASE = f"/Volumes/{CATALOG}/wheretolive/ingest"
    print(f"Schema discovery failed, using fallback: {VOLUME_BASE}")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def upload_gzip_json(data, source, filename):
    """Gzip-compress JSON and upload to Volume via SDK."""
    compressed = BytesIO()
    with gzip.open(compressed, "wt", encoding="utf-8") as f:
        json.dump(data, f)
    compressed.seek(0)

    volume_path = f"{VOLUME_BASE}/{source}/{TODAY}/{filename}.json.gz"
    W.files.upload(volume_path, compressed, overwrite=True)
    size = compressed.tell()
    print(f"  Uploaded {volume_path} ({size:,} bytes)")
    return volume_path


def clean_day_folder(source):
    """Delete existing files for today to ensure idempotent re-runs."""
    folder = f"{VOLUME_BASE}/{source}/{TODAY}"
    try:
        files = list(W.files.list_directory_contents(folder))
        for f in files:
            W.files.delete(f.path)
        print(f"  Cleaned {len(files)} existing files from {folder}")
    except Exception:
        pass  # folder doesn't exist yet


# ---------------------------------------------------------------------------
# Rentfaster scraping
# ---------------------------------------------------------------------------

def get_rfaster_page(city_id=1, page=0):
    """Retrieve a single page of Rentfaster listings."""
    url = (
        f"https://www.rentfaster.ca/api/search.json"
        f"?proximity_type=location-city&novacancy=0"
        f"&cur_page={page}&city_id={city_id}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.rentfaster.ca",
        "Origin": "https://www.rentfaster.ca",
        "X-Requested-With": "XMLHttpRequest",
    })
    r = urllib.request.urlopen(req, timeout=30)
    data = json.loads(r.read())
    return data.get("listings", [])


def scrape_rentfaster():
    """Scrape all Rentfaster listings and upload to Volume."""
    print(f"\n{'='*60}")
    print(f"Rentfaster scrape — {TODAY}")
    print(f"{'='*60}")
    clean_day_folder("rfaster")

    page = 0
    batch_count = 0
    total_listings = 0

    while True:
        try:
            listings = get_rfaster_page(city_id=CITY_ID, page=page)
        except Exception as e:
            if page == 0:
                print(f"ERROR: First page failed — {e}")
                return 0
            print(f"  Page {page} failed ({e}), stopping pagination")
            break
        if not listings:
            break
        filename = f"rfaster_page_{page:04d}"
        upload_gzip_json(listings, "rfaster", filename)
        batch_count += 1
        total_listings += len(listings)
        page += 1
        time.sleep(2 + random.random())

    print(f"Rentfaster complete: {batch_count} batches, {total_listings} listings")
    return batch_count


# ---------------------------------------------------------------------------
# MLS scraping
# ---------------------------------------------------------------------------

CALGARY_BOUNDS = {"north": 51.21, "south": 50.84, "east": -113.86, "west": -114.32}


def scrape_mls_page(page, price_min=0, price_max=10_000_000):
    """Scrape a single price bracket from MLS API via browser fetch.

    Uses page.evaluate(fetch(...)) so the request inherits the browser's
    TLS fingerprint, cookies, and headers — bypassing Incapsula detection.
    """
    payload = {
        "CultureId": "1",
        "ApplicationId": "1",
        "RecordsPerPage": "100",
        "MaximumResults": "100",
        "PropertySearchTypeId": "1",
        "PriceMin": str(price_min),
        "PriceMax": str(price_max),
        "TransactionTypeId": "2",
        "LongitudeMin": str(CALGARY_BOUNDS["west"]),
        "LongitudeMax": str(CALGARY_BOUNDS["east"]),
        "LatitudeMin": str(CALGARY_BOUNDS["south"]),
        "LatitudeMax": str(CALGARY_BOUNDS["north"]),
        "Sort": "1-A",
    }
    try:
        result = page.evaluate("""async (payload) => {
            const resp = await fetch('https://api2.realtor.ca/Listing.svc/PropertySearch_Post', {
                method: 'POST',
                headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                body: new URLSearchParams(payload).toString(),
            });
            if (!resp.ok) return {_error: true, status: resp.status};
            return await resp.json();
        }""", payload)
        if isinstance(result, dict) and result.get("_error"):
            print(f"  HTTP {result['status']}")
            return None
        return result.get("Results", [])
    except Exception as e:
        print(f"  Browser fetch error: {e}")
        return None


def scrape_mls():
    """Scrape all MLS listings via browser-based API calls and upload to Volume."""
    print(f"\n{'='*60}")
    print(f"MLS scrape — {TODAY}")
    print(f"{'='*60}")
    clean_day_folder("mls")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed — cannot scrape MLS")
        return 0

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    )
    page = context.new_page()

    print("  Navigating to realtor.ca for browser session...")
    page.goto("https://www.realtor.ca", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(5000)
    print("  Browser session established")

    price_min = 0
    batch_count = 0
    total_listings = 0

    try:
        while True:
            results = scrape_mls_page(page, price_min=price_min)
            if results is None:
                print("MLS scraping blocked — stopping")
                break
            if not results:
                break
            top_price = int(round(max(
                float(r["Property"]["PriceUnformattedValue"])
                for r in results
            ), 0))
            filename = f"mls_maxprice_{top_price:08d}"
            upload_gzip_json(results, "mls", filename)
            batch_count += 1
            total_listings += len(results)
            print(f"  Batch {batch_count}: {len(results)} listings, max price ${top_price:,}")

            # Handle per-sqft listings that stall pagination
            next_min = top_price - 1
            if next_min <= price_min:
                if price_min < 1000:
                    print(f"  Price stalled at ${top_price} — jumping to $1,000")
                    next_min = 1000
                else:
                    break
            price_min = next_min
            time.sleep(2 + random.random())
    finally:
        browser.close()
        pw.stop()

    print(f"MLS complete: {batch_count} batches, {total_listings} listings")
    return batch_count


# ---------------------------------------------------------------------------
# Pipeline trigger
# ---------------------------------------------------------------------------

def trigger_pipeline():
    """Trigger the medallion pipeline via databricks bundle run."""
    print(f"\n{'='*60}")
    print("Triggering medallion pipeline")
    print(f"{'='*60}")

    project_root = Path(__file__).resolve().parent.parent
    cmd = [
        "databricks", "bundle", "run", "medallion_pipeline",
        "--profile", PROFILE,
    ]
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=project_root, capture_output=True, text=True)
    if result.returncode == 0:
        print("  Pipeline triggered successfully")
        print(result.stdout[:500] if result.stdout else "")
    else:
        print(f"  Pipeline trigger failed (exit {result.returncode})")
        print(result.stderr[:500] if result.stderr else "")
    return result.returncode


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"WhereToLive local ingest — {TODAY}")
    print(f"Profile: {PROFILE}")

    discover_volume_path()
    if VOLUME_BASE is None:
        print("ERROR: Could not discover Volume path")
        sys.exit(1)

    rfaster_count = scrape_rentfaster()
    mls_count = scrape_mls()

    print(f"\n{'='*60}")
    print(f"Scraping summary")
    print(f"{'='*60}")
    print(f"  Rentfaster: {rfaster_count} batches")
    print(f"  MLS: {mls_count} batches")

    if rfaster_count > 0 or mls_count > 0:
        trigger_pipeline()
    else:
        print("\nNo data scraped — skipping pipeline trigger")

    print("\nDone.")


if __name__ == "__main__":
    main()
