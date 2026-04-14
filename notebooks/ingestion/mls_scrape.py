# Databricks notebook source
# MAGIC %md
# MAGIC # MLS Scrape
# MAGIC Scrapes Realtor.ca MLS listings for Calgary and writes
# MAGIC gzip-compressed JSON to the ingest Volume.
# MAGIC
# MAGIC **Path:** `/Volumes/{catalog}/{schema}/ingest/mls/{YYYY-MM-DD}/*.json.gz`
# MAGIC
# MAGIC Uses price-bracket pagination: starts at $0, fetches 100 results,
# MAGIC finds max price returned, sets that as new floor, repeats until exhausted.

# COMMAND ----------

%pip install geocoder
dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt
import gzip
import json
import os
import shutil
import time

import geocoder
import requests

# COMMAND ----------

CATALOG = "workspace"
# DAB dev mode prefixes schema names — discover the actual deployed name
_schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE '*wheretolive*'").collect()]
SCHEMA = _schemas[0] if _schemas else "wheretolive"
print(f"Using catalog={CATALOG}, schema={SCHEMA}")
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/ingest"
LOCAL_TMP = "/local_disk0/tmp/ingest"
SOURCE = "mls"
TODAY = f"{dt.date.today():%Y-%m-%d}"
MLS_API_URL = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"

# COMMAND ----------

try:
    GEOBOUND = geocoder.osm("Calgary, Canada")
    BOUNDS = {
        "north": GEOBOUND.north,
        "south": GEOBOUND.south,
        "east": GEOBOUND.east,
        "west": GEOBOUND.west,
    }
    print(f"Calgary bounds from OSM: {BOUNDS}")
except Exception as e:
    print(f"Geocoder failed ({e}), using hardcoded Calgary bounds")
    BOUNDS = {
        "north": 51.21,
        "south": 50.84,
        "east": -113.86,
        "west": -114.32,
    }

# COMMAND ----------

def clean_day_folder(source, date_str):
    """Delete and recreate the day folder for idempotent re-runs."""
    volume_dir = f"{VOLUME_BASE}/{source}/{date_str}"
    if os.path.exists(volume_dir):
        shutil.rmtree(volume_dir)
    os.makedirs(volume_dir, exist_ok=True)

# COMMAND ----------

def write_batch(results, source, filename):
    """Write gzip-compressed JSON to Volume via local temp."""
    local_dir = f"{LOCAL_TMP}/{source}/{TODAY}"
    volume_dir = f"{VOLUME_BASE}/{source}/{TODAY}"
    os.makedirs(local_dir, exist_ok=True)
    os.makedirs(volume_dir, exist_ok=True)

    local_path = f"{local_dir}/{filename}.json.gz"
    volume_path = f"{volume_dir}/{filename}.json.gz"

    with gzip.open(local_path, "wt", encoding="utf-8") as f:
        json.dump(results, f)
    shutil.copyfile(local_path, volume_path)
    return volume_path

# COMMAND ----------

def scrape_mls_page(price_min=0, price_max=10_000_000, max_attempts=10):
    """Scrape a single price bracket from MLS API.

    Returns list of listing dicts, or empty list if no results.
    Raises RuntimeError with diagnostic info if blocked or failed.
    """
    headers = {
        "accept-encoding": "gzip, deflate, br",
        "user-agent": (
            "Mozilla/5.0 (Linux; Android 12; Pixel 3) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/98.0.4758.87 Mobile Safari/537.36"
        ),
        "referer": "https://www.realtor.ca",
        "origin": "https://www.realtor.ca",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "Android",
        "sec-fetch-site": "same-site",
    }
    payload = {
        "CultureId": "1",
        "ApplicationId": "1",
        "RecordsPerPage": 100,
        "MaximumResults": 100,
        "PropertySearchTypeId": 1,
        "PriceMin": price_min,
        "PriceMax": price_max,
        "TransactionTypeId": "2",
        "LongitudeMin": BOUNDS["west"],
        "LongitudeMax": BOUNDS["east"],
        "LatitudeMin": BOUNDS["south"],
        "LatitudeMax": BOUNDS["north"],
        "Sort": "1-A",
    }

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(MLS_API_URL, data=payload, headers=headers, timeout=30)

            if r.ok:
                # Check if response is actually JSON (Incapsula returns HTML)
                content_type = r.headers.get("content-type", "")
                if "json" not in content_type and "javascript" not in content_type:
                    body_preview = r.text[:500]
                    if "incap" in body_preview.lower() or "<html" in body_preview.lower():
                        raise RuntimeError(
                            f"INCAPSULA BLOCK DETECTED: Response is HTML, not JSON. "
                            f"Content-Type: {content_type}. "
                            f"Body preview: {body_preview[:200]}... "
                            f"Cloud IP is likely blocked. "
                            f"Fallback: run MLS scraper locally and upload to Volume (see D-02)."
                        )
                try:
                    data = r.json()
                    return data.get("Results", [])
                except json.JSONDecodeError:
                    print(f"MLS response is not valid JSON (attempt {attempt})")
                    print(f"Content-Type: {content_type}")
                    print(f"Body preview: {r.text[:300]}")
            else:
                print(f"MLS request failed: HTTP {r.status_code} (attempt {attempt})")
                if r.status_code == 403:
                    print(
                        f"HTTP 403 Forbidden -- likely Incapsula blocking cloud IP. "
                        f"Response headers: {dict(r.headers)}"
                    )
        except requests.RequestException as e:
            print(f"MLS request error: {e} (attempt {attempt})")

        backoff = min(5 * attempt, 30)
        print(f"Retrying in {backoff}s...")
        time.sleep(backoff)

    raise RuntimeError(
        f"MLS scrape failed for price_min={price_min} after {max_attempts} attempts. "
        f"If this is due to Incapsula blocking, run locally and upload to Volume (D-02 fallback)."
    )

# COMMAND ----------

def scrape_mls_all():
    """Scrape all MLS listings by price bracket pagination.

    Returns the number of batch files written.
    """
    price_min = 0
    batch_count = 0

    while True:
        results = scrape_mls_page(price_min=price_min)
        if not results:
            break
        top_price = int(round(max(
            float(r["Property"]["PriceUnformattedValue"])
            for r in results
        ), 0))
        filename = f"mls_maxprice_{top_price:08d}"
        path = write_batch(results, SOURCE, filename)
        batch_count += 1
        print(f"Wrote batch {batch_count}: {path} ({len(results)} listings, max price ${top_price:,})")

        # Advance pagination: if max price didn't move (per-sqft listings
        # like vacant land return prices < $1), jump to $1000 to skip past them
        next_min = top_price - 1
        if next_min <= price_min:
            if price_min < 1000:
                print(f"  Price stalled at ${top_price} — jumping to $1,000 to skip per-sqft listings")
                next_min = 1000
            else:
                break
        price_min = next_min
        time.sleep(1)  # rate limit -- be polite

    return batch_count

# COMMAND ----------

print(f"Attempting MLS scrape from Databricks (per D-01: validate before assuming IP blocking)")
print(f"Date: {TODAY}")
print(f"Volume path: {VOLUME_BASE}/{SOURCE}/{TODAY}/")
print()

clean_day_folder(SOURCE, TODAY)

try:
    batch_count = scrape_mls_all()
    print(f"\nMLS scrape complete: {batch_count} batches written to {VOLUME_BASE}/{SOURCE}/{TODAY}/")
except RuntimeError as e:
    print(f"\n{'='*60}")
    print(f"MLS SCRAPE FAILED")
    print(f"{'='*60}")
    print(f"Error: {e}")
    print(f"\nIf Incapsula is blocking cloud IPs, use the local fallback:")
    print(f"  1. Run MLS scraper on local machine")
    print(f"  2. Upload .json.gz files to: /Volumes/{CATALOG}/{SCHEMA}/ingest/mls/{TODAY}/")
    print(f"  3. Use: databricks fs cp -r ./local_output/ dbfs:/Volumes/{CATALOG}/{SCHEMA}/ingest/mls/{TODAY}/ --profile free-edition")
    dbutils.notebook.exit(json.dumps({"status": "BLOCKED", "error": str(e)}))

# COMMAND ----------

volume_dir = f"{VOLUME_BASE}/{SOURCE}/{TODAY}"
if os.path.exists(volume_dir):
    files = sorted(os.listdir(volume_dir))
    print(f"Files in Volume: {len(files)}")
    total_bytes = 0
    for fname in files:
        size = os.path.getsize(f"{volume_dir}/{fname}")
        total_bytes += size
        print(f"  {fname} ({size:,} bytes)")
    print(f"\nTotal: {len(files)} files, {total_bytes:,} bytes")
    assert len(files) > 0, "ERROR: No files written -- scrape produced no results"
    assert all(f.endswith(".json.gz") for f in files), "ERROR: Non-.json.gz files found"
else:
    print(f"WARNING: Volume directory does not exist: {volume_dir}")
    print("This may indicate the scrape was blocked -- check the execution cell above for details.")
