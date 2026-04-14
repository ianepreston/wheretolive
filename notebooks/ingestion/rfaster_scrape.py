# Databricks notebook source
# MAGIC %md
# MAGIC # Rentfaster Scrape
# MAGIC Scrapes Rentfaster rental listings for Calgary and writes
# MAGIC gzip-compressed JSON to the ingest Volume.
# MAGIC
# MAGIC **Path:** `/Volumes/{catalog}/{schema}/ingest/rfaster/{YYYY-MM-DD}/*.json.gz`

# COMMAND ----------

import datetime as dt
import gzip
import json
import os
import shutil
import time
import urllib.request

# COMMAND ----------

CATALOG = spark.conf.get("spark.databricks.unityCatalog.catalog", "workspace")
SCHEMA = "wheretolive"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/ingest"
LOCAL_TMP = "/local_disk0/tmp/ingest"
SOURCE = "rfaster"
TODAY = f"{dt.date.today():%Y-%m-%d}"
CITY_ID = 1  # Calgary

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

def get_rfaster_page(city_id=1, page=0):
    """Retrieve a single page of Rentfaster listings.

    Returns list of listing dicts, or empty list if page is exhausted.
    """
    url = (
        f"https://www.rentfaster.ca/api/search.json"
        f"?proximity_type=location-city&novacancy=0"
        f"&cur_page={page}&city_id={city_id}"
    )
    try:
        r = urllib.request.urlopen(url, timeout=30)
        data = json.loads(r.read())
        return data.get("listings", [])
    except Exception as e:
        print(f"Rentfaster request failed for page {page}: {e}")
        return []

# COMMAND ----------

def scrape_rfaster_all(city_id=1):
    """Scrape all Rentfaster listings by page pagination.

    Returns the number of batch files written.
    """
    page = 0
    batch_count = 0

    while True:
        listings = get_rfaster_page(city_id=city_id, page=page)
        if not listings:
            break
        filename = f"rfaster_page_{page:04d}"
        path = write_batch(listings, SOURCE, filename)
        batch_count += 1
        print(f"Wrote batch {batch_count}: {path} ({len(listings)} listings)")
        page += 1
        time.sleep(1)  # rate limit -- be polite

    return batch_count

# COMMAND ----------

clean_day_folder(SOURCE, TODAY)
batch_count = scrape_rfaster_all(city_id=CITY_ID)
print(f"\nRentfaster scrape complete: {batch_count} batches written to {VOLUME_BASE}/{SOURCE}/{TODAY}/")

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
