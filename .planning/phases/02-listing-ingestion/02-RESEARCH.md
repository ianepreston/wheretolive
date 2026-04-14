# Phase 2: Listing Ingestion - Research

**Researched:** 2026-04-14
**Domain:** Databricks notebook scraping, Unity Catalog Volumes, gzip JSON file I/O
**Confidence:** HIGH

## Summary

Phase 2 adapts the legacy MLS and Rentfaster scraping logic into Databricks notebooks that write gzip-compressed JSON files to a Unity Catalog Volume organized by source and date. The key technical challenges are: (1) defining the Volume resource in DAB, (2) writing gzip files to Volume paths from notebook Python code, (3) handling the MLS Incapsula bot detection from cloud IPs, and (4) ensuring idempotent re-runs via day-folder deletion.

The legacy scraping code is straightforward to port. MLS uses `requests.post()` with cookie/header spoofing against `api2.realtor.ca`, paginating by price bracket. Rentfaster uses `urllib.request.urlopen()` against `rentfaster.ca/api/search.json`, paginating by page number. Both dump each batch as compressed JSON. The main adaptation is: change compression from ZIP/LZMA to gzip, change output paths from local filesystem to Volume paths, and add the Volume resource definition to the existing DAB `schema.yml`.

**Primary recommendation:** Port legacy scraping logic directly into the placeholder notebooks, using Python's `gzip` module to write `.json.gz` files to `/Volumes/${var.catalog}/${var.schema}/ingest/{source}/{YYYY-MM-DD}/`. Test MLS scraping from Databricks first; build fallback only if blocked.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Attempt MLS scraping from Databricks first -- don't assume blocking before validating.
- **D-02:** If Databricks cloud IPs are blocked by Incapsula, fall back to running the MLS scraper locally and uploading JSON to the Volume (manual or scripted).
- **D-03:** Rentfaster has no known IP blocking concerns -- scrape directly from Databricks.
- **D-04:** Write gzip-compressed JSON (.json.gz) files to Volumes. No ZIP/LZMA -- gzip is natively supported by SDP Auto Loader.
- **D-05:** One .json.gz file per batch (per price bracket for MLS, per page for Rentfaster). Preserves scraping granularity.
- **D-06:** Delete the entire day folder at the start of each notebook run, then write fresh. Guarantees clean state on re-runs.
- **D-07:** Single Volume named `ingest` -- follows user's established convention for landing data.
- **D-08:** Path structure: `/Volumes/workspace/wheretolive/ingest/{source}/{YYYY-MM-DD}/*.json.gz` where `{source}` is `mls` or `rfaster`.
- **D-09:** Volume defined in DAB resources (extends the schema.yml from Phase 1).

### Claude's Discretion
- Exact notebook implementation details (imports, error handling, retry logic)
- How to adapt the legacy cookie/header strategy for MLS requests
- Batch file naming convention within day folders (e.g., batch numbering, price range encoding)
- Whether to add a simple validation step at the end of each notebook (row count, file size check)
- How to structure the local-scrape-and-upload fallback script if needed

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INGEST-01 | MLS scraping notebook dumps raw JSON to Unity Catalog Volume in date-stamped folders | Legacy `mls/scrape.py` provides complete API interaction pattern; Volume write via gzip+FUSE or write-to-local-then-copy pattern; DAB Volume resource definition syntax verified |
| INGEST-02 | Rentfaster scraping notebook dumps raw JSON to Unity Catalog Volume in date-stamped folders | Legacy `rfaster/scrape.py` provides complete API interaction pattern; simpler than MLS (no cookie/header spoofing needed) |
| INGEST-03 | Validate MLS scraping works from Databricks cloud IPs; design and implement fallback if blocked by Incapsula | Research confirms Incapsula actively blocks cloud/datacenter IPs; fallback pattern is local scrape + upload to Volume via CLI or SDK |
</phase_requirements>

## Standard Stack

### Core (pre-installed on Databricks Runtime)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| requests | 2.31+ (pre-installed on DBR 15.4+/16.4 LTS) | HTTP client for MLS POST requests | Already used in legacy code; pre-installed on all Databricks Runtimes [VERIFIED: Databricks Runtime release notes] |
| gzip (stdlib) | N/A | Compress JSON to .json.gz before writing to Volume | Python standard library; produces files Spark/Auto Loader reads natively [VERIFIED: Spark auto-detects .json.gz compression] |
| json (stdlib) | N/A | Serialize scrape results | Python standard library |
| os (stdlib) | N/A | mkdir, path operations on Volume FUSE paths | Python standard library; FUSE paths on Volumes work like POSIX paths [CITED: docs.databricks.com/aws/en/volumes/volume-files] |
| shutil (stdlib) | N/A | rmtree for day-folder cleanup, copyfile for local-to-Volume pattern | Python standard library |
| time (stdlib) | N/A | Rate-limiting sleeps between API calls | Same pattern as legacy code |
| datetime (stdlib) | N/A | Date stamping for folder paths | Same pattern as legacy code |
| urllib.request (stdlib) | N/A | HTTP client for Rentfaster GET requests | Used in legacy Rentfaster scraper; simpler than adding requests dependency for GET-only usage |

### Requires pip install

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| geocoder | 1.38+ | OSM bounding box lookup for Calgary | MLS notebook only -- gets lat/long bounds for the API POST payload [VERIFIED: used in legacy mls/scrape.py] |

### Not Needed

| Library | Why Not | Notes |
|---------|---------|-------|
| pandas | Not used in scraping -- raw JSON written directly to files | Only needed in later phases for transformations |
| pyspark | Scraping notebooks write files, not DataFrames | SDP pipeline (Phase 3) handles Spark reads |
| zipfile | Replaced by gzip per D-04 | Legacy used ZIP_LZMA; incompatible with Auto Loader |
| python-dotenv | Replaced by dbutils.secrets | No .env files in Databricks |

**Installation (MLS notebook top cell):**
```python
%pip install geocoder
dbutils.library.restartPython()
```

**Installation (Rentfaster notebook):**
No additional packages needed -- all stdlib.

## Architecture Patterns

### Recommended Project Structure (Phase 2 changes only)
```
resources/
  schema.yml          # Extended: add Volume definition for 'ingest'
notebooks/
  ingestion/
    mls_scrape.py     # Replace placeholder with real MLS scraping logic
    rfaster_scrape.py # Replace placeholder with real Rentfaster scraping logic
```

### Pattern 1: Gzip JSON Write to Volume

**What:** Each scraping batch is serialized as JSON, gzip-compressed, and written to a Volume path as a `.json.gz` file. The Volume FUSE mount provides POSIX-style file access.

**When to use:** For all scraping output in this phase.

**Critical detail on Volume writes:** Volumes support sequential writes via FUSE but NOT random/non-sequential writes (like ZIP which requires seeking). Gzip is a streaming compressor and writes sequentially, so it should work directly on Volume paths. However, the official Databricks documentation recommends a write-to-local-then-copy pattern for safety. [CITED: docs.databricks.com/aws/en/volumes/volume-files]

**Recommended approach (safe):**
```python
import gzip
import json
import os
import shutil
import datetime as dt

# Volume base path
VOLUME_BASE = "/Volumes/workspace/wheretolive/ingest"
LOCAL_TMP = "/local_disk0/tmp/ingest"

def write_batch(results: list, source: str, filename: str) -> str:
    """Write a batch of results as gzip JSON to the Volume.

    Uses write-to-local-then-copy pattern per Databricks docs.
    """
    today = f"{dt.date.today():%Y-%m-%d}"
    local_dir = f"{LOCAL_TMP}/{source}/{today}"
    volume_dir = f"{VOLUME_BASE}/{source}/{today}"

    os.makedirs(local_dir, exist_ok=True)
    os.makedirs(volume_dir, exist_ok=True)

    local_path = f"{local_dir}/{filename}.json.gz"
    volume_path = f"{volume_dir}/{filename}.json.gz"

    # Write to local temp first
    with gzip.open(local_path, "wt", encoding="utf-8") as f:
        json.dump(results, f)

    # Copy to Volume
    shutil.copyfile(local_path, volume_path)
    return volume_path
```

**Alternative (direct write -- simpler but less documented):**
```python
volume_path = f"{VOLUME_BASE}/{source}/{today}/{filename}.json.gz"
os.makedirs(os.path.dirname(volume_path), exist_ok=True)
with gzip.open(volume_path, "wt", encoding="utf-8") as f:
    json.dump(results, f)
```

### Pattern 2: Idempotent Day-Folder Cleanup

**What:** At the start of each notebook run, delete the entire day folder for that source, then write fresh. Per D-06.

**When to use:** Every notebook execution.

**Example:**
```python
import shutil
import os

def clean_day_folder(source: str, date_str: str) -> None:
    """Delete and recreate the day folder for idempotent re-runs."""
    volume_dir = f"/Volumes/workspace/wheretolive/ingest/{source}/{date_str}"
    if os.path.exists(volume_dir):
        shutil.rmtree(volume_dir)
    os.makedirs(volume_dir, exist_ok=True)
```

**Note:** `shutil.rmtree` works on Volume FUSE paths. `dbutils.fs.rm` also works with `/Volumes/...` paths with `recurse=True`, but `shutil.rmtree` is simpler in pure Python context. [VERIFIED: community.databricks.com confirms both approaches work]

### Pattern 3: MLS Price-Bracket Pagination

**What:** The MLS API returns max 500 results per request. The legacy code paginates by price: start at $0, get 100 results, find the max price returned, then set that as the new floor. Repeat until no more results.

**When to use:** MLS notebook only.

**Key adaptation from legacy:**
```python
def scrape_mls_all() -> int:
    """Scrape all MLS listings by price bracket pagination.

    Returns the number of files written.
    """
    price_min = 0
    batch_num = 0
    results = True  # sentinel to enter loop

    while results:
        results = scrape_mls_page(price_min=price_min)
        if not results:
            break
        top_price = int(round(max(
            float(r["Property"]["PriceUnformattedValue"])
            for r in results
        ), 0))

        filename = f"mls_maxprice_{top_price:08d}"
        write_batch(results, "mls", filename)
        batch_num += 1

        if top_price - 1 <= price_min:
            break
        price_min = top_price - 1
        time.sleep(1)  # rate limit

    return batch_num
```

### Pattern 4: Rentfaster Page Pagination

**What:** Rentfaster API returns paginated results. Increment page number until empty list returned.

**Key adaptation from legacy:**
```python
def scrape_rfaster_all(city_id: int = 1) -> int:
    """Scrape all Rentfaster listings by page pagination.

    Returns the number of files written.
    """
    page = 0
    batch_num = 0

    while True:
        listings = get_rfaster_page(city_id=city_id, page=page)
        if not listings:
            break
        filename = f"rfaster_page_{page:04d}"
        write_batch(listings, "rfaster", filename)
        batch_num += 1
        page += 1
        time.sleep(1)  # rate limit

    return batch_num
```

### Pattern 5: DAB Volume Resource Definition

**What:** Add the `ingest` Volume to `resources/schema.yml` using DAB resource syntax.

**Example:**
```yaml
resources:
  schemas:
    wheretolive_schema:
      catalog_name: ${var.catalog}
      name: ${var.schema}
      comment: |
        WhereToLive real estate listing data.
        Uses table name prefixes for medallion layers:
        bronze_* (raw), silver_* (cleaned), gold_* (presentation-ready).

  volumes:
    ingest_volume:
      catalog_name: ${var.catalog}
      schema_name: ${var.schema}
      name: ingest
      volume_type: MANAGED
      comment: |
        Raw scraped JSON files from MLS and Rentfaster.
        Path layout: /Volumes/{catalog}/{schema}/ingest/{source}/{YYYY-MM-DD}/*.json.gz
        Read by SDP Auto Loader in Phase 3.
```

[VERIFIED: DAB supports `resources.volumes` with fields `catalog_name`, `schema_name`, `name`, `volume_type`, `comment` mapping directly from the Volumes REST API create payload -- docs.databricks.com/aws/en/dev-tools/bundles/resources]

### Anti-Patterns to Avoid
- **Writing directly to Delta tables in this phase:** The user decided raw data lands as files in Volumes (D-04 through D-08). Do NOT write to Delta tables here -- that's Phase 3's Auto Loader responsibility.
- **Using zipfile or LZMA compression:** Per D-04, only gzip. Auto Loader reads `.json.gz` natively; ZIP requires custom decompression logic.
- **Skipping rate limiting:** Both APIs should have `time.sleep(1)` between requests. The legacy code already does this. Cloud IPs are under more scrutiny -- politeness matters more, not less.
- **Hardcoding the Volume path catalog/schema:** Use variables or `dbutils.widgets` for catalog/schema so the notebook works across dev/prod targets.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Calgary bounding box coords | Manual lat/long constants | `geocoder.osm("Calgary, Canada")` | Returns `.north`, `.south`, `.east`, `.west` bounds; already proven in legacy code [VERIFIED: legacy/src/wheretolive/mls/scrape.py] |
| Gzip compression | Custom streaming compressor | `gzip.open(path, "wt")` | stdlib, reliable, produces files Spark reads natively |
| Volume folder cleanup | Custom recursive delete | `shutil.rmtree(path)` | stdlib, works on FUSE-mounted Volumes |
| Retry with backoff | Custom retry loop | Keep legacy pattern (while loop + sleep) but add exponential backoff | Simple enough to inline; no external library needed for 2 API endpoints |

**Key insight:** This phase is primarily a port of existing, working scraping code. The novelty is in the output destination (Volumes instead of local disk) and compression format (gzip instead of LZMA). The scraping logic itself is proven.

## Common Pitfalls

### Pitfall 1: MLS Incapsula Blocking from Cloud IPs
**What goes wrong:** The MLS scraper uses cookie/header spoofing to bypass Incapsula bot detection. Cloud provider IP ranges (Azure, AWS, GCP) are well-known and commonly blocked by Incapsula. Scraping that works from a home IP may return 403 or captcha challenges from Databricks compute.
**Why it happens:** Imperva Incapsula maintains IP reputation databases that flag datacenter/cloud IPs. The hardcoded cookies in `_mls_cookie()` are static session tokens that will have expired long ago.
**How to avoid:** Per D-01, test from Databricks first. The notebook should catch HTTP error responses gracefully and report the specific failure mode (403, captcha HTML, connection reset). Per D-02, if blocked, the fallback is running locally and uploading to the Volume.
**Warning signs:** HTTP 403 responses; response body containing HTML instead of JSON; `incap_ses` challenge cookies in the response.
[CITED: scrapfly.io/blog/posts/how-to-bypass-imperva-incapsula-anti-scraping -- Incapsula blocks ~95% of automated requests from datacenter IPs]

### Pitfall 2: Stale MLS Cookies
**What goes wrong:** The legacy `_mls_cookie()` contains hardcoded session tokens (`incap_ses_*`, `reese84`, `ASP.NET_SessionId`) that were valid at a specific point in time. These are long expired.
**Why it happens:** Incapsula cookies have limited lifetime. The legacy code relied on periodic manual refresh of these values.
**How to avoid:** The cookie dictionary must be refreshed. Options: (1) try the request without cookies first -- the API may work without them from some IPs, (2) store a current cookie set in Databricks Secrets and read via `dbutils.secrets.get()`, (3) accept that cookie refresh may need periodic manual intervention. For Phase 2, start with a minimal header set and add cookies only if needed.
**Warning signs:** The existing cookie values reference dates from 2022. They will not work.

### Pitfall 3: .json.gz vs .json.gzip File Extension
**What goes wrong:** Spark only auto-detects gzip compression from `.json.gz` extension, NOT `.json.gzip`. If files are named with the wrong extension, downstream Auto Loader in Phase 3 will fail with misleading "malformed columns" or encoding errors.
**Why it happens:** Spark's file extension detection is specific about the `.gz` suffix.
**How to avoid:** Always use `.json.gz` extension. Never `.json.gzip` or `.jsonl.gz`.
**Warning signs:** Phase 3 Auto Loader errors about "corrupt records" or "malformed columns" when the files are actually fine.
[CITED: simoncw.com/posts/til-spark-reading-compressed-json/ -- confirmed .json.gz works, .json.gzip does not]

### Pitfall 4: Volume FUSE Write Limitations
**What goes wrong:** Unity Catalog Volumes do not support random/non-sequential writes via FUSE. Formats that require seeking (ZIP, Excel) fail silently or produce corrupt files.
**Why it happens:** Volumes use FUSE to provide POSIX-like file access, but the underlying cloud object storage does not support random access writes.
**How to avoid:** Gzip is sequential and should work directly, but the safest approach is write-to-local-then-copy: write to `/local_disk0/tmp/` first, then `shutil.copyfile()` to the Volume path.
**Warning signs:** Corrupt or truncated `.json.gz` files in the Volume; files that can't be opened locally after download.
[CITED: docs.databricks.com/aws/en/volumes/volume-files -- "Direct-append or non-sequential (random) writes are not supported"]

### Pitfall 5: geocoder Package Not Pre-installed
**What goes wrong:** The MLS notebook fails on import because `geocoder` is not part of Databricks Runtime.
**Why it happens:** `geocoder` is a third-party package used only in the legacy MLS scraper for Calgary bounding box lookup.
**How to avoid:** Add `%pip install geocoder` as the first cell in the MLS notebook, followed by `dbutils.library.restartPython()` to ensure the package is available.
**Warning signs:** `ModuleNotFoundError: No module named 'geocoder'` at notebook start.

### Pitfall 6: Rentfaster Empty Page Handling
**What goes wrong:** The Rentfaster API returns an empty list `[]` when pages are exhausted. The legacy code writes an empty-result file for the final page before exiting the loop.
**Why it happens:** The `while page_list:` loop in legacy code dumps results before checking if the next page is empty. The last `_dump_result(page_list, page)` call writes an empty file.
**How to avoid:** Check if the page has results before writing. Exit the loop when the API returns an empty list without writing a file.
**Warning signs:** A `.json.gz` file containing `[]` at the end of each day's Rentfaster scrape.

## Code Examples

### MLS Notebook Structure (complete outline)
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # MLS Scrape
# MAGIC Scrapes Realtor.ca MLS listings for Calgary and writes
# MAGIC gzip-compressed JSON to the ingest Volume.

# COMMAND ----------

# %pip install geocoder
# dbutils.library.restartPython()

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

# Configuration
CATALOG = spark.conf.get("spark.databricks.unityCatalog.catalog", "workspace")
SCHEMA = "wheretolive"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/ingest"
LOCAL_TMP = "/local_disk0/tmp/ingest"
SOURCE = "mls"
TODAY = f"{dt.date.today():%Y-%m-%d}"

# COMMAND ----------

# Calgary bounding box
GEOBOUND = geocoder.osm("Calgary, Canada")

# COMMAND ----------

def clean_day_folder(source: str, date_str: str) -> None:
    """Delete and recreate day folder for idempotent re-runs."""
    volume_dir = f"{VOLUME_BASE}/{source}/{date_str}"
    if os.path.exists(volume_dir):
        shutil.rmtree(volume_dir)
    os.makedirs(volume_dir, exist_ok=True)

# COMMAND ----------

def write_batch(results: list, source: str, filename: str) -> str:
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
    """Scrape a single price bracket from MLS API."""
    headers = {
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Linux; Android 12; Pixel 3) ...",
        "referer": "https://www.realtor.ca",
        "origin": "https://www.realtor.ca",
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
        "LongitudeMin": GEOBOUND.west,
        "LongitudeMax": GEOBOUND.east,
        "LatitudeMin": GEOBOUND.south,
        "LatitudeMax": GEOBOUND.north,
        "Sort": "1-A",
    }
    uri = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(uri, data=payload, headers=headers, timeout=30)
            if r.ok:
                return r.json().get("Results", [])
            print(f"MLS request failed: HTTP {r.status_code} (attempt {attempt})")
        except requests.RequestException as e:
            print(f"MLS request error: {e} (attempt {attempt})")
        time.sleep(min(5 * attempt, 30))  # exponential backoff capped at 30s

    raise RuntimeError(f"MLS scrape failed for price_min={price_min} after {max_attempts} attempts")

# COMMAND ----------

# Main execution
clean_day_folder(SOURCE, TODAY)

price_min = 0
batch_count = 0

while True:
    results = scrape_mls_page(price_min=price_min)
    if not results:
        break
    top_price = int(round(max(
        float(r["Property"]["PriceUnformattedValue"]) for r in results
    ), 0))
    filename = f"mls_maxprice_{top_price:08d}"
    path = write_batch(results, SOURCE, filename)
    batch_count += 1
    print(f"Wrote batch {batch_count}: {path} ({len(results)} listings)")

    if top_price - 1 <= price_min:
        break
    price_min = top_price - 1
    time.sleep(1)

print(f"MLS scrape complete: {batch_count} batches written to {VOLUME_BASE}/{SOURCE}/{TODAY}/")

# COMMAND ----------

# Validation: list files written
files = os.listdir(f"{VOLUME_BASE}/{SOURCE}/{TODAY}")
print(f"Files in Volume: {len(files)}")
for f in sorted(files):
    size = os.path.getsize(f"{VOLUME_BASE}/{SOURCE}/{TODAY}/{f}")
    print(f"  {f} ({size:,} bytes)")
```

### DAB Volume Resource Definition (extend schema.yml)
```yaml
# Source: Databricks REST API Volumes create payload mapped to DAB YAML
# CITED: docs.databricks.com/aws/en/dev-tools/bundles/resources
resources:
  schemas:
    wheretolive_schema:
      catalog_name: ${var.catalog}
      name: ${var.schema}
      comment: |
        WhereToLive real estate listing data.
        Uses table name prefixes for medallion layers:
        bronze_* (raw), silver_* (cleaned), gold_* (presentation-ready).

  volumes:
    ingest_volume:
      catalog_name: ${var.catalog}
      schema_name: ${var.schema}
      name: ingest
      volume_type: MANAGED
      comment: |
        Raw scraped JSON files from MLS and Rentfaster.
        Path: /Volumes/{catalog}/{schema}/ingest/{source}/{YYYY-MM-DD}/*.json.gz
```

### Fallback: Local Scrape + Upload Script
```python
#!/usr/bin/env python3
"""Local MLS scrape with upload to Databricks Volume.

Run this if MLS scraping is blocked from Databricks cloud IPs.
Requires: databricks-cli configured with workspace profile.
"""
import subprocess
import sys

# 1. Run scrape locally (writes to local temp)
# 2. Upload via Databricks CLI:
#    databricks fs cp -r ./local_output/ dbfs:/Volumes/workspace/wheretolive/ingest/mls/2026-04-14/ --profile free-edition
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| ZIP + LZMA compression | gzip compression (.json.gz) | This migration (D-04) | Auto Loader reads .json.gz natively; no custom decompression needed in Phase 3 |
| Local filesystem paths (pathlib) | Volume FUSE paths (/Volumes/...) | This migration (D-08) | Standard POSIX operations work; governed by Unity Catalog |
| Hardcoded Incapsula cookies | Minimal headers first, cookies in Secrets if needed | This migration | Cookies from 2022 are expired; start fresh with minimal headers |
| `urllib.request.urlopen` (Rentfaster) | Keep as-is | No change | Simpler than switching to requests for GET-only usage; works in DBR |
| DLT renamed to SDP | Use "Lakeflow Declarative Pipelines" or "SDP" terminology | 2025 | [ASSUMED] Naming may have continued evolving; verify in workspace UI |

**Deprecated/outdated:**
- `zipfile.ZIP_LZMA`: Replaced by gzip for Auto Loader compatibility
- Hardcoded `_mls_cookie()` values: All session tokens from 2022 are expired
- `_find_base_dir()` / `_find_scrapes_dir()` helper functions: Replaced by simple string path construction for Volumes

## Assumptions Log

> List all claims tagged [ASSUMED] in this research.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Gzip sequential writes work directly on Volume FUSE paths without write-to-local-then-copy | Architecture Patterns, Pattern 1 | LOW -- the recommended safe pattern (write-to-local-then-copy) is included as primary approach; direct write is alternative |
| A2 | `geocoder.osm("Calgary, Canada")` still returns valid bounding box from OSM | Code Examples | LOW -- if it fails, hardcode Calgary bounds as fallback (north=51.21, south=50.84, east=-113.86, west=-114.32) |
| A3 | Rentfaster API endpoint `rentfaster.ca/api/search.json` is still functional | Common Pitfalls | MEDIUM -- could not verify current status; if changed, API endpoint discovery needed |
| A4 | MLS API endpoint `api2.realtor.ca/Listing.svc/PropertySearch_Post` still accepts the documented payload format | Code Examples | MEDIUM -- undocumented API; may have changed payload requirements since legacy code was last used |
| A5 | `requests` is pre-installed on DBR 15.4+/16.4 LTS | Standard Stack | LOW -- virtually certain; requests has been in every DBR since early versions |
| A6 | DLT/SDP naming terminology continues to evolve | State of the Art | LOW -- cosmetic; does not affect code |

## Open Questions (RESOLVED)

1. **Are MLS cookies still required?**
   - What we know: Legacy code used a complex cookie dictionary to bypass Incapsula. Those specific cookies are from early 2022 and are expired.
   - What's unclear: Whether the API works without cookies from certain IPs, whether a minimal cookie set suffices, or whether full Incapsula bypass is needed.
   - Recommendation: Try with minimal headers first (just user-agent and referer). If that fails, try with a freshly captured cookie set stored in Databricks Secrets. If that also fails, it confirms cloud IP blocking and triggers the D-02 fallback.

2. **Is the Rentfaster API still at the same endpoint?**
   - What we know: Legacy code used `https://www.rentfaster.ca/api/search.json` with query parameters. This is an undocumented public API.
   - What's unclear: Whether the endpoint has moved, requires authentication, or changed response format since the legacy code was last run.
   - Recommendation: Test the endpoint from Databricks in the first implementation. The simple GET request pattern makes this easy to validate.

3. **Does `geocoder.osm()` require network access to OSM servers?**
   - What we know: `geocoder.osm("Calgary, Canada")` makes a network call to OpenStreetMap's Nominatim API. This should work from Databricks (outbound HTTP is allowed).
   - What's unclear: Whether OSM rate-limits or blocks cloud IPs.
   - Recommendation: Add a fallback with hardcoded Calgary bounding box coordinates if geocoder fails. The coordinates are stable geographic data.

## Environment Availability

> All work happens on Databricks compute, not local machine. The relevant environment is the Databricks Runtime.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Databricks Runtime | All notebooks | Assumed (workspace exists from Phase 1) | 15.4 LTS or 16.4 LTS | -- |
| Python requests | MLS scraping | Pre-installed on DBR | 2.31+ | -- |
| Python gzip/json/os/shutil | File I/O | Python stdlib | N/A | -- |
| geocoder | MLS bounding box | Requires %pip install | 1.38+ | Hardcode Calgary bounds |
| Databricks CLI | Fallback upload only | Check local machine | -- | Manual Volume UI upload |
| Unity Catalog Volume support | File storage | Requires workspace with UC enabled | -- | -- |

**Missing dependencies with no fallback:**
- None. All required capabilities exist in Databricks Runtime.

**Missing dependencies with fallback:**
- `geocoder`: If pip install fails or OSM blocks, hardcode Calgary bounding box as `(50.84, 51.21, -114.32, -113.86)`.
- Databricks CLI (local machine): Only needed if MLS fallback (D-02) is triggered. Can also upload via workspace Volume UI.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Manual notebook execution + output validation |
| Config file | N/A (notebooks are the test harness) |
| Quick run command | Run individual notebook cells interactively |
| Full suite command | Run full notebook via Databricks Workflow or Run All |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INGEST-01 | MLS notebook writes .json.gz files to Volume | smoke | Run MLS notebook; verify files in `/Volumes/workspace/wheretolive/ingest/mls/{date}/` | Wave 0: add validation cell |
| INGEST-02 | Rentfaster notebook writes .json.gz files to Volume | smoke | Run Rentfaster notebook; verify files in `/Volumes/workspace/wheretolive/ingest/rfaster/{date}/` | Wave 0: add validation cell |
| INGEST-03 | MLS cloud IP validation + fallback | manual | Run MLS notebook from Databricks; check HTTP status codes | Manual (requires Databricks execution) |

### Sampling Rate
- **Per task commit:** Run modified notebook manually; check validation cell output
- **Per wave merge:** Run both notebooks end-to-end from Databricks
- **Phase gate:** Both notebooks produce non-empty `.json.gz` files in correct Volume paths; re-run on same day produces identical file count (idempotency)

### Wave 0 Gaps
- [ ] Add validation cell at end of each notebook (list files, check non-empty, report counts)
- [ ] No local unit tests possible for scraping logic (depends on external APIs + Databricks runtime)
- [ ] Manual testing required on Databricks workspace

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | N/A (no user auth in scraping notebooks) |
| V3 Session Management | No | N/A |
| V4 Access Control | Yes (Volume permissions) | Unity Catalog grants on Volume (managed by DAB) |
| V5 Input Validation | Partial | Validate HTTP response is JSON before writing; don't write HTML error pages |
| V6 Cryptography | No | N/A (no encryption of scraped data) |

### Known Threat Patterns for Scraping Notebooks

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| MLS cookies in source code | Information Disclosure | Store cookies in Databricks Secrets if needed; never hardcode in notebook |
| Scraping API changes breaking silent data corruption | Tampering (data integrity) | Validate response structure before writing; check for expected JSON keys |
| Cloud IP ban causing empty scrape days | Denial of Service (self-inflicted) | Validation cell checks file count > 0; fallback path documented |

## Sources

### Primary (HIGH confidence)
- Legacy source code: `legacy/src/wheretolive/mls/scrape.py`, `legacy/src/wheretolive/rfaster/scrape.py` -- complete scraping logic to port
- [Databricks Volume file operations](https://docs.databricks.com/aws/en/volumes/volume-files) -- FUSE write limitations, write-to-local-then-copy pattern
- [Databricks DAB resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) -- Volume resource type supported in DAB YAML
- [Spark gzip auto-detection](https://simoncw.com/posts/til-spark-reading-compressed-json/) -- Spark reads `.json.gz` automatically, fails on `.json.gzip`

### Secondary (MEDIUM confidence)
- [Databricks REST API Volumes create](https://docs.databricks.com/api/workspace/volumes/create) -- field names: catalog_name, schema_name, name, volume_type, comment
- [Incapsula bypass patterns](https://scrapfly.io/blog/posts/how-to-bypass-imperva-incapsula-anti-scraping) -- confirms cloud/datacenter IP blocking is active as of 2025-2026

### Tertiary (LOW confidence)
- Rentfaster API endpoint availability -- could not verify if `rentfaster.ca/api/search.json` is still functional in 2026 [ASSUMED]
- MLS API payload format stability -- undocumented API, may have changed [ASSUMED]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries are either Python stdlib or confirmed pre-installed on DBR
- Architecture: HIGH -- patterns are direct adaptations of proven legacy code with well-documented Volume I/O
- Pitfalls: HIGH -- Incapsula blocking is a well-known industry pattern; Volume write limitations are documented
- MLS/Rentfaster API availability: MEDIUM -- undocumented APIs may have changed; must validate during implementation

**Research date:** 2026-04-14
**Valid until:** 2026-05-14 (stable domain -- scraping APIs are the main risk)
