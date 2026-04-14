---
phase: 02-listing-ingestion
plan: 02
subsystem: ingestion
tags: [mls, scraping, realtor-ca, gzip, volumes, databricks-notebook, incapsula, geocoder]

# Dependency graph
requires:
  - phase: 01-infrastructure-dab-setup
    provides: DAB project structure with schema and notebook paths
provides:
  - MLS scraping notebook with price-bracket pagination writing gzip JSON to ingest Volume
  - Incapsula IP blocking detection with fallback instructions
  - Idempotent day-folder cleanup pattern for re-runs
affects: [02-03, 03-pipeline]

# Tech tracking
tech-stack:
  added: [geocoder (pip install in notebook)]
  patterns: [write-to-local-then-copy for Volume FUSE, price-bracket pagination, Databricks notebook cell format]

key-files:
  created: []
  modified: [notebooks/ingestion/mls_scrape.py]

key-decisions:
  - "Used requests.post directly instead of requests.Session to keep scraping stateless (no cookies)"
  - "Hardcoded Calgary bounding box fallback if geocoder.osm fails from cloud IP"
  - "Exponential backoff capped at 30s for MLS API retries"
  - "File naming encodes max price (mls_maxprice_00000000) for traceability"

patterns-established:
  - "Write-to-local-then-copy: gzip.open to /local_disk0/tmp then shutil.copyfile to Volume path"
  - "Idempotent day-folder cleanup: shutil.rmtree + os.makedirs at notebook start"
  - "Databricks notebook cell format: # COMMAND ---------- separators"
  - "Incapsula detection: check Content-Type and body for HTML/incap before accepting response"

requirements-completed: [INGEST-01, INGEST-03]

# Metrics
duration: 1min
completed: 2026-04-14
---

# Phase 2 Plan 02: MLS Scraping Notebook Summary

**MLS scraping notebook with price-bracket pagination, gzip JSON output to Unity Catalog Volume, and Incapsula cloud IP blocking detection with fallback instructions**

## Performance

- **Duration:** 1 min 23s
- **Started:** 2026-04-14T16:22:37Z
- **Completed:** 2026-04-14T16:24:00Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Complete MLS scraping notebook replacing placeholder, adapted from legacy mls/scrape.py
- Price-bracket pagination fetching all Calgary listings via api2.realtor.ca POST API
- Gzip-compressed JSON output to /Volumes/{catalog}/{schema}/ingest/mls/{date}/ via write-to-local-then-copy pattern
- Incapsula IP blocking detection (HTTP 403, HTML response body, incap keywords) with actionable D-02 fallback instructions
- Idempotent re-runs via day-folder deletion at notebook start
- Calgary bounding box from geocoder.osm with hardcoded fallback coordinates
- Validation cell verifying file count, extensions, and sizes

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement MLS scraping notebook** - `064d112` (feat)

**Plan metadata:** pending

## Files Created/Modified
- `notebooks/ingestion/mls_scrape.py` - Complete MLS scraping Databricks notebook with 11 cells: markdown header, pip install, imports, configuration, bounding box, clean_day_folder, write_batch, scrape_mls_page, scrape_mls_all, main execution, and validation

## Decisions Made
- Used `requests.post` directly instead of `requests.Session` -- no cookies needed (legacy cookies from 2022 are expired per Pitfall 2), keeping scraping stateless
- Hardcoded Calgary bounding box fallback (north=51.21, south=50.84, east=-113.86, west=-114.32) if geocoder.osm fails
- Exponential backoff on retries: `min(5 * attempt, 30)` seconds between failures, capped at 30s
- File naming pattern `mls_maxprice_{top_price:08d}` encodes the max price in the batch for traceability and debugging
- Content-Type checking plus body inspection for Incapsula detection (not just HTTP status code)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- MLS scraping notebook is ready for deployment to Databricks workspace
- Cloud IP blocking (INGEST-03) must be validated by running the notebook on Databricks compute
- If blocked by Incapsula, fallback instructions are printed with exact databricks CLI upload command
- Ready for Plan 03 (Rentfaster scraping notebook) which follows the same patterns established here

## Self-Check: PASSED

- FOUND: notebooks/ingestion/mls_scrape.py
- FOUND: .planning/phases/02-listing-ingestion/02-02-SUMMARY.md
- FOUND: commit 064d112

---
*Phase: 02-listing-ingestion*
*Completed: 2026-04-14*
