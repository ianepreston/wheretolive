---
status: testing
phase: 02-listing-ingestion
source: [02-01-SUMMARY.md, 02-02-SUMMARY.md, 02-03-SUMMARY.md]
started: 2026-04-14T17:05:00Z
updated: 2026-04-14T17:05:00Z
---

## Current Test

number: 2
name: Local ingest script scrapes Rentfaster and uploads to Volume
expected: |
  Run `uv sync && uv run python scripts/local_ingest.py`.
  Rentfaster listings scraped locally, .json.gz files uploaded to Volume.
  Files visible via `databricks fs ls dbfs:/Volumes/workspace/dev_ian_e_preston_wheretolive/ingest/rfaster/{date}/`.
awaiting: user response

## Tests

### 1. DAB bundle validates and deploys with ingest Volume
expected: `databricks bundle validate` and `deploy` succeed. Volume `ingest` is listed under the wheretolive schema in Unity Catalog.
result: pass

### 2. Local ingest script scrapes Rentfaster and uploads to Volume
expected: Running `python scripts/local_ingest.py` scrapes Rentfaster listings locally, uploads .json.gz files to `/Volumes/workspace/{schema}/ingest/rfaster/{date}/`, and prints batch counts. Files are visible in Volume via Catalog UI or `databricks fs ls`.
result: [pending]

### 3. Local ingest script scrapes MLS and uploads to Volume
expected: Same script scrapes MLS via price-bracket pagination, uploads .json.gz to `/Volumes/workspace/{schema}/ingest/mls/{date}/`. If Incapsula blocks locally too, script reports the failure clearly.
result: [pending]

### 4. Local ingest script triggers pipeline after upload
expected: After scraping, the script runs `databricks bundle run medallion_pipeline`. The command executes (pipeline itself may fail since Phase 3 hasn't built it yet, but the trigger mechanism works).
result: [pending]

### 5. Notebook runtime fixes work on Databricks
expected: Both notebooks get past the configuration cell (no `CONFIG_NOT_AVAILABLE` error). Schema is discovered dynamically. Notebooks fail on scraping (403/Incapsula) but not on configuration.
result: [pending]

## Summary

total: 5
passed: 1
issues: 0
pending: 4
skipped: 0

## Gaps

[none yet]
