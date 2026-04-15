---
phase: quick-260415-9bs
plan: 01
tags: [geocoder, osm, mls, scraping, dependency-removal]

provides:
  - Removed geocoder dependency — hardcoded Calgary bounds used directly
  - Eliminated OSM Nominatim 403 errors from MLS scraping path

affects: [02-listing-ingestion, local-ingest-script, mls-notebook]

key-files:
  modified:
    - scripts/local_ingest.py
    - notebooks/ingestion/mls_scrape.py
    - pyproject.toml
    - uv.lock

key-decisions:
  - "Calgary bounding box is static geographic data — no API call needed"
  - "geocoder dependency fully removed (pyproject.toml, lockfile, both scripts)"

duration: ~3min
completed: 2026-04-15
---

# Quick Task 260415-9bs: Remove OSM Geocoder Dependency

**Hardcoded Calgary bounding box replaces flaky OSM Nominatim API call**

## Accomplishments

1. **local_ingest.py:** Replaced `get_calgary_bounds()` — removed try/except geocoder call, function now returns hardcoded bounds directly. Removed `import geocoder`.
2. **mls_scrape.py notebook:** Removed `%pip install geocoder` cell, `import geocoder`, and geocoder try/except block. Replaced with inline `BOUNDS = {...}` constant.
3. **pyproject.toml:** Removed `geocoder>=1.38.1` from dependencies.
4. **uv.lock:** Regenerated — removed geocoder, click, future, ratelim, six (5 transitive deps eliminated).

## Root Cause

OSM Nominatim consistently returns HTTP 403 Forbidden. The geocoder call was always unnecessary — Calgary's bounding box (north=51.21, south=50.84, east=-113.86, west=-114.32) is static geographic data.

## Commits

- `bdab8f6`: fix(quick-260415-9bs): remove OSM geocoder dependency — use hardcoded Calgary bounds

---
*Phase: quick-260415-9bs*
*Completed: 2026-04-15*
