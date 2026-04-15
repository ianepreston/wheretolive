---
phase: quick-260415-9bs
plan: 01
tags: [geocoder, osm, mls, scraping, incapsula, playwright, dependency-removal]

provides:
  - Removed geocoder dependency — hardcoded Calgary bounds used directly
  - Eliminated OSM Nominatim 403 errors from MLS scraping path
  - Browser-based MLS API calls bypass Incapsula TLS fingerprinting

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
  - "MLS API calls via page.evaluate(fetch(...)) — inherits browser TLS fingerprint to bypass Incapsula"
  - "requests dependency removed from local_ingest.py (still transitive via databricks-sdk)"

duration: ~10min
completed: 2026-04-15
---

# Quick Task 260415-9bs: Fix MLS Scraping — Remove Geocoder + Browser-Based API Calls

**Hardcoded Calgary bounds + Playwright fetch bypasses both OSM 403 and Incapsula TLS detection**

## Accomplishments

1. **Removed geocoder dependency:**
   - local_ingest.py: replaced `get_calgary_bounds()` with hardcoded `CALGARY_BOUNDS` constant
   - mls_scrape.py notebook: removed `%pip install geocoder` cell, import, and try/except block
   - pyproject.toml: removed `geocoder>=1.38.1`, `requests>=2.32.0` from direct dependencies
   - uv.lock: regenerated — removed geocoder + 5 transitive deps

2. **Browser-based MLS API calls:**
   - Replaced `get_incapsula_cookies()` + `requests.post()` approach with `page.evaluate(fetch(...))`
   - API requests now inherit the browser's TLS fingerprint, cookies, and headers
   - Bypasses Incapsula's TLS fingerprint detection that was blocking `requests` library calls
   - Browser session lifecycle managed in `scrape_mls()` with proper cleanup in `finally` block

3. **Cleaned up unused imports:** removed `tempfile`, `requests`, `import geocoder`

## Root Causes

- **OSM 403:** Nominatim consistently returns 403. Calgary bounds are static — no API needed.
- **MLS Incapsula 403:** Incapsula detects `requests` library via TLS fingerprinting even with valid browser cookies. Using the browser's own `fetch()` API makes requests indistinguishable from real browser traffic.

## Commits

- `bdab8f6`: fix(quick-260415-9bs): remove OSM geocoder dependency — use hardcoded Calgary bounds
- `c7cbda0`: fix(quick-260415-9bs): use browser-based fetch for MLS API calls

---
*Phase: quick-260415-9bs*
*Completed: 2026-04-15*
