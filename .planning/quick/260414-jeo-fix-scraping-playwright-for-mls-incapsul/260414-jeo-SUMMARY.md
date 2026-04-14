---
phase: quick
plan: 260414-jeo
tags: [scraping, playwright, incapsula, rentfaster, rate-limiting]

provides:
  - Playwright-based Incapsula cookie extraction for MLS scraping
  - Randomized 2-3s delays for Rentfaster rate limiting
  - Geocoder None-bounds fallback fix
  - Volume discovery fix (schemas.list instead of volumes.list)

affects: [02-listing-ingestion, local-ingest-script]

key-files:
  modified:
    - scripts/local_ingest.py
    - pyproject.toml

key-decisions:
  - "Playwright headless Chromium extracts cookies from realtor.ca, passed to requests.post()"
  - "Graceful degradation: if Playwright fails, scraping attempts without cookies"
  - "Both MLS and Rentfaster use randomized 2-3s delays between requests"

duration: ~5min
completed: 2026-04-14
---

# Quick Task 260414-jeo: Fix Scraping Summary

**Playwright cookie extraction for MLS Incapsula bypass + randomized delays for Rentfaster**

## Accomplishments

1. **MLS Incapsula bypass:** Added `get_incapsula_cookies()` function that launches headless Chromium via Playwright, navigates to realtor.ca, waits for Incapsula challenge to resolve, and extracts session cookies. Cookies are passed to `scrape_mls_page()` via `requests.post(cookies=...)`.

2. **Rentfaster rate limiting:** Replaced fixed `time.sleep(1)` with `time.sleep(2 + random.random())` for randomized 2-3s delays between page requests.

3. **MLS inter-batch delay:** Also randomized to 2-3s (was fixed 1s).

4. **Dependency:** Added `playwright>=1.40.0` to pyproject.toml.

## Also Fixed (from earlier in session)

- **Volume discovery:** Changed from `W.volumes.list(schema_name=None)` (crashes) to `W.schemas.list()` for proper schema discovery
- **Geocoder fallback:** Added None-bounds check so hardcoded Calgary bounds are used when OSM returns 403

## Test Results

- Playwright successfully obtained 32 cookies from browser session
- IP was rate-limited by test time (multiple runs in one session), so full end-to-end scraping deferred to next fresh session
- Rentfaster: confirmed 240 listings (5 batches) on prior run with randomized delays
- MLS: confirmed 200 listings (2 batches) on prior run with proper Calgary bounds

## Commits

- `89b8665` feat: add Playwright cookie extraction for MLS Incapsula bypass
- `5c0c7b7` fix: add randomized delays for Rentfaster rate limiting
- `781f7a5` chore: add uv.lock for dependency resolution
