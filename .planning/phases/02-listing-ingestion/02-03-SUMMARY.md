---
phase: 02-listing-ingestion
plan: 03
subsystem: infra
tags: [databricks, dab, volumes, scraping, validation]

requires:
  - phase: 02-01
    provides: ingest Volume resource + Rentfaster scraping notebook
  - phase: 02-02
    provides: MLS scraping notebook

provides:
  - Deployed and validated DAB bundle with ingest Volume
  - Confirmed both scraping sources blocked from Databricks cloud IPs
  - Runtime fixes for free-edition Databricks compatibility

affects: [03-medallion-pipeline, phase-2-followup]

tech-stack:
  added: []
  patterns:
    - "Schema discovery via SHOW SCHEMAS for DAB dev-mode prefix handling"
    - "Job submission via databricks jobs submit for automated notebook validation"

key-files:
  created: []
  modified:
    - notebooks/ingestion/mls_scrape.py
    - notebooks/ingestion/rfaster_scrape.py

key-decisions:
  - "Both MLS and Rentfaster blocked from Databricks cloud IPs — local fallback (D-02) needed for both"
  - "spark.conf.get replaced with hardcoded catalog + dynamic schema discovery"
  - "MLS price pagination needs jump-ahead logic for per-sqft vacant land listings"

patterns-established:
  - "Dynamic schema discovery: SHOW SCHEMAS IN {catalog} LIKE '*wheretolive*' handles dev-mode prefix"
  - "Notebook validation via CLI: databricks jobs submit + get-run-output for automated testing"

requirements-completed:
  - INGEST-01
  - INGEST-02
  - INGEST-03

duration: 25min
completed: 2026-04-14
---

# Plan 02-03: Deploy & Validate Summary

**DAB bundle deployed with ingest Volume; both scrapers blocked from cloud IPs — MLS intermittently, Rentfaster consistently (HTTP 403)**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-04-14T16:25:00Z
- **Completed:** 2026-04-14T16:53:00Z
- **Tasks:** 2 (1 auto deploy + 1 validation)
- **Files modified:** 2

## Accomplishments
- DAB bundle validates and deploys with ingest Volume resource
- Volume `workspace.dev_ian_e_preston_wheretolive.ingest` created and accessible
- Both notebooks deployed and executed on Databricks via CLI job submission
- INGEST-03 validated: MLS blocked by Incapsula (intermittent), Rentfaster blocked (consistent 403)
- Fixed 3 runtime bugs discovered during cloud execution

## Task Commits

1. **Task 1: Validate and deploy DAB bundle** — deployment-only, no local file changes
2. **Task 2: Run notebooks and validate** — automated via `databricks jobs submit`

**Runtime fixes:** `d075bac` (fix: notebook runtime issues found during cloud validation)

## Files Created/Modified
- `notebooks/ingestion/mls_scrape.py` — Fixed spark.conf.get, schema discovery, %pip syntax, pagination stall
- `notebooks/ingestion/rfaster_scrape.py` — Fixed spark.conf.get, schema discovery, added User-Agent + diagnostics

## Decisions Made
- **Both sources need local fallback:** MLS got through once before Incapsula flagged the IP; Rentfaster consistently returns 403. D-02 fallback approach confirmed necessary for both.
- **Schema discovery at runtime:** Replaced `spark.conf.get` (broken on free-edition) with hardcoded catalog + SQL `SHOW SCHEMAS` to find dev-prefixed schema name.
- **API parameter findings:** MLS API returns per-sqft vacant land listings at bottom of price range; pagination needs jump-ahead logic to skip past them. RecordsPerPage=100 works, 500 does not.

## Deviations from Plan

### Auto-fixed Issues

**1. spark.conf.get crash on free-edition Databricks**
- **Found during:** Task 2 (first notebook run)
- **Issue:** `spark.conf.get("spark.databricks.unityCatalog.catalog", "workspace")` throws `CONFIG_NOT_AVAILABLE` on Spark Connect instead of returning default
- **Fix:** Hardcode `CATALOG = "workspace"` + discover schema via `SHOW SCHEMAS`
- **Files modified:** both notebooks
- **Verification:** Subsequent runs got past configuration cell

**2. DAB dev-mode schema prefix not handled**
- **Found during:** Task 2 (file path mismatch)
- **Issue:** Notebooks used `SCHEMA = "wheretolive"` but deployed schema is `dev_ian_e_preston_wheretolive`
- **Fix:** Dynamic schema discovery via SQL
- **Files modified:** both notebooks

**3. MLS price pagination stalled on per-sqft listings**
- **Found during:** Task 2 (MLS only produced 1 file)
- **Issue:** Vacant land per-sqft prices ($0.07-$1.00) caused max_price to round to 0, breaking pagination
- **Fix:** Jump-ahead to $1,000 when price stalls below threshold
- **Files modified:** notebooks/ingestion/mls_scrape.py

---

**Total deviations:** 3 auto-fixed (all runtime bugs discovered during cloud validation)
**Impact on plan:** Fixes were necessary for notebooks to execute on Databricks. No scope creep.

## Issues Encountered
- MLS Incapsula blocking is intermittent — first request may succeed before IP is flagged
- Rentfaster blocks all requests from Databricks Azure IPs with HTTP 403
- `RecordsPerPage: 500` silently returns empty results from MLS API (reverted to 100)

## Next Phase Readiness
- Volume exists and is accessible in Unity Catalog
- Notebook code is structurally correct and handles blocking gracefully
- **Blocker for Phase 3:** Raw JSON data must exist in the Volume before SDP pipeline can process it. Need local fallback to populate Volume with scraped data.
- Suggested follow-up: local scraping script + `databricks fs cp` upload to Volume

---
*Phase: 02-listing-ingestion*
*Completed: 2026-04-14*
