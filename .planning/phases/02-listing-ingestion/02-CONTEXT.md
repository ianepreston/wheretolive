# Phase 2: Listing Ingestion - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Both MLS and Rentfaster scraping notebooks reliably produce raw JSON files in Unity Catalog Volumes. This phase adapts the legacy scraping logic into Databricks notebooks, writes gzip-compressed JSON to a single `ingest` Volume organized by source and date, and validates whether MLS scraping works from cloud IPs.

</domain>

<decisions>
## Implementation Decisions

### IP blocking fallback
- **D-01:** Attempt MLS scraping from Databricks first — don't assume blocking before validating.
- **D-02:** If Databricks cloud IPs are blocked by Incapsula, fall back to running the MLS scraper locally and uploading JSON to the Volume (manual or scripted).
- **D-03:** Rentfaster has no known IP blocking concerns — scrape directly from Databricks.

### Raw data format
- **D-04:** Write gzip-compressed JSON (.json.gz) files to Volumes. No ZIP/LZMA — gzip is natively supported by SDP Auto Loader.
- **D-05:** One .json.gz file per batch (per price bracket for MLS, per page for Rentfaster). Preserves scraping granularity.

### Idempotency strategy
- **D-06:** Delete the entire day folder at the start of each notebook run, then write fresh. Guarantees clean state on re-runs.

### Volume path layout
- **D-07:** Single Volume named `ingest` — follows user's established convention for landing data.
- **D-08:** Path structure: `/Volumes/workspace/wheretolive/ingest/{source}/{YYYY-MM-DD}/*.json.gz` where `{source}` is `mls` or `rfaster`.
- **D-09:** Volume defined in DAB resources (extends the schema.yml from Phase 1).

### Claude's Discretion
- Exact notebook implementation details (imports, error handling, retry logic)
- How to adapt the legacy cookie/header strategy for MLS requests
- Batch file naming convention within day folders (e.g., batch numbering, price range encoding)
- Whether to add a simple validation step at the end of each notebook (row count, file size check)
- How to structure the local-scrape-and-upload fallback script if needed

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project context
- `.planning/PROJECT.md` — Project vision, constraints (workspace catalog, single schema, Databricks Secrets deferred to Phase 8)
- `.planning/REQUIREMENTS.md` — INGEST-01 (MLS JSON to Volume), INGEST-02 (Rentfaster JSON to Volume), INGEST-03 (validate cloud IP blocking + fallback)
- `.planning/ROADMAP.md` §Phase 2 — Success criteria for this phase

### Phase 1 decisions
- `.planning/phases/01-infrastructure-dab-setup/01-CONTEXT.md` — DAB project layout (D-03, D-04), table naming conventions (D-01), single schema (D-02)

### Legacy scraping code (reference for adaptation)
- `legacy/src/wheretolive/mls/scrape.py` — MLS API endpoint, payload structure, Incapsula cookie, price-bracket pagination
- `legacy/src/wheretolive/rfaster/scrape.py` — Rentfaster API endpoint, page-based pagination
- `legacy/src/wheretolive/mls/common.py` — Path resolution patterns (adapt to Volume paths)
- `legacy/src/wheretolive/rfaster/common.py` — Path resolution patterns (adapt to Volume paths)

### DAB resources (extend in this phase)
- `resources/schema.yml` — Add Volume definition for `ingest`
- `notebooks/ingestion/mls_scrape.py` — Placeholder to replace with real implementation
- `notebooks/ingestion/rfaster_scrape.py` — Placeholder to replace with real implementation

### Research
- `.planning/research/PITFALLS.md` — Pitfall on cloud IP blocking, compute cost awareness

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `legacy/src/wheretolive/mls/scrape.py`: MLS API endpoint (`api2.realtor.ca/Listing.svc/PropertySearch_Post`), request payload structure, price-bracket pagination loop, and Incapsula cookie construction. Core logic to adapt into notebook form.
- `legacy/src/wheretolive/rfaster/scrape.py`: Rentfaster API endpoint (`rentfaster.ca/api/search.json`), page-based pagination. Simpler adapter — straightforward port to notebook.

### Established Patterns
- Both scrapers use the same flow: paginate through results, serialize each batch to compressed JSON, write to date-stamped directory. This pattern maps directly to the Volume path layout decided here.
- Legacy uses `zipfile.ZIP_LZMA` compression — must change to `gzip` for Auto Loader compatibility.
- Legacy uses `requests` (MLS) and `urllib` (Rentfaster) — both available in Databricks runtime.

### Integration Points
- `resources/schema.yml` needs a Volume resource definition for `ingest`
- Placeholder notebooks at `notebooks/ingestion/` are the files to replace
- Phase 3 SDP pipeline will read from `/Volumes/workspace/wheretolive/ingest/{source}/` using Auto Loader

</code_context>

<specifics>
## Specific Ideas

- Volume named `ingest` per user's convention from other Databricks projects
- User prefers validate-first approach over defensive over-engineering — try MLS from cloud IPs before building fallback infrastructure

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 02-listing-ingestion*
*Context gathered: 2026-04-14*
