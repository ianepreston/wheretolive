# Requirements: WhereToLive — Databricks Migration

**Defined:** 2026-04-14
**Core Value:** Daily real estate listings flow reliably from source to presentation, and each presentation layer is polished enough to genuinely teach how the Databricks feature works.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Infrastructure

- [ ] **INFRA-01**: Unity Catalog schema setup in `workspace` catalog via Databricks Asset Bundles (DAB)
- [ ] **INFRA-02**: DAB project structure for all Databricks resource definitions (notebooks, pipelines, workflows, apps)

### Ingestion

- [ ] **INGEST-01**: MLS scraping notebook dumps raw JSON to Unity Catalog Volume in date-stamped folders
- [ ] **INGEST-02**: Rentfaster scraping notebook dumps raw JSON to Unity Catalog Volume in date-stamped folders
- [ ] **INGEST-03**: Validate MLS scraping works from Databricks cloud IPs; design and implement fallback if blocked by Incapsula

### Pipeline

- [ ] **PIPE-01**: Spark Declarative Pipeline (SDP) loads raw JSON from Volumes into bronze Delta tables
- [ ] **PIPE-02**: SDP transforms bronze to silver tables (typed, parsed, deduped, with data quality expectations)
- [ ] **PIPE-03**: SDP transforms silver to gold tables (wide tables ready for all presentation layers)
- [ ] **PIPE-04**: Column COMMENTs on all gold table columns and table descriptions for Genie readiness

### Orchestration

- [ ] **ORCH-01**: Databricks Workflow DAG with parallel scrape notebook tasks followed by SDP pipeline task
- [ ] **ORCH-02**: Daily cron schedule trigger for the workflow

### Dashboard

- [ ] **DASH-01**: AI/BI Dashboard with summary metrics (total listings, median price, new listings today)
- [ ] **DASH-02**: Dashboard filter controls (price range, bedrooms, location/area)

### Genie

- [ ] **GENIE-01**: Genie Space connected to gold tables with comprehensive table and column descriptions
- [ ] **GENIE-02**: 3-4 metric views (e.g., median price by property type, new listings by day, listings by neighbourhood)

### App

- [ ] **APP-01**: Dash-based Databricks App with interactive listing browser (search, sort, detail view)
- [ ] **APP-02**: Lakebase backend for application operational state
- [ ] **APP-03**: Interactive map explorer with listing location clustering

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Enrichment

- **ENRICH-01**: Foursquare amenity data (grocery stores) integrated into gold tables
- **ENRICH-02**: Azure Maps isochrone data (travel time polygons) integrated into gold tables

### Dashboard Enhancements

- **DASH-03**: Map visualization with listing locations in AI/BI Dashboard
- **DASH-04**: Cross-filtering between dashboard widgets
- **DASH-05**: Date-range historical views

### Genie Enhancements

- **GENIE-03**: 10-15 curated sample questions
- **GENIE-04**: Custom real estate domain instructions
- **GENIE-05**: Feedback review and iteration cycle for Genie quality

### App Enhancements

- **APP-04**: Per-user candidate filtering via Databricks Secrets
- **APP-05**: Saved searches and favorites

### Orchestration Enhancements

- **ORCH-03**: Workflow failure alerting via email or webhook

## Out of Scope

| Feature | Reason |
|---------|--------|
| PostGIS spatial operations | Replaced by Databricks; pre-compute any spatial fields in gold layer |
| Dropbox upload / HTML reports | Replaced by Databricks presentation layers |
| Local Docker infrastructure | Fully replaced by Databricks |
| Python package structure (Click CLI, Poetry, nox) | Not applicable in Databricks |
| Streaming / real-time ingestion | Daily batch is sufficient for real estate listings |
| Custom web framework (Flask, FastAPI) | Databricks Apps with Dash is the target |
| Mobile app | Personal learning project |
| Separate catalog creation | Free edition uses `workspace` catalog only |
| Multi-schema medallion (bronze/silver/gold schemas) | Single schema sufficient at this scale |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| INFRA-01 | TBD | Pending |
| INFRA-02 | TBD | Pending |
| INGEST-01 | TBD | Pending |
| INGEST-02 | TBD | Pending |
| INGEST-03 | TBD | Pending |
| PIPE-01 | TBD | Pending |
| PIPE-02 | TBD | Pending |
| PIPE-03 | TBD | Pending |
| PIPE-04 | TBD | Pending |
| ORCH-01 | TBD | Pending |
| ORCH-02 | TBD | Pending |
| DASH-01 | TBD | Pending |
| DASH-02 | TBD | Pending |
| GENIE-01 | TBD | Pending |
| GENIE-02 | TBD | Pending |
| APP-01 | TBD | Pending |
| APP-02 | TBD | Pending |
| APP-03 | TBD | Pending |

**Coverage:**
- v1 requirements: 18 total
- Mapped to phases: 0
- Unmapped: 18 (will be mapped during roadmap creation)

---
*Requirements defined: 2026-04-14*
*Last updated: 2026-04-14 after initial definition*
