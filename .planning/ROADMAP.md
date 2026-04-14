# Roadmap: WhereToLive — Databricks Migration

## Overview

Migrate WhereToLive from local Docker/PostGIS to a Databricks lakehouse. Start by standing up the DAB project structure and Unity Catalog schema, then build ingestion notebooks that dump raw JSON to Volumes, wire up SDP pipelines for bronze-through-gold transformations, orchestrate everything with a daily Workflow, and finally surface listings through three progressively complex presentation layers: AI/BI Dashboard, Genie Space, and a Databricks App with Lakebase.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Infrastructure & DAB Setup** - DAB project structure and Unity Catalog schema in workspace catalog
- [ ] **Phase 2: Listing Ingestion** - Scraping notebooks dump raw JSON to Unity Catalog Volumes
- [ ] **Phase 3: SDP Bronze Loading** - Declarative pipeline reads raw JSON from Volumes into bronze Delta tables
- [ ] **Phase 4: SDP Silver & Gold Transformations** - Pipeline transforms bronze through silver to gold with metadata
- [ ] **Phase 5: Workflow Orchestration** - DAG wires scraping tasks and SDP pipeline with daily schedule
- [ ] **Phase 6: AI/BI Dashboard** - Dashboard with summary metrics and filter controls on gold tables
- [ ] **Phase 7: Genie Space** - Natural language query interface with metric views on gold tables
- [ ] **Phase 8: Databricks App & Lakebase** - Interactive listing browser with map explorer and operational backend

## Phase Details

### Phase 1: Infrastructure & DAB Setup
**Goal**: All Databricks resources are defined in a DAB project and a working Unity Catalog schema exists in the workspace catalog
**Depends on**: Nothing (first phase)
**Requirements**: INFRA-01, INFRA-02
**Success Criteria** (what must be TRUE):
  1. Running `databricks bundle deploy` from the repo successfully creates/updates all defined resources in the Databricks workspace
  2. A Unity Catalog schema exists under the `workspace` catalog and is accessible from notebooks
  3. The DAB project defines placeholder entries for notebooks, pipelines, and workflows that later phases will populate
**Plans**: 2 plans
Plans:
- [x] 01-01-PLAN.md — Archive legacy code and create DAB project structure with databricks.yml, resource YAMLs, and placeholder notebooks
- [x] 01-02-PLAN.md — Validate, deploy to Databricks workspace, and verify schema and resources exist

### Phase 2: Listing Ingestion
**Goal**: Both MLS and Rentfaster scraping notebooks reliably produce raw JSON files in Unity Catalog Volumes
**Depends on**: Phase 1
**Requirements**: INGEST-01, INGEST-02, INGEST-03
**Success Criteria** (what must be TRUE):
  1. MLS scraping notebook runs from Databricks and writes JSON files to a Volume path organized by date (or a documented fallback is in place if cloud IPs are blocked)
  2. Rentfaster scraping notebook runs from Databricks and writes JSON files to a Volume path organized by date
  3. Running either notebook a second time on the same day does not corrupt or duplicate the raw data
  4. Raw JSON files are browsable via Unity Catalog Volume UI in the workspace
**Plans**: 3 plans
Plans:
- [x] 02-01-PLAN.md — Add ingest Volume to DAB schema and implement Rentfaster scraping notebook
- [x] 02-02-PLAN.md — Implement MLS scraping notebook with Incapsula detection
- [x] 02-03-PLAN.md — Deploy bundle, run notebooks on Databricks, and validate scraping results (INGEST-03)

### Phase 3: SDP Bronze Loading
**Goal**: A Spark Declarative Pipeline reads raw JSON from Volumes and materializes bronze Delta tables
**Depends on**: Phase 2
**Requirements**: PIPE-01
**Success Criteria** (what must be TRUE):
  1. An SDP pipeline definition exists that reads JSON files from the Volume paths and creates bronze Delta tables
  2. Bronze tables are visible in Unity Catalog under the project schema
  3. Running the pipeline after new JSON arrives correctly appends new data to bronze tables
**Plans**: TBD

### Phase 4: SDP Silver & Gold Transformations
**Goal**: The SDP pipeline transforms bronze data through silver (cleaned, typed, deduped) to gold (wide tables ready for presentation) with full metadata
**Depends on**: Phase 3
**Requirements**: PIPE-02, PIPE-03, PIPE-04
**Success Criteria** (what must be TRUE):
  1. Silver tables contain typed, parsed, and deduplicated listings with SDP data quality expectations rejecting malformed rows
  2. Gold tables merge MLS and Rentfaster listings into wide tables usable by all three presentation layers
  3. Every gold table has a table-level COMMENT and every column has a column-level COMMENT describing its meaning
  4. Querying a gold table from a notebook returns current-day listings with correct types and no duplicates
**Plans**: TBD

### Phase 5: Workflow Orchestration
**Goal**: A single Databricks Workflow runs the full pipeline end-to-end on a daily schedule
**Depends on**: Phase 2, Phase 4
**Requirements**: ORCH-01, ORCH-02
**Success Criteria** (what must be TRUE):
  1. A Databricks Workflow DAG runs MLS and Rentfaster scraping tasks in parallel, followed by the SDP pipeline task
  2. The workflow is triggered by a daily cron schedule
  3. Manually triggering the workflow produces fresh data flowing from scraping through to gold tables
**Plans**: TBD

### Phase 6: AI/BI Dashboard
**Goal**: Users can browse listing data visually through an AI/BI Dashboard with key metrics and filtering
**Depends on**: Phase 4
**Requirements**: DASH-01, DASH-02
**Success Criteria** (what must be TRUE):
  1. An AI/BI Dashboard displays summary metrics including total listings, median price, and new listings today
  2. Dashboard filter controls allow narrowing by price range, bedrooms, and location/area
  3. Changing filters updates all dashboard widgets to reflect the filtered subset
**Plans**: TBD
**UI hint**: yes

### Phase 7: Genie Space
**Goal**: Users can ask natural language questions about listings and get accurate answers from a Genie Space backed by well-described gold tables
**Depends on**: Phase 4
**Requirements**: GENIE-01, GENIE-02
**Success Criteria** (what must be TRUE):
  1. A Genie Space is connected to the gold tables and leverages the table/column COMMENTs from Phase 4
  2. At least 3 metric views are configured (e.g., median price by property type, new listings by day, listings by neighbourhood)
  3. Asking a natural language question like "what is the median price of 2-bedroom condos" returns a reasonable answer
**Plans**: TBD
**UI hint**: yes

### Phase 8: Databricks App & Lakebase
**Goal**: Users can interactively browse, search, and explore listings on a map through a Dash-based Databricks App with Lakebase for operational state
**Depends on**: Phase 4
**Requirements**: APP-01, APP-02, APP-03
**Success Criteria** (what must be TRUE):
  1. A Dash-based Databricks App provides an interactive listing browser with search, sort, and detail view
  2. Lakebase stores application operational state (separate from analytical gold tables)
  3. An interactive map view displays listing locations with clustering for dense areas
  4. The app reads listing data from gold tables via Unity Catalog (not from Lakebase)
**Plans**: TBD
**UI hint**: yes

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8

| Phase | Plans Complete | Status | Completed |
|-------|---------------|--------|-----------|
| 1. Infrastructure & DAB Setup | 0/2 | Planned | - |
| 2. Listing Ingestion | 0/3 | Planned | - |
| 3. SDP Bronze Loading | 0/TBD | Not started | - |
| 4. SDP Silver & Gold Transformations | 0/TBD | Not started | - |
| 5. Workflow Orchestration | 0/TBD | Not started | - |
| 6. AI/BI Dashboard | 0/TBD | Not started | - |
| 7. Genie Space | 0/TBD | Not started | - |
| 8. Databricks App & Lakebase | 0/TBD | Not started | - |
