# Architecture Research

**Domain:** Real estate listing aggregator on Databricks lakehouse
**Researched:** 2026-04-14
**Confidence:** MEDIUM (training data through May 2025; web verification unavailable)

## Standard Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     DATABRICKS WORKFLOW (Daily)                         │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────┐    │
│  │  Task 1:     │   │  Task 2:     │   │  Task 3:                 │    │
│  │  MLS Scrape  │   │  Rentfaster  │──▶│  SDP/DLT Pipeline        │    │
│  │  (notebook)  │──▶│  Scrape      │   │  (bronze→silver→gold)    │    │
│  └──────────────┘   │  (notebook)  │   └──────────────────────────┘    │
│                     └──────────────┘                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                     UNITY CATALOG (wheretolive)                         │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ BRONZE SCHEMA                                                   │    │
│  │  ┌──────────────┐  ┌──────────────┐                             │    │
│  │  │ mls_raw      │  │ rfaster_raw  │  (append-only, partitioned  │    │
│  │  │ (Delta)      │  │ (Delta)      │   by scrape_date)           │    │
│  │  └──────┬───────┘  └──────┬───────┘                             │    │
│  ├─────────┼─────────────────┼─────────────────────────────────────┤    │
│  │ SILVER SCHEMA             │                                     │    │
│  │         ▼                 ▼                                     │    │
│  │  ┌──────────────┐  ┌──────────────┐                             │    │
│  │  │ mls_cleaned  │  │ rfaster_     │  (parsed, typed, deduped,   │    │
│  │  │ (Delta/SDP)  │  │ cleaned      │   SCD or latest-snapshot)   │    │
│  │  └──────┬───────┘  │ (Delta/SDP)  │                             │    │
│  │         │          └──────┬───────┘                             │    │
│  ├─────────┼─────────────────┼─────────────────────────────────────┤    │
│  │ GOLD SCHEMA               │                                     │    │
│  │         ▼                 ▼                                     │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │    │
│  │  │ mls_wide     │  │ rfaster_wide │  │ candidates_* │          │    │
│  │  │ (Delta/SDP)  │  │ (Delta/SDP)  │  │ (per-user    │          │    │
│  │  │              │  │              │  │  views/tables)│          │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │    │
│  └─────────┼─────────────────┼─────────────────┼──────────────────┘    │
├─────────────┼─────────────────┼─────────────────┼──────────────────────┤
│             ▼                 ▼                 ▼                      │
│  PRESENTATION LAYER                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐       │
│  │  AI/BI       │  │  Genie       │  │  Databricks App        │       │
│  │  Dashboard   │  │  Space       │  │  (Gradio/Streamlit     │       │
│  │              │  │              │  │   + Lakebase)           │       │
│  └──────────────┘  └──────────────┘  └────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| **MLS Scrape Notebook** | Paginate through Realtor.ca API by price bracket, write raw JSON responses to bronze Delta table | Databricks notebook (Python), runs as Workflow task |
| **Rentfaster Scrape Notebook** | Paginate through Rentfaster API, write raw JSON responses to bronze Delta table | Databricks notebook (Python), runs as Workflow task |
| **SDP/DLT Pipeline** | Declarative transformations: bronze raw to silver cleaned to gold enriched/wide | SDP (Serverless Data Pipeline, formerly DLT) pipeline definition, runs as Workflow pipeline task |
| **Bronze Tables** | Immutable append-only raw data store, full scrape history | Delta tables in Unity Catalog, partitioned by `scrape_date` |
| **Silver Tables** | Parsed, typed, deduplicated, quality-checked listings | Delta tables managed by SDP with expectations (data quality rules) |
| **Gold Tables** | Business-ready wide tables and per-user candidate views | Delta tables or views managed by SDP; candidate filtering uses secrets |
| **AI/BI Dashboard** | Visual exploration of listings with filters, maps, charts | Databricks AI/BI dashboard connected to gold tables/views |
| **Genie Space** | Natural language Q&A over listings data | Genie space with metric views and curated sample questions |
| **Databricks App + Lakebase** | Interactive CRUD experience for listing exploration and preference management | Streamlit/Gradio app deployed as Databricks App, backed by Lakebase (serverless Postgres) |
| **Unity Catalog** | Centralized governance: catalog > schema > table namespace, permissions, lineage | Three schemas: `bronze`, `silver`, `gold` under one catalog |
| **Databricks Secrets** | Store per-user candidate filter criteria (price range, bedroom counts, etc.) | Secrets scope per user, read at gold-layer view/table generation time |

## Recommended Project Structure

The Databricks workspace does not use a traditional filesystem project layout. Instead, code is organized across notebooks and pipeline definitions. The local repo should mirror this.

```
wheretolive-databricks/
├── notebooks/
│   ├── ingestion/
│   │   ├── mls_scrape.py          # Scraping logic adapted for notebook execution
│   │   └── rfaster_scrape.py      # Rentfaster scraping adapted for notebook
│   ├── pipeline/
│   │   └── medallion_pipeline.py  # SDP/DLT pipeline definition (bronze→silver→gold)
│   └── setup/
│       └── init_catalog.py        # One-time: create catalog, schemas, seed secrets
├── src/                           # Shared Python modules (optional, for wheel packaging)
│   └── wheretolive/
│       ├── scrapers/
│       │   ├── mls_client.py      # MLS API interaction (extracted, testable)
│       │   └── rfaster_client.py  # Rentfaster API interaction (extracted, testable)
│       └── transforms/
│           ├── mls_transforms.py  # Parsing/cleaning logic for MLS
│           └── rfaster_transforms.py
├── app/
│   ├── app.py                     # Databricks App entry point (Streamlit or Gradio)
│   └── requirements.txt           # App-specific dependencies
├── tests/
│   ├── test_mls_transforms.py
│   └── test_rfaster_transforms.py
├── databricks.yml                 # Databricks Asset Bundles config
└── resources/
    ├── workflow.yml               # Workflow definition (tasks, dependencies)
    └── pipeline.yml               # SDP/DLT pipeline settings
```

### Structure Rationale

- **notebooks/ingestion/:** Scraping is imperative code (API calls, pagination, retries) that does not fit declarative SDP/DLT. Kept as standard notebooks run as Workflow tasks.
- **notebooks/pipeline/:** The SDP/DLT pipeline definition is a single file that declaratively defines all bronze-to-silver-to-gold transformations. SDP requires this to be a notebook or Python file referenced by the pipeline.
- **src/wheretolive/:** Extracted, testable modules that can be packaged as a wheel and installed on clusters. Keeps parsing logic out of notebooks.
- **app/:** Databricks Apps are deployed from a directory with an entry point. Lakebase connection config is managed by the platform.
- **databricks.yml + resources/:** Databricks Asset Bundles (DABs) for infrastructure-as-code deployment of workflows, pipelines, and apps.

## Architectural Patterns

### Pattern 1: Append-Only Bronze with Snapshot Semantics

**What:** Raw scraped data is appended to bronze tables with a `scrape_date` partition column. Every day's scrape is a complete snapshot. Data is never updated or deleted in bronze.

**When to use:** When the source is a full-refresh scrape (you get all current listings each day, not incremental changes).

**Trade-offs:** Uses more storage (daily full snapshots) but provides complete audit trail and simplifies reprocessing. Storage cost is negligible for this data volume (thousands of listings, not millions).

**Example:**
```python
# In ingestion notebook
from pyspark.sql import functions as F

raw_df = spark.createDataFrame(scraped_records)
raw_df = raw_df.withColumn("scrape_date", F.current_date())
raw_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

raw_df.write.format("delta") \
    .mode("append") \
    .partitionBy("scrape_date") \
    .saveAsTable("wheretolive.bronze.mls_raw")
```

### Pattern 2: SDP/DLT Declarative Medallion Pipeline

**What:** A single pipeline file defines streaming or batch tables for each layer. SDP manages dependencies, ordering, and data quality expectations automatically. Tables are defined with `@dlt.table` decorators and quality rules via `@dlt.expect`.

**When to use:** For all bronze-to-silver-to-gold transformations. SDP handles lineage tracking, error handling, and incremental processing.

**Trade-offs:** Declarative approach limits flexibility for complex imperative logic (e.g., API calls) but is ideal for SQL/DataFrame transformations. Cannot run arbitrary side effects (no HTTP calls inside SDP). This is why scraping stays outside as a separate Workflow task.

**Example:**
```python
import dlt
from pyspark.sql import functions as F

# Silver: cleaned and typed
@dlt.table(
    name="mls_cleaned",
    comment="Parsed, typed, deduplicated MLS listings"
)
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_location", "latitude IS NOT NULL AND longitude IS NOT NULL")
def mls_cleaned():
    return (
        dlt.read_stream("mls_raw")
        .withColumn("price", F.col("Property.PriceUnformattedValue").cast("double"))
        .withColumn("latitude", F.col("Property.Address.Latitude").cast("double"))
        # ... more parsing ...
        .dropDuplicates(["mls_number", "scrape_date"])
    )

# Gold: wide table joining enrichments
@dlt.table(
    name="mls_wide",
    comment="MLS listings with all enrichments joined"
)
def mls_wide():
    return dlt.read("mls_cleaned")  # batch read from silver
```

### Pattern 3: Secrets-Driven Candidate Filtering

**What:** Per-user filter criteria (price range, bedroom count, location preferences) are stored in Databricks secrets rather than hardcoded in SQL or code. Gold-layer candidate tables or views read these secrets at pipeline execution time to generate personalized results.

**When to use:** When filter criteria are sensitive (real budget preferences) and must not be in source control. Also enables adding/modifying users without code changes.

**Trade-offs:** Secrets are string-only and have no versioning UI. Slightly more complex than SQL WHERE clauses. But avoids the hardcoded `ian_candidates`, `parents_candidates`, `jill_candidates` pattern in the current system.

**Example:**
```python
import json
from pyspark.sql import functions as F

# Read user preferences from secrets
prefs = json.loads(dbutils.secrets.get(scope="wheretolive", key="ian_prefs"))

# Apply dynamic filtering
candidates = spark.table("wheretolive.gold.mls_wide").filter(
    (F.col("price").between(prefs["price_min"], prefs["price_max"])) &
    (F.col("bedrooms") >= prefs["min_bedrooms"]) &
    (F.col("bathrooms") >= prefs["min_bathrooms"])
)
```

### Pattern 4: Workflow as Orchestration Wrapper

**What:** A single Databricks Workflow defines the full daily pipeline as a DAG of tasks. Imperative tasks (scraping notebooks) run first, then a DLT/SDP pipeline task runs the declarative transformations. Task dependencies ensure correct ordering.

**When to use:** Always, for this project. The Workflow is the single entry point for the daily run.

**Trade-offs:** Workflow UI provides monitoring, retry, and alerting out of the box. The key constraint is that DLT/SDP pipeline tasks and notebook tasks are different task types in the workflow definition -- they cannot be mixed within a single DLT pipeline.

**DAG structure:**
```
Workflow: wheretolive_daily
├── Task: mls_scrape (notebook task, depends_on: none)
├── Task: rfaster_scrape (notebook task, depends_on: none)
│         [mls_scrape and rfaster_scrape run in parallel]
└── Task: medallion_pipeline (DLT/SDP pipeline task, depends_on: mls_scrape, rfaster_scrape)
```

## Data Flow

### Daily Pipeline Flow

```
[Realtor.ca API]          [Rentfaster API]
       │                         │
       ▼                         ▼
┌──────────────┐          ┌──────────────┐
│ mls_scrape   │          │ rfaster_     │     INGESTION
│ notebook     │          │ scrape       │     (Workflow notebook tasks)
│              │          │ notebook     │
└──────┬───────┘          └──────┬───────┘
       │ .write.append()         │ .write.append()
       ▼                         ▼
┌──────────────┐          ┌──────────────┐
│ bronze.      │          │ bronze.      │     BRONZE
│ mls_raw      │          │ rfaster_raw  │     (raw JSON as struct columns)
└──────┬───────┘          └──────┬───────┘
       │ SDP readStream          │ SDP readStream
       ▼                         ▼
┌──────────────┐          ┌──────────────┐
│ silver.      │          │ silver.      │     SILVER
│ mls_cleaned  │          │ rfaster_     │     (typed, parsed, quality-checked)
│              │          │ cleaned      │
└──────┬───────┘          └──────┬───────┘
       │                         │
       ▼                         ▼
┌──────────────┐          ┌──────────────┐
│ gold.        │          │ gold.        │     GOLD
│ mls_wide     │          │ rfaster_wide │     (enriched, joined)
└──────┬───────┘          └──────┬───────┘
       │                         │
       ├────────┬────────────────┤
       ▼        ▼                ▼
┌──────────┐┌──────────┐┌──────────────────┐
│ AI/BI    ││ Genie    ││ Databricks App   │  PRESENTATION
│ Dashboard││ Space    ││ + Lakebase       │
└──────────┘└──────────┘└──────────────────┘
```

### Presentation Layer Data Access

Each presentation layer reads from gold tables differently:

1. **AI/BI Dashboard:** Directly queries gold tables/views via SQL. The dashboard editor connects to Unity Catalog tables. No intermediate data store needed. Supports filters, parameters, map visualizations natively.

2. **Genie Space:** Configured with "metric views" that define measures and dimensions over gold tables. Genie uses these metric views plus sample questions and instructions to interpret natural language queries. Reads from the same gold tables but through a semantic layer.

3. **Databricks App + Lakebase:** The app (Streamlit/Gradio) runs as a Databricks App with its own compute. Lakebase provides a serverless Postgres-compatible database for the app's operational data (user sessions, saved searches, preference management). The app reads listing data from gold Delta tables via Spark or Databricks SQL connector, and uses Lakebase for its own application state. This is the key architectural distinction: Delta tables = analytical data, Lakebase = application/transactional data.

### Key Data Flows

1. **Scrape-to-bronze:** Imperative Python code calls external APIs, collects JSON responses, writes as structured Delta rows with `scrape_date` partition. This is a full-refresh pattern: each run appends today's complete snapshot.

2. **Bronze-to-silver (SDP):** SDP reads bronze as a streaming source (new partitions trigger processing). Applies schema enforcement, type casting, field extraction from nested JSON structs, deduplication, and data quality expectations. Failed rows are quarantined by SDP automatically.

3. **Silver-to-gold (SDP):** Joins silver tables with any enrichment tables (future: commute times, flood zones). Produces wide denormalized tables optimized for querying. Candidate filtering happens here or in views on top of gold.

4. **Gold-to-presentation:** Each presentation layer reads gold tables directly. No ETL between gold and presentation -- this is the lakehouse advantage over a traditional warehouse-to-BI-tool pattern.

## Scaling Considerations

This is a personal learning project with small data volumes (thousands of listings per day, not millions). Scaling is not a primary concern, but understanding Databricks scaling patterns is part of the learning goal.

| Scale | Architecture Adjustments |
|-------|--------------------------|
| Current (2 sources, ~5K listings/day) | Single-node scraping, small SDP pipeline, serverless SQL for dashboards. Total data < 1GB. No optimization needed. |
| 10 cities (50K listings/day) | Parameterize scraping notebooks per city. SDP handles the volume natively. Partition gold tables by city. |
| Historical analysis (years of data) | Bronze append-only pattern already supports this. Add Z-ordering on frequently filtered columns in gold. Consider liquid clustering. |

### Scaling Priorities

1. **First bottleneck (if it ever matters):** Scraping rate limits from source APIs. Mitigation: request throttling is already in the existing code; carry it forward.
2. **Second bottleneck:** Dashboard query latency on large gold tables. Mitigation: Z-order or liquid clustering on `scrape_date` and `price` columns.

## Anti-Patterns

### Anti-Pattern 1: Putting Scraping Logic Inside SDP/DLT

**What people do:** Try to make API calls (HTTP requests) inside `@dlt.table` functions.
**Why it's wrong:** SDP/DLT is a declarative data transformation framework. It expects to read from data sources (Delta tables, files, streams), not make external API calls. HTTP calls inside SDP will fail or behave unpredictably -- SDP may retry transformations, reorder execution, or run in a context where network access is restricted.
**Do this instead:** Keep scraping as separate notebook tasks in the Workflow. SDP only reads from the Delta tables those notebooks produce.

### Anti-Pattern 2: Truncate-and-Replace in Bronze

**What people do:** TRUNCATE the bronze table and INSERT the latest scrape, mimicking the current PostGIS pattern (`TRUNCATE mls; ... to_sql(... if_exists="append")`).
**Why it's wrong:** Destroys history. The current system only keeps the latest snapshot because PostGIS was storage-constrained. Delta storage is cheap. Append-only bronze preserves every scrape for historical analysis, trend detection, and reprocessing.
**Do this instead:** Always append to bronze with a `scrape_date` column. Handle "latest snapshot" semantics in silver/gold layers.

### Anti-Pattern 3: Hardcoded Candidate Filters in Source Control

**What people do:** Write SQL views like `CREATE VIEW ian_candidates AS ... WHERE price BETWEEN 300000 AND 650000` directly in committed code.
**Why it's wrong:** Leaks sensitive financial preferences into version control. Makes adding/changing users a code change rather than a configuration change.
**Do this instead:** Store filter criteria in Databricks secrets as JSON. Read at pipeline time. Generate candidate tables dynamically.

### Anti-Pattern 4: One Notebook per Transformation Step

**What people do:** Create separate notebooks for bronze-to-silver, silver-to-gold, and each enrichment join, then chain them as separate Workflow tasks.
**Why it's wrong:** Loses SDP/DLT's core value: automatic dependency resolution, lineage tracking, data quality management, and incremental processing. You end up reimplementing what SDP gives you for free.
**Do this instead:** Define the entire medallion pipeline in a single SDP pipeline definition file. Let SDP manage the execution order based on the declared table dependencies.

### Anti-Pattern 5: Using Lakebase for Analytical Queries

**What people do:** Replicate gold table data into Lakebase and query it for dashboards/reports.
**Why it's wrong:** Lakebase is a transactional (OLTP) database for application state, not analytical queries. Delta tables with serverless SQL warehouses are purpose-built for analytical workloads. Duplicating data adds complexity and staleness.
**Do this instead:** Use Lakebase only for Databricks App operational state (user sessions, saved searches, UI preferences). Read listing data from Delta gold tables via `spark.table()` or Databricks SQL connector.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Realtor.ca API | HTTP POST with cookies and headers, paginate by price bracket | Fragile: depends on undocumented API, cookies, and user-agent spoofing. Build retry/backoff logic. Monitor for API changes. |
| Rentfaster API | HTTP GET with pagination, JSON response | More stable public API than MLS. Still add retry logic. |
| Databricks Secrets | `dbutils.secrets.get(scope, key)` | Available in notebooks and SDP. Not available in Databricks Apps directly -- pass via environment variables or Lakebase config table. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Scraping notebooks <-> Bronze tables | Direct Delta write (`df.write.saveAsTable`) | Notebooks own the write path. SDP owns the read path. Clear producer/consumer boundary. |
| SDP Pipeline <-> Silver/Gold tables | Declarative SDP table definitions | SDP fully manages these tables. Do not write to them from outside the pipeline. |
| Gold tables <-> AI/BI Dashboard | SQL queries via serverless SQL warehouse | Dashboard reads only. No write path. |
| Gold tables <-> Genie Space | Metric views + SQL warehouse | Genie reads through metric view semantic layer. |
| Gold tables <-> Databricks App | `spark.table()` or Databricks SQL connector | App reads listing data from gold, writes app state to Lakebase. |
| Databricks App <-> Lakebase | Standard Postgres wire protocol (psycopg2/SQLAlchemy) | App owns Lakebase schema. No other component writes to Lakebase. |

## Build Order (Dependency-Driven)

The following build order reflects technical dependencies: each step requires the previous one to exist.

### Phase 1: Foundation (must come first)
- Unity Catalog setup (catalog, schemas: bronze/silver/gold)
- Secrets scope and initial user preferences
- **Rationale:** Everything depends on the catalog structure existing.

### Phase 2: Ingestion (depends on Phase 1)
- MLS scrape notebook writing to bronze
- Rentfaster scrape notebook writing to bronze
- **Rationale:** Bronze tables must exist and be populated before SDP can read them.

### Phase 3: SDP/DLT Pipeline (depends on Phase 2)
- Bronze-to-silver transformations with expectations
- Silver-to-gold wide tables
- Candidate filtering (secrets-driven)
- **Rationale:** Needs bronze data to exist. This is the core data engineering work.

### Phase 4: Workflow Orchestration (depends on Phases 2 + 3)
- Wire scrape tasks + SDP pipeline into a single Workflow
- Schedule daily trigger
- **Rationale:** Individual pieces must work before orchestrating them together.

### Phase 5: AI/BI Dashboard (depends on Phase 3)
- Build dashboard against gold tables
- **Rationale:** Simplest presentation layer. Gold tables must exist. Good first proof that the data pipeline works end-to-end.

### Phase 6: Genie Space (depends on Phase 3, benefits from Phase 5 learnings)
- Create metric views over gold tables
- Configure sample questions and instructions
- **Rationale:** Requires understanding the data well (gained from dashboard work). Metric views are a distinct concept from dashboard charts.

### Phase 7: Databricks App + Lakebase (depends on Phase 3)
- Provision Lakebase instance
- Build Streamlit/Gradio app reading from gold tables
- Add interactive features using Lakebase for state
- **Rationale:** Most complex presentation layer. Benefits from experience with the data gained in Phases 5-6.

## Sources

- Databricks documentation: Delta Live Tables / Serverless Data Pipelines (docs.databricks.com/en/delta-live-tables/)
- Databricks documentation: Workflows and Jobs (docs.databricks.com/en/workflows/)
- Databricks documentation: AI/BI Dashboards (docs.databricks.com/en/dashboards/)
- Databricks documentation: Genie Spaces (docs.databricks.com/en/genie/)
- Databricks documentation: Databricks Apps (docs.databricks.com/en/apps/)
- Databricks documentation: Lakebase (docs.databricks.com/en/lakebase/)
- Databricks documentation: Unity Catalog (docs.databricks.com/en/data-governance/unity-catalog/)
- Databricks documentation: Asset Bundles (docs.databricks.com/en/dev-tools/bundles/)
- Existing codebase: src/wheretolive/mls/, src/wheretolive/rfaster/, scripts/postgis/

**Confidence note:** All architectural patterns described are based on Databricks documentation and ecosystem knowledge through May 2025. Lakebase and Databricks Apps were in public preview as of that date. SDP (formerly DLT) naming and API surface may have evolved. Verify current naming and availability in the workspace before implementation.

---
*Architecture research for: WhereToLive Databricks Migration*
*Researched: 2026-04-14*
