# Technology Stack

**Project:** WhereToLive — Databricks Migration
**Researched:** 2026-04-14
**NOTE:** WebSearch/WebFetch/Brave/Exa were all unavailable during research. Findings are based on training data (cutoff early 2025) plus codebase analysis. Lakebase and some newer features are flagged LOW confidence accordingly. Verify version numbers and GA status against current Databricks docs before committing to implementation.

## Recommended Stack

### Platform Core

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Databricks Workspace | Latest (Azure) | Compute, storage, orchestration | Already provisioned; single platform for entire pipeline | HIGH |
| Unity Catalog | Current | Data governance, catalog, schema management | Standard for new Databricks deployments; required for AI/BI and Genie features | HIGH |
| Delta Lake | 3.x (bundled with DBR) | Table format for all layers | Default table format in Databricks; ACID transactions, time travel, schema evolution | HIGH |
| Databricks Runtime | 15.x LTS or later | Notebook/job execution environment | LTS for stability; includes PySpark, Delta, pandas, requests pre-installed | HIGH |

### Ingestion (Scraping)

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Python notebooks | N/A | Scraping logic for MLS and Rentfaster | Direct port of existing requests-based scraping; notebooks are the natural unit of work in Databricks Jobs | HIGH |
| requests | 2.31+ (pre-installed on DBR) | HTTP calls to MLS/Rentfaster APIs | Already used in existing code; pre-installed on Databricks Runtime | HIGH |
| geocoder | 1.38+ (pip install in notebook) | Geocoding for MLS bounding box | Used in existing scraping code for Calgary bounds; install via %pip install geocoder at notebook top | HIGH |
| Databricks Secrets | N/A | Store sensitive cookies, API keys, candidate preferences | Replaces .env files and hardcoded values; scoped per-user for candidate preferences | HIGH |
| Volumes (Unity Catalog) | N/A | Temporary raw file staging if needed | Only if raw JSON needs to be persisted before Delta conversion; prefer writing directly to Delta tables | MEDIUM |

### Transformation (Medallion Pipeline)

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Lakeflow Declarative Pipelines (formerly DLT) | Current | Declarative bronze-to-silver-to-gold transformations | Databricks renamed DLT to "Lakeflow Declarative Pipelines" (also called Serverless Data Pipelines / SDP); declarative approach handles schema management, data quality expectations, and lineage automatically | MEDIUM |
| Python DLT API | dlt module | Define streaming/materialized tables and views | Use @dlt.table and @dlt.view decorators; Python is the right choice given existing codebase is all Python | HIGH |
| DLT Expectations | Built-in | Data quality enforcement | Declare @dlt.expect constraints on silver tables (e.g., price > 0, valid lat/long); failed rows quarantined automatically | HIGH |
| PySpark | Bundled with DBR | DataFrame transformations within DLT | Used for silver-layer cleaning logic (parsing bedrooms, sq_feet, dates); replaces pandas-based cleaning in existing code | HIGH |
| Spark SQL | Bundled with DBR | Gold-layer aggregations and views | SQL is cleaner for aggregation queries (avg price by neighbourhood, listing counts by type) | HIGH |

### Orchestration

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| Databricks Workflows | Current | End-to-end pipeline orchestration | Single DAG: scraping task(s) then DLT pipeline task then downstream refresh tasks | HIGH |
| Task types: Notebook Task | N/A | Run scraping notebooks | For the Python scraping step that calls external APIs | HIGH |
| Task types: Pipeline Task | N/A | Run DLT/SDP pipeline | Triggers the declarative pipeline for bronze to silver to gold | HIGH |
| Schedule trigger | Cron-based | Daily execution | Replace existing shell cron with Databricks native scheduling; set to run daily during off-peak hours | HIGH |

### Presentation Layer

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| AI/BI Dashboards | Current (GA) | Visual dashboards with filters, maps | Databricks-native dashboarding; supports map visualizations, filters, and auto-generated SQL; reads directly from Unity Catalog gold tables | HIGH |
| Genie Spaces | Current | Natural language query interface | Allows asking questions of gold tables in plain English; requires curated metric definitions and sample questions for good results | HIGH |
| Databricks Apps | Current (GA as of late 2024) | Custom interactive application | For richer UX than dashboards allow; supports Dash, Gradio, Streamlit; deployed and hosted within Databricks | MEDIUM |
| Lakebase | Preview/GA (verify) | Transactional database for App backend | PostgreSQL-wire-compatible database in Databricks; designed as the backend for Databricks Apps needing low-latency reads/writes; replaces need for external Postgres | LOW |

### Databricks App Framework Choice

| Framework | Version | Purpose | Why | Confidence |
|-----------|---------|---------|-----|------------|
| Dash (Plotly) | 2.x | Databricks App UI framework | More flexible than Streamlit for map-centric real estate UIs; better control over layout, callbacks, and interactive map components (via plotly.express scatter_mapbox); Databricks Apps officially supports Dash | MEDIUM |
| **Alternative: Streamlit** | 1.x | Simpler prototyping | Faster to build initial prototype; less flexible for complex layouts; also officially supported by Databricks Apps | MEDIUM |
| **Alternative: Gradio** | 4.x | ML-focused interfaces | Better for AI/ML demos; overkill for data browsing; skip unless adding LLM-based listing analysis later | MEDIUM |

**Recommendation:** Start with **Dash** because real estate listing UIs benefit from the map-first layout control Dash provides. Streamlit is acceptable if rapid prototyping speed is prioritized over polish.

### Supporting Libraries (for scraping notebooks)

| Library | Version | Purpose | When to Use | Confidence |
|---------|---------|---------|-------------|------------|
| requests | 2.31+ | HTTP client | MLS scraping (POST requests with cookies/headers) | HIGH |
| geocoder | 1.38+ | Geo bounding box | MLS Calgary bounds lookup | HIGH |
| time (stdlib) | N/A | Rate limiting | Sleep between API calls to avoid hammering sources | HIGH |
| json (stdlib) | N/A | Parse API responses | MLS and Rentfaster return JSON | HIGH |
| pyspark.sql.functions | Bundled | DataFrame transforms | Silver-layer data cleaning in DLT pipelines | HIGH |

### Libraries to NOT Use

| Library | Why Not | Replacement |
|---------|---------|-------------|
| pandas | Not needed; PySpark DataFrames are native in Databricks; pandas causes memory issues on driver for large datasets | pyspark.sql for all transformations |
| fastparquet | Parquet files replaced by Delta tables; no file-based intermediate storage | Delta Lake (default format) |
| sqlalchemy | No external database connections needed; Unity Catalog handles all storage | Spark SQL / spark.sql() |
| python-dotenv | Environment variables replaced by Databricks Secrets | dbutils.secrets.get() |
| Click | No CLI interface in Databricks; notebooks are the execution unit | Databricks Workflows for orchestration |
| dropbox SDK | Output replaced by Databricks presentation layers | AI/BI Dashboards, Genie, Apps |
| Poetry/nox | No local package management needed; Databricks manages its own environment | %pip install in notebooks, cluster libraries |
| selenium/playwright | Not needed; both MLS and Rentfaster have JSON APIs (no browser rendering required) | requests library |
| scrapy | Over-engineered for two simple API endpoints; adds unnecessary complexity | requests in notebooks |

## Data Storage Architecture

### Unity Catalog Structure

```
catalog: wheretolive
  schema: bronze
    table: mls_raw          -- Raw JSON responses, one row per listing per scrape
    table: rfaster_raw       -- Raw JSON responses, one row per listing per scrape
  schema: silver
    table: mls_listings      -- Cleaned, typed, deduplicated MLS listings
    table: rfaster_listings  -- Cleaned, typed, deduplicated Rentfaster listings
  schema: gold
    table: all_listings      -- Unified view across MLS and Rentfaster
    table: listings_by_neighbourhood  -- Aggregated stats
    table: price_trends      -- Time series of price movements
    table: candidate_listings -- Per-user filtered views (parameterized by secrets)
  schema: app
    -- Lakebase tables for App-specific state (saved searches, user prefs display)
```

**Confidence:** HIGH for bronze/silver/gold pattern. LOW for Lakebase app schema -- verify Lakebase supports Unity Catalog schemas or whether it uses its own namespace.

### Delta Table Configuration

| Setting | Value | Why |
|---------|-------|-----|
| Format | Delta (default) | ACID, time travel, schema evolution |
| Partitioning | scrape_date on bronze tables | Daily scrapes are the natural partition; keeps reads efficient for latest-day queries |
| Liquid clustering | Consider for gold tables on neighbourhood, listing_type | Better than static partitioning for query patterns that vary; available in recent Delta Lake versions |
| Z-ordering | price, latitude, longitude on gold tables | Optimizes for range queries on price and spatial proximity lookups in dashboards |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Pipeline framework | Lakeflow Declarative Pipelines (DLT) | Plain Spark notebooks chained in Workflows | DLT handles data quality, lineage, error handling declaratively; notebooks require manual implementation of all these concerns |
| Pipeline framework | Lakeflow Declarative Pipelines (DLT) | dbt on Databricks | dbt is SQL-only by default; existing scraping/parsing logic is Python-heavy; DLT is native and better integrated with Workflows |
| App framework | Dash | Streamlit | Dash offers better map control and callback patterns for the interactive listing explorer use case |
| App framework | Dash | External Flask/FastAPI app | Databricks Apps manages hosting, auth, scaling; external app adds infrastructure management burden |
| Database (App backend) | Lakebase | External PostgreSQL | Lakebase is native to Databricks, no infra to manage; the whole point of the project is learning Databricks features |
| Database (App backend) | Lakebase | Direct Delta table reads | Lakebase provides low-latency transactional reads suitable for an interactive app; Delta scans are slower for point lookups |
| Scraping execution | Databricks notebook in Job | External cron + push to Databricks | Keeps everything in one platform; simpler operational model |
| Scraping library | requests | httpx / aiohttp | Scraping is sequential (rate-limited by design); async adds complexity with no benefit for roughly 100 paginated API calls |
| Orchestration | Databricks Workflows | Apache Airflow | Workflows is native, zero infra; Airflow requires separate deployment and adds operational overhead for a personal project |
| Orchestration | Databricks Workflows | Databricks Asset Bundles (DABs) for deployment | DABs are for CI/CD deployment of pipeline code, not orchestration itself; use DABs alongside Workflows if adding source control deployment later |

## Installation / Setup

No local installation required. All code runs in Databricks notebooks on the workspace.

### Cluster / Compute Configuration

```
Scraping job compute:
  - Single-node (driver only, no workers)
  - Databricks Runtime 15.x LTS or newer
  - Instance type: smallest available (scraping is I/O bound, not compute bound)
  - Auto-termination: 10 minutes

DLT Pipeline compute:
  - Serverless (let Databricks manage; SDP/DLT handles its own compute)
  - OR: Single-node if serverless not available on workspace SKU
  - Enhanced autoscaling: enabled

Dashboard/Genie:
  - Serverless SQL Warehouse (auto-starts on query)
  - Size: Small (data volume is tiny -- hundreds to low thousands of listings)
```

### Notebook Setup Pattern

```python
# Top of every scraping notebook:
# %pip install geocoder

# Access secrets:
cookie_value = dbutils.secrets.get(scope="wheretolive", key="mls-cookie")
candidate_prefs = dbutils.secrets.get(scope="wheretolive", key="ian-preferences")

# Write to bronze Delta table:
from pyspark.sql import functions as F
raw_df = spark.createDataFrame(results_list)
(raw_df
  .withColumn("scrape_date", F.current_date())
  .withColumn("ingestion_timestamp", F.current_timestamp())
  .write
  .mode("append")
  .saveAsTable("wheretolive.bronze.mls_raw"))
```

### DLT Pipeline Definition Pattern

```python
# In a DLT notebook:
import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Cleaned MLS listings with typed columns",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_location", "latitude IS NOT NULL AND longitude IS NOT NULL")
def mls_listings():
    return (
        dlt.read("mls_raw")
        .select(
            F.col("MlsNumber").alias("mls_number"),
            # ... column transformations
        )
        .dropDuplicates(["mls_number", "scrape_date"])
    )
```

### Databricks App Setup

```yaml
# app.yaml (Databricks App configuration)
command:
  - python
  - app.py
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "your-warehouse-id"
```

```python
# app.py (Dash-based Databricks App)
from dash import Dash, html, dcc, callback, Output, Input
from databricks import sql as dbsql
# OR if using Lakebase:
# import psycopg2  (Lakebase is PostgreSQL wire-compatible)

app = Dash(__name__)
# ... layout with map, filters, listing table
```

## Version Verification Needed

The following versions and features should be verified against current Databricks documentation before implementation:

| Item | What to Verify | Why |
|------|----------------|-----|
| DLT / SDP naming | Confirm current official name (DLT vs Lakeflow Declarative Pipelines vs SDP) | Databricks was in process of renaming; docs may use different terms |
| Lakebase GA status | Confirm if Lakebase is GA or still in preview | Training data shows it as announced but possibly preview-only |
| Lakebase + Unity Catalog integration | Confirm how Lakebase tables relate to Unity Catalog | May have separate namespace or full integration |
| Databricks Apps GA status | Confirm GA and supported frameworks list | Was GA as of late 2024; verify current framework support |
| Genie Spaces availability | Confirm availability on your workspace SKU | May require specific Databricks tier |
| AI/BI Dashboard map visualization | Confirm native map chart type availability | Training data indicates this exists; verify for your workspace |
| DBR LTS version | Check current LTS version (may be 16.x by now) | Use latest LTS for stability |
| Serverless compute availability | Confirm serverless Jobs and SQL Warehouse on your workspace | Availability varies by cloud region and tier |

## Sources

- Codebase analysis: src/wheretolive/mls/scrape.py, src/wheretolive/rfaster/scrape.py, src/wheretolive/mls/parse.py, src/wheretolive/rfaster/parse.py, src/wheretolive/mls/ingest.py, src/wheretolive/postgis.py
- Project context: .planning/PROJECT.md
- Databricks documentation (training data, not live-verified): Delta Live Tables, Unity Catalog, Databricks Workflows, AI/BI Dashboards, Genie, Databricks Apps
- **IMPORTANT:** All Databricks feature claims are from training data (cutoff early 2025). No live documentation was accessible during this research. Verify all version numbers and feature availability against current docs.
