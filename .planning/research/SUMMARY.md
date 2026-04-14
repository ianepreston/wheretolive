# Project Research Summary

**Project:** WhereToLive — Databricks Migration
**Domain:** Real estate listing aggregator migrated to Databricks lakehouse
**Researched:** 2026-04-14
**Confidence:** MEDIUM (training data through early 2025; Lakebase flagged LOW)

## Executive Summary

Migrate WhereToLive from local Docker/PostGIS to Databricks. Scraping notebooks write raw JSON to bronze Delta tables, a DLT pipeline handles bronze-to-silver-to-gold transformations declaratively, and three presentation layers (AI/BI Dashboard, Genie Space, Databricks App with Lakebase) read from Unity Catalog gold tables. Per-user candidate preferences stored in Databricks Secrets, never in code.

## Recommended Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Runtime | Databricks Runtime LTS (verify current version) | Pre-installed requests/pyspark; stable |
| Governance | Unity Catalog (wheretolive catalog) | Required for AI/BI and Genie |
| Storage | Delta Lake (bronze/silver/gold schemas) | ACID, time travel, schema evolution |
| Transformation | Lakeflow Declarative Pipelines (DLT/SDP) | Declarative, expectations, lineage |
| Orchestration | Databricks Workflows | Native, zero-infra, replaces shell cron |
| Dashboard | AI/BI Dashboards | Maps, filters, charts on gold tables |
| NL Query | Genie Spaces | Quality driven by Unity Catalog metadata |
| App | Databricks Apps (Dash) + Lakebase | Interactive map-centric listing browser |
| Secrets | Databricks Secrets | Replaces hardcoded candidate SQL views |

**Do not use:** pandas (PySpark instead), sqlalchemy, python-dotenv, DBFS writes (deprecated), selenium/scrapy, Poetry/nox, Dropbox SDK.

## Table Stakes Features

- Daily automated scraping via Databricks Jobs
- Bronze/silver/gold Delta tables in Unity Catalog
- DLT pipeline with data quality expectations
- Databricks Workflow orchestrating scrape tasks + DLT pipeline task
- Deduplication and idempotency from day one
- Databricks Secrets for per-user candidate preferences
- AI/BI Dashboard with metrics, filters, map, tabular view
- Genie Space with table/column descriptions and sample questions
- Databricks App with interactive listing browser backed by Lakebase

## Differentiators

- Append-only bronze with full daily snapshot history (replaces TRUNCATE pattern)
- DLT expectations with quarantine tables for rejected records
- Historical tracking in silver (first_seen/last_seen, price change detection)
- Map visualization in AI/BI Dashboard
- Genie metric views (median price by type, new listings by day)
- Map-based listing explorer in App with clustering
- Unified gold table merging MLS and Rentfaster with source tag

## Architecture

Clean producer-consumer boundary:
1. **Scraping notebooks** (imperative, HTTP calls) write raw JSON rows to bronze Delta tables
2. **DLT pipeline** (declarative, no side effects) reads bronze, transforms through silver to gold
3. **Presentation layers** all read from gold tables directly — no ETL between gold and presentation
4. **Lakebase** holds only application operational state (saved searches, UI prefs), never analytical data

**Critical rule:** Scraping and DLT must never be mixed. HTTP calls in separate Workflow notebook tasks only.

## Critical Pitfalls

| # | Pitfall | Prevention | Phase |
|---|---------|-----------|-------|
| 1 | Realtor.ca blocked from cloud IPs | Test scraping from Databricks day one; have fallback plan | 2 |
| 2 | Mixing scraping into DLT pipeline | Scraping in separate notebook tasks only | 2-3 |
| 3 | DLT imperative mental model | No TRUNCATE/INSERT; fully declarative @dlt.table | 3 |
| 4 | File-system intermediate storage | Write directly to Delta tables; no DBFS/parquet files | 2 |
| 5 | Hardcoded candidate preferences | Databricks Secrets as JSON blobs per user | 1 |
| 6 | Missing Unity Catalog metadata | Add COMMENT ON TABLE/COLUMN when creating gold tables | 3 |
| 7 | Lakebase is not PostGIS | Pre-compute spatial fields in gold; Lakebase serves flat results | 7 |

## Recommended Phase Order

| Phase | Name | Rationale |
|-------|------|-----------|
| 1 | Infrastructure & Secrets | UC schema design is irreversible; secrets scope needed before gold layer |
| 2 | Ingestion (Bronze) | Must validate cloud IP scraping works before building downstream |
| 3 | DLT Pipeline (Silver + Gold) | Core learning goal; prerequisite for all presentation layers; add column COMMENTs here |
| 4 | Workflow Orchestration | Wire pieces together once each works independently |
| 5 | AI/BI Dashboard | Simplest presentation layer; proves pipeline end-to-end |
| 6 | Genie Space | Builds on gold metadata from Phase 3; dashboard experience improves questions |
| 7 | Databricks App + Lakebase | Most complex; benefits from all prior phases; highest uncertainty |

## Research Flags

**Needs research during planning:**
- Phase 3: DLT streaming vs. batch mode, DEVELOPMENT vs. PRODUCTION mode costs
- Phase 7: Lakebase GA status, Unity Catalog integration, supported Postgres features

**Standard patterns (skip research):**
- Phase 1: Unity Catalog three-schema setup
- Phase 2: requests -> spark.createDataFrame() -> Delta write
- Phase 4: Workflows notebook + pipeline task
- Phase 5: AI/BI Dashboard conventions
- Phase 6: Genie metadata quality iteration

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack (core) | HIGH | Stable, well-documented Databricks features |
| Architecture (pipeline) | HIGH | Standard medallion pattern |
| Features (pipeline + dashboard) | MEDIUM | Clear landscape |
| Genie Space | MEDIUM | Evolving feature; iteration expected |
| Lakebase | LOW | Newest feature; verify current capabilities |

## Gaps to Address

- **Cloud IP scraping:** Unknown if Databricks Azure IPs blocked by Incapsula. Test in Phase 2.
- **Lakebase capabilities:** Verify GA status, spatial support, Delta sync at Phase 7 start.
- **DLT/SDP naming:** Verify current official product name in workspace UI.
- **DBR LTS version:** Use current LTS at implementation time.

---
*Research completed: 2026-04-14*
*Ready for roadmap: yes*
