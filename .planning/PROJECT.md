# WhereToLive — Databricks Migration

## What This Is

A real estate listing aggregator for Calgary that scrapes MLS (Realtor.ca) and Rentfaster daily, processes listings through a medallion lakehouse architecture in Databricks, and surfaces results through three Databricks presentation layers: AI/BI dashboards, a Genie space, and a Databricks App backed by Lakebase. The primary goal is learning how these Databricks features work in practice.

## Core Value

Daily real estate listings flow reliably from source to presentation, and each presentation layer (dashboard, Genie, App) is polished enough to genuinely teach how the feature works — not just a hello-world.

## Requirements

### Validated

- [x] DAB project structure defines all resources (schema, pipeline, job, notebooks) — Validated in Phase 1: Infrastructure & DAB Setup
- [x] Unity Catalog schema exists and is accessible from notebooks — Validated in Phase 1: Infrastructure & DAB Setup

### Active

- [ ] Scrape MLS and Rentfaster listings daily via Databricks Jobs
- [ ] Store raw scraped data as bronze Delta tables in Unity Catalog
- [ ] Transform and clean listings via Serverless Data Pipeline (DLT) into silver tables
- [ ] Build gold-layer aggregated/enriched tables for presentation
- [ ] Orchestrate full pipeline via Databricks Workflows (ingestion tasks + SDP pipeline task)
- [ ] Support multiple users with candidate preferences stored in Databricks secrets
- [ ] AI/BI dashboard showing listings with filters and map views
- [ ] Genie space with metric views, sample questions, and feedback review mechanism
- [ ] Databricks App with Lakebase backend for richer interactive experience
- [ ] Iterate on each presentation layer beyond first cut to understand what works well

### Out of Scope

- Foursquare amenity enrichment — deferred to future milestone after core pipeline works
- Azure Maps isochrone enrichment — deferred to future milestone
- Dropbox upload/HTML report generation — replaced by Databricks presentation layers
- Local Docker/PostGIS infrastructure — fully replaced by Databricks
- Existing Python package structure (Click CLI, Poetry, nox) — not needed in Databricks
- Mobile app or external-facing deployment — this is a personal learning project

## Context

This is a migration of a 5-year-old personal project. The original ran locally on Docker containers with PostGIS, Python scraping scripts, and Dropbox for output. The architecture patterns (singleton DB connections, file-based intermediate storage, shell script cron orchestration) are artifacts of that era and should not be carried forward.

The user has an existing Databricks workspace ready to use. The migration is explicitly a learning exercise — the goal is deep understanding of Databricks features, not just getting the same functionality running on new infrastructure.

Key existing data sources:
- **MLS (Realtor.ca):** Property listings scraped by price bracket, parsed for bedrooms/dates/descriptions
- **Rentfaster:** Rental listings with similar pipeline structure
- **Candidate filtering:** Per-user views with location/price/size criteria — preferences are sensitive and must not be hardcoded

The current scraping logic in `src/wheretolive/mls/scrape.py` and `src/wheretolive/rfaster/scrape.py` contains the API interaction patterns that need to be adapted for Databricks notebook execution.

## Constraints

- **Platform:** All processing and storage in Databricks (Unity Catalog, Delta tables, Workflows)
- **Sensitive data:** User candidate preferences stored in Databricks secrets, never in code or config files
- **Architecture:** Medallion pattern (bronze/silver/gold) for data layering
- **Orchestration:** Databricks Workflows with SDP (DLT) pipeline task steps for transformations
- **Learning focus:** Each feature (AI/BI, Genie, Apps) must be explored beyond first-cut to understand strengths and patterns

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Databricks Jobs for scraping | Keeps entire pipeline in one platform, simpler orchestration | — Pending |
| Medallion architecture | Standard lakehouse pattern, good learning exercise, clean separation of concerns | — Pending |
| SDP/DLT for transformations | Declarative pipeline for bronze→silver→gold, Workflows for orchestration wrapper | — Pending |
| Dashboard → Genie → App ordering | Dashboard is most straightforward, Genie builds on data layer, App is most complex | — Pending |
| Preferences in Databricks secrets | Candidate criteria are sensitive, must not be in source control | — Pending |
| Drop old infrastructure patterns | Docker/PostGIS/Dropbox/CLI patterns are not fit for Databricks architecture | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-14 after Phase 1 completion*
