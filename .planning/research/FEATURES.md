# Feature Landscape

**Domain:** Real estate listing aggregator on Databricks lakehouse with AI/BI dashboards, Genie spaces, and Databricks Apps (Lakebase)
**Researched:** 2026-04-14

## Table Stakes

Features users expect. Missing = product feels incomplete.

### Data Pipeline (Ingestion + Medallion Architecture)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Daily automated scraping via Databricks Jobs | Core value proposition -- listings must flow daily without manual intervention | Med | Replace cron/shell scripts with Databricks Workflows job tasks. Scraping notebooks for MLS and Rentfaster run as separate tasks. |
| Bronze layer: raw JSON landing as Delta tables | Medallion architecture is the stated constraint; bronze captures raw data for reprocessability | Low | Store raw API responses with scrape_dt partition. Use append-mode writes from notebooks. No parsing at this stage. |
| Silver layer: parsed/cleaned listings via DLT | Declarative transformations are the learning goal; DLT handles schema enforcement and data quality | Med | DLT pipeline defines expectations (e.g., price > 0, valid lat/lon). Replaces the _ListingCleaner classes. DLT expectations replace manual validation. |
| Gold layer: enriched/aggregated tables for presentation | Presentation layers need pre-joined, query-ready tables | Med | Replaces the PostGIS materialized views (mls_wide, rfaster_wide). Gold tables join listings with any enrichment data available. |
| Deduplication and idempotency | Scraping the same listings across days must not produce duplicates; re-runs must be safe | Med | Use MERGE (upsert) patterns in DLT or silver-layer notebooks. The existing code already handles dedup via drop_duplicates(). |
| Unity Catalog registration | All tables must be discoverable, governed, and lineage-tracked | Low | Register bronze/silver/gold tables in a Unity Catalog schema. Required for AI/BI dashboards and Genie to find data. |
| Workflow orchestration (ingestion + DLT pipeline) | End-to-end pipeline must run as single triggered workflow | Med | Databricks Workflow with task dependencies: scrape tasks -> DLT pipeline task. Daily trigger via cron schedule. |
| Secrets management for sensitive preferences | Candidate filtering criteria are sensitive per PROJECT.md constraints | Low | Store per-user preferences in Databricks secrets. Read at runtime in gold-layer transformations or candidate view generation. |

### AI/BI Dashboard

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Listings overview with key metrics | Dashboard without summary metrics is just a table dump | Low | Total active listings, median price, new-today count, listings by type. Use counter/stat tiles. |
| Filter controls (price range, bedrooms, property type, source) | Users must be able to narrow results to their criteria interactively | Low | AI/BI dashboards support filter widgets that bind to query parameters. Map these to the same criteria as the existing candidate views. |
| Tabular listing detail view | Must see individual listings with links, descriptions, key attributes | Low | Table visualization showing the columns from gold tables. Include clickable links to original listings. |
| Date-based filtering (scrape date, listing date) | Daily pipeline means users want to see "what's new today" vs. historical | Low | Date picker filter widget bound to scrape_dt or insert date columns. |
| Multiple data sources displayed (MLS + Rentfaster) | Both sources are core to the project; dashboard must show both | Low | Either tabbed layout with separate queries per source, or unified gold table with a source column. Unified is better for cross-source comparison. |

### Genie Space

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Connected to gold-layer tables | Genie needs well-structured data to generate useful SQL answers | Low | Point Genie at the gold tables in Unity Catalog. Genie uses table/column metadata to understand the domain. |
| Table and column descriptions in Unity Catalog | Genie's natural language understanding depends heavily on metadata quality | Med | Add COMMENT ON TABLE and COMMENT ON COLUMN for every gold table and column. This is the single most impactful thing for Genie quality. |
| Sample questions provided | Users (and Genie itself) need seed questions to understand what's askable | Low | Configure 5-10 representative questions like "Show me 3-bedroom houses under $500K listed this week" or "What's the median rent for 2-bedroom apartments?" |
| At least one metric view | Genie metric views provide pre-defined aggregation logic that Genie can reference for consistent answers | Med | Define metrics like "median listing price by property type" or "new listings per day by source." These give Genie authoritative answers instead of ad-hoc SQL. |

### Databricks App (with Lakebase)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Interactive listing browser | The App must do something the dashboard cannot -- richer interactivity | Med | A Streamlit, Gradio, or Dash app that lets users browse, sort, and filter listings with a more app-like UX than dashboards provide. |
| Lakebase as application backend | Lakebase is the stated learning goal for the App layer | High | Lakebase (serverless Postgres-compatible database in Databricks) provides low-latency reads for the app. Populate from gold-layer Delta tables via sync or ETL. |
| Per-user candidate filtering | Replaces the old per-user SQL views (ian_candidates, parents_candidates) | Med | App reads user preferences (from secrets or Lakebase user table) and applies them as dynamic filters. Each user sees their own candidate set. |
| Deployment within Databricks workspace | App must be deployable and accessible without external infrastructure | Med | Databricks Apps deploys from a Git repo or workspace files. Must configure app.yaml with resources, permissions, and environment. |

## Differentiators

Features that set this project apart from a basic hello-world. Not expected, but make the learning exercise genuinely valuable.

### Data Pipeline Differentiators

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| DLT expectations with quarantine tables | Understand DLT data quality features deeply -- quarantine bad records instead of dropping them silently | Med | Use @dlt.expect_or_drop and @dlt.expect_or_fail strategically. Quarantine table lets you audit what was rejected and why. |
| Schema evolution handling in bronze layer | Real-world APIs change schemas; handling this gracefully is a key lakehouse skill | Med | Configure mergeSchema or overwriteSchema options on bronze writes. DLT supports schema evolution natively. |
| Historical tracking (SCD Type 2 or append-with-date) | The old system truncated and replaced; a lakehouse should preserve history | Med | Track when listings appear, change price, and disappear. Enables "days on market" and price trend analysis. Append-mode silver table with first_seen_dt/last_seen_dt is simpler than full SCD2. |
| Pipeline monitoring and alerting | Know when scraping fails before noticing stale data days later | Low | Databricks Workflows has built-in email/webhook alerts on failure. Configure on the daily workflow. |
| Unified source schema in gold | Single gold table with MLS and Rentfaster listings normalized to common columns + source tag | Med | Enables cross-source queries. Requires mapping MLS and Rentfaster schemas to common fields (price, bedrooms, bathrooms, sqft, lat/lon, link, source, scrape_dt). |

### AI/BI Dashboard Differentiators

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Map visualization of listings | Spatial data is the natural view for real estate -- lat/lon is already captured | Med | AI/BI dashboards support map visualizations. Plot listings as points colored by price range or source. This is a significant upgrade over the old HTML table output. |
| Price distribution charts | Understand the market, not just individual listings | Low | Histogram or box plot of prices by property type. Simple but immediately useful. |
| New listings trend over time | "Is the market heating up or cooling down?" is a natural question | Low | Line chart of new listing count per day, broken out by source. Requires historical tracking in silver/gold layer. |
| AI-generated summary text | AI/BI dashboards can include AI-generated natural language summaries | Med | Use the AI/BI "AI summary" widget to auto-describe what the data shows. Explore how well this works for real estate context. |
| Cross-filtering between widgets | Click on a bar chart segment to filter the table and map simultaneously | Low | AI/BI dashboards support cross-filtering natively. Wire up chart selections to filter other widgets. |
| Parameterized candidate views | Dashboard equivalent of the old per-user SQL views, but interactive | Med | Use dashboard parameters to let a user input their criteria (price range, min bedrooms, max commute) and see matching listings. More flexible than hardcoded views. |

### Genie Space Differentiators

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Custom instructions for real estate domain | Guide Genie's SQL generation with domain knowledge | Med | Add instructions like "When users ask about 'affordable', use price < $400K for MLS and price < $2500 for Rentfaster" or "Always exclude vacant land from MLS results." |
| Feedback loop review | Learn what questions Genie handles well vs. poorly | Low | Genie captures thumbs-up/down feedback. Regularly review to improve table descriptions, add metric views, or refine instructions. |
| Multiple metric views covering key analyses | Pre-built metrics for common questions give Genie reliable anchors | Med | Metrics for: median price by type, listing count by neighbourhood, avg sqft by bedroom count, new listings by day. Each metric view is a learning exercise in how Genie uses them. |
| Verified answers for critical questions | Mark specific question-SQL pairs as "verified" so Genie returns consistent results | Med | For the most common questions (e.g., "cheapest 3-bed house"), verify and pin the SQL so Genie always returns the same correct answer. |

### Databricks App Differentiators

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Map-based listing explorer with clustering | Interactive map where you can pan/zoom and see listing clusters, click for details | High | Use Streamlit with pydeck or folium. Lakebase provides low-latency spatial queries. This is the "wow" feature the dashboard cannot replicate. |
| Saved searches / watchlists | Users can save filter combinations and get a personalized "what's new" view | Med | Store saved searches in Lakebase. On each visit, show new listings matching saved criteria since last visit. |
| Side-by-side listing comparison | Select 2-3 listings and compare attributes in a table | Med | App-specific interaction pattern impossible in dashboards. Read from Lakebase for instant responses. |
| Price history for individual listings | Show how a listing's price has changed over time | Med | Requires historical tracking in silver layer. App queries Lakebase for price history of a selected listing. |
| Responsive design for mobile viewing | Check listings on phone while walking around neighbourhoods | Low | Streamlit handles basic responsive layout. Not a custom mobile app, but usable on mobile browsers. |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Custom web framework outside Databricks | Defeats the learning purpose; adds infrastructure to maintain | Use Databricks Apps with supported frameworks (Streamlit, Gradio, Dash). The constraint is "all in Databricks." |
| Real-time streaming ingestion | Listings update daily at most; streaming adds complexity with zero benefit | Daily batch via Databricks Workflows. The APIs are scraped once per day. |
| PostGIS spatial operations in Databricks | PostGIS functions (ST_Contains, ST_DistanceSphere) don't exist in Spark SQL | Use H3 geospatial indexing (built into Databricks), simple lat/lon distance calculations, or defer spatial enrichment (isochrones, flood zones) to a future milestone as stated in PROJECT.md. |
| Automated notification/email system | Over-engineering for a personal learning project | View results in dashboard/Genie/App. The old Dropbox HTML upload pattern is replaced by these presentation layers. |
| Multi-tenant authentication system | This is a personal project with a few known users, not a SaaS product | Use Databricks workspace access controls. Per-user preferences via secrets, not a user management system. |
| Complex CI/CD pipeline for the scraping code | Scraping code is simple and changes rarely; heavyweight CI is wasted effort | Use Databricks Repos (Git integration) for version control. Deploy by syncing from Git. Use nox or similar only if tests are already written. |
| Recreating isochrone/commute calculations | Deferred to future milestone per PROJECT.md; depends on external services (OpenTripPlanner, Azure Maps) that add major complexity | Focus on the data that exists (listings with lat/lon). Commute enrichment is explicitly out of scope for this milestone. |
| Recreating Foursquare amenity enrichment | Deferred to future milestone per PROJECT.md | Same as above. Build the pipeline and presentation layers first, enrich later. |
| Building a custom query engine or semantic layer | Genie IS the semantic layer; building another one alongside it is redundant | Invest effort in Genie instructions, metric views, and table metadata instead. |

## Feature Dependencies

```
Unity Catalog registration --> AI/BI Dashboard (needs tables to query)
Unity Catalog registration --> Genie Space (needs table metadata)
Unity Catalog registration --> Databricks App (needs tables or Lakebase sync source)

Bronze Delta tables --> Silver DLT pipeline --> Gold enriched tables
Gold enriched tables --> AI/BI Dashboard
Gold enriched tables --> Genie Space (tables + descriptions)
Gold enriched tables --> Lakebase sync --> Databricks App

Secrets management --> Gold candidate filtering (per-user preferences)
Secrets management --> Databricks App per-user views

Table/column descriptions --> Genie quality (descriptions are THE input to Genie)
Metric views --> Genie verified answers (metrics anchor Genie's responses)

Historical tracking (silver) --> Price trend charts (dashboard)
Historical tracking (silver) --> Price history per listing (App)
Historical tracking (silver) --> "New today" filtering (all presentation layers)

Workflow orchestration --> Daily pipeline runs (everything downstream depends on fresh data)

Lakebase populated from gold --> App interactive features (low-latency queries)
Map visualization (dashboard) --> Map explorer (App) [App version builds on dashboard learnings]
```

## MVP Recommendation

Prioritize (Phase 1 -- Pipeline):
1. **Daily scraping via Databricks Jobs** -- without data, nothing else works
2. **Bronze/silver/gold Delta tables in Unity Catalog** -- foundation for all three presentation layers
3. **DLT pipeline for silver transformations** -- core learning goal
4. **Workflow orchestration** -- automates the daily cycle
5. **Deduplication and idempotency** -- data quality from day one

Prioritize (Phase 2 -- Dashboard):
1. **Listings overview with metrics and filters** -- table stakes for AI/BI dashboard
2. **Map visualization** -- natural view for real estate, high-impact differentiator
3. **Table/column descriptions** -- needed for Genie, but also makes dashboard queries self-documenting

Prioritize (Phase 3 -- Genie):
1. **Table and column descriptions** -- single highest-impact feature for Genie
2. **Sample questions** -- low effort, high impact for usability
3. **At least one metric view** -- learn how metric views change Genie behavior
4. **Custom instructions** -- real estate domain guidance

Prioritize (Phase 4 -- App):
1. **Basic interactive listing browser** -- table stakes for the App
2. **Lakebase backend setup and sync** -- learning goal
3. **Per-user candidate filtering** -- replaces the old SQL candidate views
4. **Map-based explorer** -- the flagship differentiator for the App

Defer:
- **Saved searches / watchlists**: Nice-to-have, do after core App works
- **Side-by-side comparison**: Polish feature, only if time allows
- **Schema evolution handling**: Worth learning but not blocking for MVP
- **Verified Genie answers**: Iterative improvement after initial Genie setup
- **DLT quarantine tables**: Deeper DLT exploration after basic pipeline works
- **Unified cross-source gold table**: Useful but can start with separate MLS/Rentfaster gold tables and unify later

## Sources

- Existing codebase analysis: src/wheretolive/mls/, src/wheretolive/rfaster/, scripts/postgis/
- PROJECT.md constraints and decisions
- Databricks documentation (training data, not live-verified -- MEDIUM confidence for Databricks-specific feature capabilities)
- Note: Web search and fetch tools were unavailable during this research. Databricks feature capabilities (AI/BI dashboard visualization types, Genie metric view configuration, Lakebase specifics, Databricks Apps deployment model) are based on training data as of early 2025. These should be verified against current documentation during implementation, particularly Lakebase which was in preview and may have changed significantly.
