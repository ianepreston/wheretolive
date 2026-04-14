# Pitfalls Research

**Domain:** Databricks lakehouse migration (local Docker/PostGIS/Python to Databricks)
**Researched:** 2026-04-14
**Confidence:** MEDIUM (based on Databricks documentation and practitioner knowledge; WebSearch/WebFetch unavailable for live verification of latest changes -- some features like Lakebase are very new and may have evolved)

## Critical Pitfalls

### Pitfall 1: Carrying Forward Local File-System Patterns Into Databricks

**What goes wrong:**
The existing codebase writes scraped JSON to zipped files on local disk, reads them back, parses to Pandas DataFrames, then loads to PostGIS. Developers instinctively replicate this pattern in Databricks -- writing intermediate files to DBFS or volumes, then reading them back. This creates unnecessary complexity, DBFS deprecation risk, and bypasses the entire point of Delta tables.

**Why it happens:**
The existing `scrape.py` files use `_dump_result()` to save zipped JSON to disk as intermediate storage. When migrating, it feels natural to replicate this pattern with DBFS paths or Unity Catalog volumes. The scraping code is already structured around file I/O, and it is tempting to keep that structure.

**How to avoid:**
Scrape directly into a Spark DataFrame or Pandas DataFrame and write to a bronze Delta table in one step. There is no need for intermediate file storage. The scraping notebook should `requests.post()` -> parse JSON -> `spark.createDataFrame()` -> `.write.mode("append").saveAsTable("catalog.bronze.mls_raw")`. For Rentfaster, the same pattern applies: HTTP GET -> JSON -> DataFrame -> Delta table.

**Warning signs:**
- Any code that writes to `/dbfs/`, `/Volumes/`, or uses `zipfile` in a Databricks notebook
- Seeing `Path` or `pathlib` usage in notebooks for data storage
- Intermediate parquet files being created before loading to Delta

**Phase to address:**
Phase 1 (Ingestion/Bronze layer) -- establish the correct pattern from the very first notebook.

---

### Pitfall 2: DLT/SDP Pipeline Misunderstanding -- Declarative vs. Imperative Mental Model

**What goes wrong:**
DLT (now called Serverless Data Pipelines / SDP) uses a declarative model. You define what a table should look like, not how to update it. Developers coming from imperative ETL (like the existing `TRUNCATE` + `INSERT` pattern in `ingest.py`) try to write procedural logic inside DLT -- calling `spark.sql("TRUNCATE...")`, using mutable state, or trying to control execution order manually. DLT pipelines fail or behave unpredictably.

**Why it happens:**
The existing code does `conn.execute("TRUNCATE mls;")` followed by `mls_df.to_sql(...)`. This is classic imperative ETL. DLT expects you to define tables with `@dlt.table` or `@dlt.view` decorators, and the framework manages materialization. The mental model shift is significant.

**How to avoid:**
- Bronze: Use `@dlt.table` with `spark.readStream` from an Auto Loader source (if landing files) or define as a standard table populated by the ingestion job that runs before the DLT pipeline task in the Workflow.
- Silver: Use `@dlt.table` with `dlt.read("bronze_table")` as input, applying cleaning transformations declaratively.
- Gold: Use `@dlt.table` reading from silver, applying aggregations.
- Never use `spark.sql()` for DDL inside a DLT pipeline. Never TRUNCATE. Let DLT manage table lifecycle.
- For the "complete refresh" pattern (scrape replaces all listings daily), use a DLT COMPLETE materialization mode or design the pipeline so bronze accumulates daily snapshots and silver/gold derive the latest view.

**Warning signs:**
- `spark.sql("DROP TABLE")` or `spark.sql("TRUNCATE")` inside DLT notebooks
- Trying to pass parameters or runtime arguments into DLT table definitions
- Pipeline runs succeed but tables are empty or have unexpected row counts
- Errors about "table already exists" or "cannot modify table in DLT pipeline"

**Phase to address:**
Phase 2 (Silver/Gold transformation layer via SDP/DLT) -- must be correct from the start. Redesign is painful because DLT manages its own metadata.

---

### Pitfall 3: Mixing Scraping (Side-Effects) Into DLT Pipelines

**What goes wrong:**
Developers try to put the web scraping (HTTP calls to Realtor.ca and Rentfaster APIs) inside a DLT pipeline. DLT is designed for data transformations on existing data, not for side-effect-heavy operations like HTTP scraping. DLT retries, parallelism, and declarative execution conflict with rate-limited, sequential web scraping. Scraping tasks fail intermittently, DLT retries them causing duplicate data or API bans, and debugging becomes extremely difficult.

**Why it happens:**
The desire to have "one pipeline that does everything" is strong. Since the Workflow will orchestrate the full flow, it seems simpler to put scraping inside DLT. Also, the project description mentions "SDP pipeline" which might be mentally expanded to include ingestion.

**How to avoid:**
Strict separation in the Databricks Workflow:
1. **Task 1: Scraping notebook** (regular Python task, not DLT) -- runs `requests.post()` to Realtor.ca, writes raw JSON responses as rows into `bronze.mls_raw` Delta table using `spark.createDataFrame()`.
2. **Task 2: Scraping notebook** (regular Python task) -- same for Rentfaster.
3. **Task 3: DLT/SDP pipeline task** -- reads from bronze Delta tables, transforms to silver and gold. No HTTP calls, no external side effects.

The Workflow `depends_on` ensures Task 3 only runs after Tasks 1 and 2 complete.

**Warning signs:**
- `import requests` in a DLT notebook
- `time.sleep()` in a DLT notebook
- DLT pipeline runs taking much longer than expected due to scraping
- Intermittent DLT failures correlated with external API availability

**Phase to address:**
Phase 1 (Ingestion) -- the architecture decision of "scraping is a Job task, not a DLT step" must be made before any code is written.

---

### Pitfall 4: Unity Catalog Schema Design -- Too Flat or Too Deep

**What goes wrong:**
Developers either put everything in one schema (`default.mls_bronze`, `default.mls_silver`, `default.mls_gold`, `default.rfaster_bronze`...) or create overly nested structures. The flat approach makes access control messy and pollutes table listings. The deep approach creates unnecessary complexity for a personal project.

**Why it happens:**
Unity Catalog has a three-level namespace: `catalog.schema.table`. Coming from PostGIS where everything was in `public` schema, developers either replicate the flat structure or over-engineer based on enterprise patterns they read about.

**How to avoid:**
Use one catalog (e.g., `wheretolive`) with three schemas matching medallion layers:
- `wheretolive.bronze.mls_raw`, `wheretolive.bronze.rfaster_raw`
- `wheretolive.silver.mls_listings`, `wheretolive.silver.rfaster_listings`
- `wheretolive.gold.all_listings`, `wheretolive.gold.ian_candidates`, `wheretolive.gold.price_trends`

This maps cleanly to DLT's `target_schema` configuration -- you can set the DLT pipeline to target `silver` schema and all `@dlt.table` definitions land there.

**Warning signs:**
- All tables in `default` schema
- Schema names that don't communicate layer (e.g., `schema1`, `data`)
- More than 4-5 schemas for this project's scope

**Phase to address:**
Phase 1 (Infrastructure setup) -- Unity Catalog structure must be defined before the first table is created. Renaming later requires pipeline recreation.

---

### Pitfall 5: Hardcoding Candidate Preferences in SQL or Notebooks

**What goes wrong:**
The existing `candidate_views.sql` hardcodes per-user filtering criteria directly in SQL (`WHERE price BETWEEN 300000 AND 650000 AND bedrooms >= 3`). If this pattern is carried to Databricks notebooks or DLT definitions, preferences end up in source control, are hard to update, and violate the project's own constraint that "candidate criteria are sensitive."

**Why it happens:**
It is the existing pattern (see `candidate_views.sql` with `ian_candidates`, `parents_candidates`, `jill_candidates`). It works. The temptation is to just copy the SQL into a DLT gold-layer definition.

**How to avoid:**
Store candidate preferences in Databricks secrets (as PROJECT.md specifies). Design the gold layer so that:
1. A shared gold table (`gold.all_listings_enriched`) contains all listings with all computed fields.
2. Candidate filtering is applied at query time (in dashboards, Genie, or the App) using preferences fetched from secrets or a small config table.
3. Alternatively, a parameterized notebook generates candidate views by reading preferences from `dbutils.secrets.get()` and creating per-user views dynamically.

Do NOT put preference values in DLT `@dlt.table` definitions -- DLT tables cannot accept runtime parameters, so hardcoded filters there are even more brittle than in plain SQL.

**Warning signs:**
- Price ranges, bedroom counts, or location names appearing as literals in DLT notebooks
- Per-user view definitions in source-controlled SQL
- Secrets scope not created in workspace setup

**Phase to address:**
Phase 3 (Gold layer / candidate filtering) -- but the secrets scope should be created in Phase 1 infrastructure setup.

---

### Pitfall 6: Genie Space With Poor Table Descriptions and No Sample Questions

**What goes wrong:**
Genie (the natural language query interface) performs poorly because the underlying tables lack descriptive column comments, table descriptions, and the Genie space has no curated sample questions or metric definitions. Users ask questions and get wrong or no answers. The conclusion is "Genie doesn't work" when the real issue is insufficient metadata.

**Why it happens:**
Developers focus on getting data into tables and skip the metadata step. Column names like `sq_feet_in`, `gf_work_car_time`, or `m_to_grocery` are meaningless to a language model without descriptions. Genie relies heavily on Unity Catalog column comments and the space's "instructions" and "sample questions" configuration.

**How to avoid:**
- Add `COMMENT` on every column in gold tables: `ALTER TABLE wheretolive.gold.all_listings ALTER COLUMN sq_feet_in COMMENT 'Interior square footage of the property'`
- Add table comments: `COMMENT ON TABLE wheretolive.gold.all_listings IS 'Daily scraped real estate listings from MLS and Rentfaster for Calgary, enriched with computed fields'`
- When creating the Genie space, provide 10-15 sample questions that map to the actual columns
- Define metric views (e.g., "average price by neighborhood", "new listings this week") as explicit assets in the space
- Add instructions explaining domain terms (e.g., "above grade bedrooms means bedrooms not in the basement")
- Review Genie's generated SQL for the first 10 questions and provide feedback through the "thumbs up/down" mechanism

**Warning signs:**
- Gold tables have columns with no comments
- Genie space has 0 sample questions
- First natural language query returns wrong results or errors
- Genie generates SQL referencing non-existent columns (hallucination from poor metadata)

**Phase to address:**
Phase 4 (Genie space) -- but column comments should be added in Phase 3 when gold tables are created. Retrofitting comments is tedious.

---

### Pitfall 7: AI/BI Dashboard -- Treating It Like a BI Tool Instead of an AI-Assisted Tool

**What goes wrong:**
Developers try to build complex, highly customized dashboards with pixel-perfect layouts in AI/BI dashboards. The tool is designed for quick, AI-assisted dashboard creation -- you describe what you want and it generates visualizations. Fighting against its conventions (trying to force specific chart types, exact positioning, custom CSS) leads to frustration and poor results.

**Why it happens:**
Experience with traditional BI tools (Tableau, Power BI, Looker) sets expectations for full layout control. AI/BI dashboards trade fine-grained control for speed and simplicity. The "AI" part means the tool generates SQL and chart configurations from natural language descriptions.

**How to avoid:**
- Start by describing the dashboard in natural language: "Show me a map of listings colored by price, a price distribution histogram, and a table of recent listings"
- Use the AI assistant to generate initial visualizations, then refine
- Accept that layouts will follow the tool's grid system, not pixel-perfect designs
- For complex custom visualizations, plan to use the Databricks App (Phase 5) instead
- Focus dashboards on standard chart types: maps, bar charts, line charts, tables with filters
- Use dashboard parameters for interactive filtering (price range, property type, date range)

**Warning signs:**
- Spending more than 30 minutes trying to customize a single chart's appearance
- Wanting to embed custom JavaScript or HTML in the dashboard
- Dashboard has more than 10 visualizations (probably too complex for this tool)

**Phase to address:**
Phase 4 (AI/BI Dashboard) -- set expectations before starting. The dashboard is for quick insights, the App is for custom UX.

---

### Pitfall 8: Databricks App -- Underestimating the Lakebase Learning Curve

**What goes wrong:**
Lakebase is Databricks' managed PostgreSQL-compatible database for low-latency app queries. Developers assume it works exactly like their existing PostGIS -- including spatial functions, materialized views, and extensions. Lakebase has a different feature set. Spatial queries that worked in PostGIS (ST_Distance, ST_Within, isochrone joins) may not be directly available, requiring alternative approaches.

**Why it happens:**
The project's existing architecture heavily uses PostGIS spatial functions (`ST_SetSRID`, `ST_MakePoint`, spatial joins for commute isochrones). "PostgreSQL-compatible" suggests drop-in replacement, but Lakebase is a managed service optimized for app-serving workloads, not a full PostGIS replacement.

**How to avoid:**
- Verify Lakebase's supported functions before designing the App's data model. Check whether GIS extensions are available.
- Plan for the possibility that spatial operations stay in the silver/gold Delta layer (computed by Spark) and Lakebase only stores pre-computed results.
- Design the App data model so Lakebase serves flat, pre-joined, pre-filtered tables -- not raw data requiring complex spatial joins at query time.
- Pre-compute all commute times, distances, and spatial relationships in the gold layer and store results as simple columns (e.g., `downtown_drive_minutes: 15`) rather than relying on spatial functions in Lakebase.
- Fall back to using Delta tables directly via Databricks SQL if Lakebase limitations are too restrictive.

**Warning signs:**
- Attempting to create PostGIS extensions in Lakebase
- App queries failing with "function not found" for spatial operations
- App latency much higher than expected because queries are too complex for Lakebase
- Designing the App assuming Lakebase = PostGIS

**Phase to address:**
Phase 5 (Databricks App) -- but spatial computation strategy should be decided in Phase 2/3 when designing silver/gold transformations. If gold tables pre-compute everything, Lakebase just needs simple SELECTs.

---

### Pitfall 9: Web Scraping Rate Limits and IP Bans From Databricks Infrastructure

**What goes wrong:**
The existing scraping code runs from a home network with residential IP. When moved to Databricks (cloud infrastructure), the source websites (Realtor.ca, Rentfaster) are more likely to block or rate-limit requests from cloud IP ranges. Scrapers that worked locally fail intermittently or permanently on Databricks.

**Why it happens:**
Realtor.ca already requires cookie/header spoofing (see `_mls_cookie()` and the mobile user-agent in the existing code). Cloud provider IP ranges are well-known and commonly blocked by anti-scraping services (the `incap_ses` and `reese84` cookies in the current code are Incapsula/Imperva bot-detection tokens). Running from Azure/AWS/GCP IPs makes detection more likely.

**How to avoid:**
- Test scraping from a Databricks notebook early -- do not assume it will work just because it works locally.
- Keep retry logic with exponential backoff (the existing `max_attempts_page` with `time.sleep(5)` is a start but needs exponential backoff).
- If cloud IPs are blocked, consider: (a) running scraping externally (e.g., GitHub Actions cron job, home server) and uploading results to a Unity Catalog volume as the bronze ingestion source, or (b) using a proxy service.
- Rentfaster's API is more open (no cookie spoofing needed), so it is more likely to work from Databricks directly.
- Monitor scrape success rates from the first day -- do not wait until the pipeline is "complete" to discover scraping is blocked.

**Warning signs:**
- HTTP 403 responses from Realtor.ca when running from Databricks
- Captcha challenges (Imperva/Incapsula responses instead of JSON)
- Scraping notebook succeeds locally but fails in Databricks Job
- Intermittent `RuntimeError` from the existing retry logic

**Phase to address:**
Phase 1 (Ingestion) -- this is a day-one blocker. Test scraping from Databricks before building anything else. Have a fallback plan ready.

---

### Pitfall 10: DLT Pipeline Compute Costs -- Serverless Pricing Surprises

**What goes wrong:**
DLT pipelines (especially in serverless mode) spin up compute on demand. For a daily pipeline processing a few thousand listings, the dataset is small but the pipeline still provisions compute resources. If not configured carefully, you pay for minimum cluster startup time even for trivially small workloads. Over a month, costs add up surprisingly for what feels like a simple job.

**Why it happens:**
Databricks serverless pricing charges for DBU consumption. Even a small DLT pipeline has a minimum runtime due to cluster spin-up. If the pipeline is triggered multiple times for testing/development, costs multiply. The project's data volume (maybe 1000-3000 listings per scrape) does not justify large compute.

**How to avoid:**
- Use the smallest possible compute configuration for the DLT pipeline (serverless auto-scales down, but be aware of minimum costs).
- During development, use `DEVELOPMENT` mode for the DLT pipeline -- it retains compute between runs and avoids repeated cluster startup costs. Switch to `PRODUCTION` mode only when the pipeline is stable.
- Consider whether DLT is overkill for this data volume -- a simple Python notebook with `spark.createDataFrame()` and `df.write.saveAsTable()` for bronze-to-silver transformation might be simpler and cheaper.
- Set Workflow schedules carefully -- do not accidentally trigger hourly when daily is sufficient.
- Set up cost alerts in the Databricks account.

**Warning signs:**
- Monthly Databricks bill higher than expected for a personal project
- DLT pipeline taking 5+ minutes for a dataset that processes in seconds
- Multiple pipeline runs per day during development without `DEVELOPMENT` mode
- No cost alerts configured

**Phase to address:**
Phase 1/2 (Infrastructure and Pipeline setup) -- choose compute strategy before building pipelines. Monitor costs from day one.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Skipping column comments on Delta tables | Faster initial development | Genie space and AI/BI dashboards perform poorly; every user needs tribal knowledge | Never -- add comments when creating tables, it takes 2 minutes per table |
| Using DBFS instead of Unity Catalog volumes for intermediate files | Familiar file-system semantics | DBFS is deprecated; files outside Unity Catalog lack governance; migration pain later | Never -- DBFS is actively being deprecated |
| Hardcoding secrets in notebooks during development | Faster iteration | Secrets end up in notebook revision history, potentially committed to git | Never -- use `dbutils.secrets.get()` from day one |
| Single notebook for scrape + transform + load | Simpler to develop initially | Cannot use DLT for transforms; workflow task separation becomes painful refactor | Only acceptable for initial proof-of-concept (first day), then refactor immediately |
| Skipping schema evolution configuration in DLT | Simpler table definitions | When Realtor.ca or Rentfaster changes their API response shape, pipeline breaks silently or fails | Never -- set `mergeSchema` or use DLT schema evolution expectations from the start |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Realtor.ca scraping | Assuming the hardcoded cookies in the existing code will work indefinitely | Cookies will expire. Build the scraper to handle auth failures gracefully, refresh cookies programmatically if possible, or accept that manual cookie updates may be needed periodically |
| Rentfaster API | Assuming page numbering is stable | The API returns empty list when pages are exhausted. The existing `while page_list:` pattern is correct but needs defensive handling for API changes (e.g., different empty response format) |
| Databricks Secrets | Storing each preference as a separate secret key | Use one secret per user containing a JSON blob of all preferences. Databricks secrets have a limit on the number of secrets per scope (depends on backing store). Fewer, richer secrets are better |
| DLT reading from external sources | Trying to use Auto Loader for data that is written by another task in the same Workflow | Auto Loader watches for new files in a location. If bronze data is in Delta tables (not files), use `dlt.read()` or `dlt.read_stream()` instead of Auto Loader |
| Lakebase sync from Delta | Assuming Lakebase auto-syncs with gold Delta tables | Lakebase requires explicit data loading. Plan a sync mechanism -- either a notebook task in the Workflow that writes to Lakebase after gold tables are updated, or use Lakebase's Delta integration features if available |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Full table scan in Genie queries | Genie-generated SQL takes 30+ seconds on small tables | Add Z-ORDER or OPTIMIZE on columns frequently filtered (price, scrape_dt, property_type). For this data volume, likely not an issue -- but good practice | At 100K+ rows with complex filters |
| Unpartitioned bronze tables accumulating daily snapshots | Query performance degrades as months of daily scrapes accumulate | Partition bronze tables by `scrape_date`. Or better: silver layer deduplicates and only keeps latest per listing | After 6 months of daily scrapes (~500K rows) |
| Dashboard queries not cached | Every dashboard load re-queries the full dataset | Use dashboard query caching (configured in dashboard settings). Gold tables with OPTIMIZE are also faster to scan | When multiple users view dashboards frequently |
| Pandas UDFs in DLT for simple transformations | Unnecessary serialization overhead for operations Spark handles natively | Use native Spark/SQL functions for cleaning (regexp_replace, cast, when/otherwise). Reserve Pandas UDFs for truly complex logic that needs pandas | When pipeline runtime matters (mostly a code quality issue at this scale) |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Candidate preferences in notebook source code | Preferences reveal personal information (budget, location criteria, family details). If repo is public or shared, this data leaks | Use `dbutils.secrets.get(scope="wheretolive", key="ian_preferences")` and never print secret values in notebook output |
| Scraping cookies/tokens in source code | The existing `_mls_cookie()` contains session tokens. If committed to a public repo, could be used for impersonation | Move all scraping auth tokens to Databricks secrets. Rotate regularly |
| Overly permissive Unity Catalog grants | All users can see all candidate views (including other users' preferences) | Use row-level security or separate gold views per user with appropriate grants. For a personal project with few users, per-user views with `GRANT SELECT` to specific principals |
| Databricks App exposing unfiltered data | App endpoint serves all listings without authentication | Databricks Apps run within the workspace security perimeter. Ensure the App uses the service principal's permissions, not an admin's |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Genie space with too many tables | Genie gets confused about which table to query, returns wrong results | Expose only 2-3 well-documented gold tables to the Genie space. Less is more |
| Dashboard with no default filters | Users see all listings across all categories, overwhelming and useless | Set sensible defaults: current week's listings, price within a reasonable range, map centered on Calgary |
| App showing stale data without indication | Users don't know if they're seeing today's scrape or last week's | Always display "Data as of: [scrape_date]" prominently. Show pipeline status or last-updated timestamp |
| Map visualization without clustering | Hundreds of markers overlap, making the map unusable | Use marker clustering in the App. For AI/BI dashboard maps, ensure the visualization type supports density/clustering |

## "Looks Done But Isn't" Checklist

- [ ] **Bronze tables:** Often missing `scrape_date` column -- verify every row can be attributed to a specific scrape run
- [ ] **DLT pipeline:** Often missing error handling for schema drift -- verify pipeline handles new/removed columns from source APIs gracefully
- [ ] **Gold candidate views:** Often missing deduplication -- verify the same listing scraped on consecutive days appears only once in candidate results (with latest scrape date)
- [ ] **Genie space:** Often missing metric definitions -- verify that "average price" and "new listings" are defined as explicit metrics, not left for Genie to guess
- [ ] **AI/BI dashboard:** Often missing map visualization -- verify lat/long columns are properly typed and the map widget renders listing locations
- [ ] **Databricks App:** Often missing loading states and error handling -- verify the app handles Lakebase connection failures and slow queries gracefully
- [ ] **Workflow scheduling:** Often missing alerting -- verify email/Slack notifications are configured for failed runs
- [ ] **Secrets:** Often missing from workspace -- verify the secrets scope exists and all preference keys are populated before the first gold layer run

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Wrong Unity Catalog structure | MEDIUM | Create new schemas, migrate tables with `CREATE TABLE new_schema.table AS SELECT * FROM old_schema.table`, update all DLT and notebook references. DLT pipeline may need recreation |
| Scraping blocked from cloud IPs | MEDIUM | Set up external scraping (GitHub Actions cron job), upload results to Unity Catalog volume, modify bronze ingestion to read from volume instead of direct HTTP |
| DLT pipeline with imperative logic | HIGH | Must rewrite pipeline notebooks from scratch with declarative pattern. Cannot incrementally fix -- the mental model must change |
| Hardcoded preferences in DLT | MEDIUM | Extract to secrets, modify gold layer to read from secrets. If preferences were in DLT table definitions, pipeline needs recreation |
| Missing column comments for Genie | LOW | Batch `ALTER TABLE ... ALTER COLUMN ... COMMENT` statements. Tedious but mechanical. Do it once, maintain going forward |
| Cost overrun from dev-mode pipelines | LOW | Switch to DEVELOPMENT mode, consolidate test runs, set billing alerts. No technical debt -- purely operational |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Carrying forward file-system patterns | Phase 1 (Bronze/Ingestion) | No `zipfile`, `pathlib`, or DBFS writes in any notebook |
| DLT declarative vs imperative | Phase 2 (Silver/Gold DLT) | All DLT notebooks use only `@dlt.table`/`@dlt.view` decorators, no `spark.sql()` DDL |
| Scraping mixed into DLT | Phase 1 (Ingestion) | Workflow has separate task types: Python task for scraping, DLT task for transforms |
| Unity Catalog schema design | Phase 1 (Infrastructure) | Three schemas exist (bronze, silver, gold) under one catalog before any tables created |
| Hardcoded candidate preferences | Phase 3 (Gold/Candidates) | Secrets scope exists, no literal preference values in any notebook or SQL file |
| Genie poor metadata | Phase 3-4 (Gold tables + Genie) | Every column in gold tables has a COMMENT; Genie space has 10+ sample questions |
| Dashboard expectations mismatch | Phase 4 (Dashboard) | Dashboard scope defined before building: max 6-8 widgets, standard chart types only |
| Lakebase spatial limitations | Phase 5 (App) | Spatial fields pre-computed in gold layer; Lakebase stores flat results only |
| Scraping from cloud IPs blocked | Phase 1 (Ingestion) | Scraping tested from Databricks notebook before building downstream pipeline |
| DLT compute costs | Phase 1-2 (Pipeline setup) | DEVELOPMENT mode for iteration; cost alerts configured; daily schedule (not more frequent) |

## Sources

- Databricks official documentation (DLT, Unity Catalog, Genie, AI/BI dashboards, Databricks Apps)
- Existing project source code analysis (`scrape.py`, `ingest.py`, `parse.py`, `candidate_views.sql`, `postgis.py`)
- Practitioner knowledge of Databricks lakehouse patterns and common migration mistakes
- **Confidence note:** Lakebase is a newer Databricks feature (GA in late 2025). Specific Lakebase limitations are MEDIUM confidence -- verify against current documentation when starting Phase 5. Genie space behavior is MEDIUM confidence -- the feature has been evolving rapidly and UX may have changed.

---
*Pitfalls research for: Databricks lakehouse migration (wheretolive real estate aggregator)*
*Researched: 2026-04-14*
