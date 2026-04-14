# Architecture

**Analysis Date:** 2026-04-14

## Pattern Overview

**Overall:** Modular pipeline-based architecture with data ingestion, processing, and export workflows. The system is organized around separate data sources (MLS and Rentfaster) with a common database backend (PostGIS) and utility layers for logging, storage, and external integrations.

**Key Characteristics:**
- Layered architecture separating scraping, parsing, ingestion, and output operations
- Duplicate module structure for two data sources (MLS and Rentfaster) with identical pipeline patterns
- External service integrations (Foursquare, Dropbox, PostGIS) accessed through dedicated modules
- File-based data processing during intermediate stages (ZIP files, Parquet, JSON)
- Database-driven candidate filtering and reporting

## Layers

**Scraping Layer:**
- Purpose: Extract raw data from external web sources and APIs
- Location: `src/wheretolive/mls/scrape.py`, `src/wheretolive/rfaster/scrape.py`
- Contains: HTTP requests, web scraping logic, data collection from MLS and Rentfaster APIs
- Depends on: External APIs, requests library, logging
- Used by: Daily runner scripts

**Parsing Layer:**
- Purpose: Clean, validate, and transform raw scraped data into structured formats
- Location: `src/wheretolive/mls/parse.py`, `src/wheretolive/rfaster/parse.py`
- Contains: Data cleaning logic, format validation, type coercion, removal of invalid records (e.g., vacant land)
- Depends on: pandas, zipfile, JSON processing utilities
- Used by: Ingestion layer

**Ingestion Layer:**
- Purpose: Load cleaned data into PostGIS database and refresh materialized views
- Location: `src/wheretolive/mls/ingest.py`, `src/wheretolive/rfaster/ingest.py`
- Contains: Database insert logic, materialized view refresh, geometry conversion
- Depends on: SQLAlchemy, PostGIS connection, pandas
- Used by: Daily runner scripts

**Output/Export Layer:**
- Purpose: Extract filtered results and export to external storage
- Location: `src/wheretolive/mls/upload_candidates.py`, `src/wheretolive/rfaster/upload_candidates.py`
- Contains: SQL queries for candidate filtering, HTML report generation, Dropbox upload
- Depends on: PostGIS connection, pandas, Dropbox integration
- Used by: Daily runner scripts

**Data Persistence Layer:**
- Purpose: Manage database connections and metadata reflection
- Location: `src/wheretolive/postgis.py`
- Contains: Base database class using singleton metadata pattern, connection management, table metadata reflection
- Depends on: SQLAlchemy, environment variables for credentials
- Used by: All layers requiring database access

**Infrastructure & Cross-Cutting Concerns:**
- Purpose: Provide shared utilities for logging, external integrations, and spatial data
- Location: `src/wheretolive/logconf.py`, `src/wheretolive/dropbox_uploader.py`, `src/wheretolive/foursquare.py`, `src/wheretolive/isochrone.py`
- Contains: Logging configuration, Dropbox API integration, Foursquare grocery store data fetching, isochrone data loading
- Depends on: External APIs, environment variables, file system
- Used by: All layers

## Data Flow

**Daily MLS Workflow:**

1. **Scrape** (`daily_mls.py` calls `scrape_all()`)
   - Extract listings from MLS API by price brackets
   - Save as ZIP-compressed JSON files in `/data/mls/YYYY-MM-DD/`
   
2. **Parse** (`parse_scrapes()`)
   - Load ZIP files and deserialize JSON
   - Validate each listing, skip vacant land
   - Parse fields: bedrooms (above/below grade), dates, descriptions
   - Export to Parquet format: `/data/mls/YYYY-MM-DD/clean.pq`

3. **Ingest** (`update_mls_postgis()`)
   - Read Parquet file for the day
   - Truncate existing `mls` table
   - Insert all records into PostgreSQL
   - Convert latitude/longitude to PostGIS geometry (SRID 4326)
   - Refresh materialized views (`mls_wide`)

4. **Export** (`upload_candidate("user_name")`)
   - Query `user_name_candidates` view from database
   - Filter for new entries (by date)
   - Generate two HTML reports (full and newest)
   - Upload to Dropbox at `/wheretolive/{user_name}/mls_*.html`
   - Clean up local files

**Daily Rentfaster Workflow:**

Similar to MLS but uses `rfaster_candidates` views and separate file paths `/data/rfaster/YYYY-MM-DD/`.

**State Management:**

- **Raw data:** File-based (ZIP + JSON in `/data/mls/` or `/data/rfaster/`)
- **Processed data:** Parquet files (`clean.pq`) for intermediate storage
- **Canonical state:** PostgreSQL database (`mls` and `rfaster` tables)
- **Materialized views:** Pre-filtered candidate lists by user criteria (e.g., `ian_candidates`, `parents_candidates`)
- **Output state:** HTML reports pushed to Dropbox, local copies deleted after upload

## Key Abstractions

**_ListingCleaner (MLS):**
- Purpose: Encapsulate validation and field parsing logic for individual listings
- Examples: `src/wheretolive/mls/parse.py` class `_ListingCleaner`
- Pattern: Object-oriented with methods for each field type (`_parse_bedrooms()`, `_parse_date()`, etc.)

**Candidate Upload Function:**
- Purpose: Abstraction for querying, filtering, and exporting user-specific results
- Examples: `src/wheretolive/mls/upload_candidates.py:upload_candidate()`, `src/wheretolive/rfaster/upload_candidates.py:upload_candidate()`
- Pattern: Function takes `requestor_name` parameter, queries database view, generates and exports files

**PostGIS Singleton:**
- Purpose: Centralize database connection management and metadata reflection
- Examples: `src/wheretolive/postgis.py` classes `_DB`, `PostGIS`
- Pattern: Singleton pattern for metadata, lazy connection creation via property

**Data Source Helpers:**
- Purpose: Centralize path resolution for scraped data directories
- Examples: `src/wheretolive/mls/common.py:_find_scrapes_dir()`, `src/wheretolive/rfaster/common.py:_find_scrapes_dir()`
- Pattern: Functions to locate data by date, returns `Path` objects

## Entry Points

**Daily MLS Runner:**
- Location: `scripts/daily_mls.py`
- Triggers: Scheduled by external cron/orchestration, manually invoked during development
- Responsibilities: Orchestrate scrape → parse → ingest → export pipeline for MLS data
- Calls: `scrape_all()`, `parse_scrapes()`, `update_mls_postgis()`, `upload_candidate()` for each user

**Daily Rentfaster Runner:**
- Location: `scripts/daily_rfaster.py`
- Triggers: Scheduled by external cron/orchestration, manually invoked during development
- Responsibilities: Orchestrate scrape → parse → ingest → export pipeline for Rentfaster data
- Calls: Same pattern as MLS runner but for Rentfaster modules

**Isochrone Setup:**
- Location: `src/wheretolive/isochrone.py` (can be run as `__main__`)
- Triggers: One-time setup, ad-hoc data updates
- Responsibilities: Create/refresh isochrone tables and insert travel time geometry data

**Foursquare Grocery Data:**
- Location: `src/wheretolive/foursquare.py` (can be run as `__main__`)
- Triggers: One-time data collection, periodic refresh
- Responsibilities: Query Foursquare API for grocery store locations across Calgary grid, export to CSV

**PostGIS Setup:**
- Location: `scripts/postgis/setup_db.py`
- Triggers: Initial database setup
- Responsibilities: Execute SQL scripts to create tables and views

## Error Handling

**Strategy:** Logging-based with exception raising. Errors are logged at appropriate levels (INFO, DEBUG, WARNING) and exceptions propagate to caller.

**Patterns:**
- Validation errors during parsing log warnings but skip invalid records (graceful degradation)
- Database connection errors raise exceptions (fail-fast)
- Dropbox authentication and upload errors raise `RuntimeError` with descriptive messages
- Missing files or directories raise `FileNotFoundError`

## Cross-Cutting Concerns

**Logging:** Configured centrally in `src/wheretolive/logconf.py`. Both file-based (daily log files in `/data/logs/YYYY-MM-DD_logs.txt`) and console output. Standard format: timestamp, logger name, level, message.

**Validation:** Data validation occurs during parsing phase (`_ListingCleaner` class for MLS, similar pattern for Rentfaster). Validation rules filter out invalid types (e.g., vacant land).

**Authentication:** Environment variables for external services:
- `POSTGIS_PASS`: Database password
- `FOURSQUARE_KEY`: Foursquare API token
- `DROPBOX_KEY`: Dropbox access token
- Loaded via `python-dotenv` from `.env` file

**Geospatial:** PostGIS provides spatial operations. Latitude/longitude converted to POINT geometries with SRID 4326 (WGS84) during ingestion. Isochrone data stored as GeoJSON polygons.

---

*Architecture analysis: 2026-04-14*
