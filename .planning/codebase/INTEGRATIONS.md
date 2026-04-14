# External Integrations

**Analysis Date:** 2026-04-14

## APIs & External Services

**Real Estate Data:**
- MLS (Multiple Listing Service) - Property listing scraping and ingestion
  - Scraper: `src/wheretolive/mls/scrape.py`
  - Parser: `src/wheretolive/mls/parse.py`
  - Ingestion: `src/wheretolive/mls/ingest.py`
  - Upload candidates: `src/wheretolive/mls/upload_candidates.py`

- RFaster - Alternative property data source
  - Scraper: `src/wheretolive/rfaster/scrape.py`
  - Parser: `src/wheretolive/rfaster/parse.py`
  - Ingestion: `src/wheretolive/rfaster/ingest.py`
  - Upload candidates: `src/wheretolive/rfaster/upload_candidates.py`

**Location & Amenity Data:**
- Foursquare Places API - Grocery store location and amenity search
  - SDK/Client: `requests` HTTP library
  - Integration: `src/wheretolive/foursquare.py`
  - Endpoint: `https://api.foursquare.com/v3/places/search`
  - Auth: Bearer token via `Authorization` header
  - Environment Variable: `FOURSQUARE_KEY`
  - Usage: Grid-based geographic searches for point-of-interest data

**Travel Time & Routing:**
- Azure Maps Route Range API - Isochrone generation (travel time reachability)
  - Integration: `scripts/make_isochrone.py`
  - Endpoint: `https://atlas.microsoft.com/route/range/json`
  - Auth: Subscription key query parameter
  - Environment Variable: `MAPS_KEY`
  - Features: Drive time isochrones, departure time scheduling, timezone handling

**Geocoding:**
- Geocoder Library - Location coordinate parsing
  - Package: `geocoder` 1.38.1+
  - Used for converting addresses to coordinates

## Data Storage

**Databases:**
- PostgreSQL with PostGIS extension
  - Connection: `src/wheretolive/postgis.py` - singleton pattern via `PostGIS` class
  - Environment Variables: `POSTGIS_PASS` (password)
  - Connection String: `postgresql://postgres:{PG_PASSWORD}@mars:5432/postgres`
  - Client: SQLAlchemy 1.4.27+ ORM
  - Tables: `mls`, `rfaster`, `isochrones`, and materialized views (`mls_wide`)
  - Operations: Listing ingestion, geospatial queries, isochrone storage

**File Storage:**
- Dropbox API - Cloud file backup and upload
  - SDK/Client: `dropbox` 11.25.0+
  - Integration: `src/wheretolive/dropbox_uploader.py`
  - Auth: OAuth2 access token
  - Environment Variable: `DROPBOX_KEY`
  - Usage: Upload scraped data, candidate lists, logs to cloud

**Caching:**
- PostgreSQL Materialized Views
  - Used for pre-computed wide table views (`mls_wide`)
  - Refreshed after data ingestion via `REFRESH MATERIALIZED VIEW` SQL commands

**File Format Storage:**
- Parquet (via FastParquet) - Efficient columnar storage for scraped data
  - Location: Scraped data directories with `clean.pq` files
  - Used by: MLS and RFaster ingestion pipelines

## Authentication & Identity

**Auth Provider:**
- Custom (no centralized auth service)
- Service-level API key authentication

**API Keys Required:**
- `FOURSQUARE_KEY` - Foursquare Places API authentication
- `MAPS_KEY` - Azure Maps Route Range API subscription key
- `POSTGIS_PASS` - PostgreSQL database password
- `DROPBOX_KEY` - Dropbox API access token

**Load Method:**
- Environment variables via `python-dotenv` (loads from `.env` file)
- Validated at module import time: `load_dotenv()` calls in:
  - `src/wheretolive/postgis.py`
  - `src/wheretolive/foursquare.py`
  - `src/wheretolive/dropbox_uploader.py`
  - `scripts/make_isochrone.py`

## Monitoring & Observability

**Error Tracking:**
- None detected - error handling via try/except blocks

**Logging:**
- Custom logging configuration: `src/wheretolive/logconf.py`
- Logger function: `get_logger(__name__)` - used throughout codebase
- Logging levels: Info, warning (via `logger.info()`, `logger.warn()`)
- Output: Console and file logging configured in `logconf.py`

**Debugging:**
- IPython 7.30.1+ - Interactive shell support (in dev dependencies)
- IPykernel 6.8.0+ - Jupyter notebook kernel support for exploratory analysis

## CI/CD & Deployment

**Hosting:**
- Not detected - appears to be personal project with manual deployment

**CI Pipeline:**
- GitHub Actions workflow (inferred from `.readthedocs.yml`)
- ReadTheDocs automatic documentation builds on push

**Task Automation:**
- Nox sessions defined in `noxfile.py`:
  - `pre-commit` - Linting via pre-commit hooks
  - `safety` - Dependency vulnerability scanning
  - `mypy` - Type checking
  - `tests` - Test execution with coverage
  - `xdoctest` - Doctest example validation
  - `docs-build` - Static documentation generation
  - `docs` - Live documentation server with autobuild

**Scheduled Tasks:**
- Shell scripts in `scripts/` directory:
  - `scripts/daily_mls.sh` - Daily MLS scrape and ingest
  - `scripts/daily_rfaster.sh` - Daily RFaster scrape and ingest

## Environment Configuration

**Required env vars:**
- `POSTGIS_PASS` - PostgreSQL database password
- `FOURSQUARE_KEY` - Foursquare API token
- `MAPS_KEY` - Azure Maps API subscription key
- `DROPBOX_KEY` - Dropbox API access token (optional for local use)

**Optional env vars:**
- Variables can be loaded from `.env` file or system environment

**Secrets location:**
- `.env` file (not committed, listed in `.gitignore`)
- Can be generated from template: `env.j2`

**Configuration files:**
- `pyproject.toml` - Project metadata and dependency versions
- `setup.yml` - Infrastructure setup playbook
- Environment template: `env.j2` - Jinja2 for dynamic env generation

## Webhooks & Callbacks

**Incoming:**
- None detected

**Outgoing:**
- ReadTheDocs webhooks (for documentation builds on commit)
- GitHub integration (implicit via ReadTheDocs)

## Data Processing Pipelines

**MLS Pipeline:**
1. Scraping: `wheretolive.mls.scrape.scrape_all()` - Download listings
2. Parsing: `wheretolive.mls.parse.parse_scrapes()` - Clean and standardize data
3. Ingestion: `wheretolive.mls.ingest.update_mls_postgis()` - Load to PostGIS
4. Upload: `wheretolive.mls.upload_candidates.upload_candidate()` - Send to Dropbox
5. Materialized view refresh - Pre-computed wide table

**RFaster Pipeline:**
- Identical structure to MLS: `wheretolive.rfaster.*`

**Isochrone Pipeline:**
- Generation: `scripts/make_isochrone.py`
  - Calls Azure Maps Route Range API
  - Timezone-aware datetime handling for travel time accuracy
  - Handles weekday vs. weekend travel patterns
- Storage: `src/wheretolive/isochrone.py`
  - Creates PostGIS table with GeoJSON geometries
  - SQL script: `src/wheretolive/setup_isochrone.sql`

**Foursquare Amenity Pipeline:**
- Grid-based search: `wheretolive.foursquare.foursquare_groceries()`
- Saves to CSV: `data/foursquare/groceries.csv`

---

*Integration audit: 2026-04-14*
