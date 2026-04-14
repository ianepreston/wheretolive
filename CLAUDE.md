<!-- GSD:project-start source:PROJECT.md -->
## Project

**WhereToLive — Databricks Migration**

A real estate listing aggregator for Calgary that scrapes MLS (Realtor.ca) and Rentfaster daily, processes listings through a medallion lakehouse architecture in Databricks, and surfaces results through three Databricks presentation layers: AI/BI dashboards, a Genie space, and a Databricks App backed by Lakebase. The primary goal is learning how these Databricks features work in practice.

**Core Value:** Daily real estate listings flow reliably from source to presentation, and each presentation layer (dashboard, Genie, App) is polished enough to genuinely teach how the feature works — not just a hello-world.

### Constraints

- **Platform:** All processing and storage in Databricks (Unity Catalog, Delta tables, Workflows)
- **Sensitive data:** User candidate preferences stored in Databricks secrets, never in code or config files
- **Architecture:** Medallion pattern (bronze/silver/gold) for data layering
- **Orchestration:** Databricks Workflows with SDP (DLT) pipeline task steps for transformations
- **Learning focus:** Each feature (AI/BI, Genie, Apps) must be explored beyond first-cut to understand strengths and patterns
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python 3.7+ - All application code, scripts, and CLI tools
## Runtime
- Python 3.7+ (3.9 recommended in noxfile)
- Supported versions: 3.7, 3.8, 3.9 (per pyproject.toml)
- Poetry
- Lockfile: `poetry.lock` (present and maintained)
## Frameworks
- Click 8.0.1+ - CLI command framework (`src/wheretolive/__main__.py`)
- Pydantic 1.8.2+ - Data validation and modeling
- SQLAlchemy 1.4.27+ - ORM and database abstraction (`src/wheretolive/postgis.py`)
- Pandas 1.3.3+ - DataFrames and CSV/Parquet operations (`src/wheretolive/mls/ingest.py`, `src/wheretolive/mls/parse.py`)
- FastParquet 0.7.1+ - Parquet file serialization (used with Pandas)
- NumPy (dependency via Pandas) - Numerical grid operations (`src/wheretolive/foursquare.py`)
- Pytest 6.2.4+ - Test runner and fixtures
- Coverage 6.0+ with TOML support - Code coverage reporting
- MyPy 0.910+ - Static type checking (strict mode enabled)
- Flake8 3.9.2+ - Style linting
- Black 20.8b1+ - Code formatting
- Pre-commit 2.13.0+ - Git hook framework
- Safety 1.10.3+ - Security vulnerability scanning
- Sphinx 4.1.1+ - Documentation generation
- Sphinx-autobuild 2021.3.14+ - Live documentation server
- Sphinx-rtd-theme 1.0.0+ - ReadTheDocs theme
- Sphinx-click 3.0.1+ - CLI documentation from Click
- xdoctest 0.15.10+ - Run doctest examples
- TypeGuard 2.12.1+ - Runtime type guards for mypy
- Darglint 1.8.0+ - Docstring linting
- Flake8-bandit 2.1.2+ - Security checks for flake8
- Flake8-bugbear 21.4.3+ - Bug detection for flake8
- Flake8-docstrings 1.6.0+ - Docstring checks for flake8
- Flake8-rst-docstrings 0.2.3+ - RST docstring validation
- PEP8-naming 0.12.0+ - Naming convention enforcement
- Reorder-python-imports 2.5.0+ - Import ordering (Google style)
- Pre-commit-hooks 4.0.1+ - Common git hooks
- IPython 7.30.1+ - Interactive shell for development
- IPykernel 6.8.0+ - Jupyter kernel support
## Key Dependencies
- SQLAlchemy 1.4.27+ - Database ORM for PostGIS interactions
- Pandas 1.3.3+ - Data frame manipulation for ingestion pipelines
- requests 2.26.0 - HTTP client for external APIs (Foursquare, Azure Maps)
- python-dotenv 0.19.1+ - Environment variable loading from .env files
- psycopg2-binary 2.9.2+ - PostgreSQL/PostGIS database driver
- dropbox 11.25.0+ - Dropbox SDK for file backup and storage
- geocoder 1.38.1+ - Geocoding library (for location parsing)
- pytz (dependency via scripts) - Timezone handling for isochrone calculations
## Configuration
- Loaded via `python-dotenv` (see `load_dotenv()` calls throughout codebase)
- Template: `env.j2` - Jinja2 template for environment file generation
- `pyproject.toml` - Poetry configuration with all dependencies, metadata, and tool config
- `noxfile.py` - Nox automation for testing, linting, type checking, and docs
- `setup.yml` - Ansible playbook for infrastructure setup
- `.pre-commit-config.yaml` - Git hook pipeline (black, flake8, reorder-python-imports, prettier)
- `.flake8` - Flake8 linting configuration (Google import style, max complexity 10)
- `.darglint` - Darglint docstring checking
- `.readthedocs.yml` - ReadTheDocs documentation build config
- `codecov.yml` - Code coverage service configuration
- Minimum threshold: 90% fail_under (set in pyproject.toml)
- Reporting: Combined coverage reports via `coverage combine`
- MyPy strict mode enabled with full type checking
- All source files type-checked via nox session
## Platform Requirements
- Python 3.7.1+ (3.9 recommended)
- Poetry for dependency management
- Nox for task automation
- Git for version control
- Pre-commit hooks enabled
- PostgreSQL/PostGIS database server (connection string in `postgis.py`)
- API keys for external services (via environment variables)
- Dropbox API access (for file uploads)
- PostgreSQL with PostGIS extension
- Environment variables configured for API keys and database credentials
- Python 3.7+ runtime environment
- Network access to external APIs (Foursquare, Azure Maps)
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Module files use lowercase with underscores: `postgis.py`, `logconf.py`, `dropbox_uploader.py`
- Modules have consistent names across similar functionality: `mls/scrape.py`, `mls/parse.py`, `mls/ingest.py`, `rfaster/scrape.py`, `rfaster/parse.py`, `rfaster/ingest.py`
- Test files use `test_` prefix: `test_main.py`, `test_mls.py`, `test_rfaster.py`
- Public functions use snake_case: `get_logger()`, `dropbox_save()`, `foursquare_groceries()`, `get_mls_scrape_df()`, `update_mls_postgis()`
- Private/internal functions (not exported) use leading underscore: `_find_base_dir()`, `_find_scrapes_dir()`, `_mls_cookie()`, `_mls_scrape_page()`, `_parse_bedrooms()`, `_console_logger()`, `_latlong_grid()`, `_get_fs_path()`
- Methods follow same convention: `_is_vacant_land()`, `_parse_date()`, `_parse_listing()`
- Local variables use snake_case with descriptive names: `all_listings`, `raw_bedrooms`, `raw_sq_feet`, `clean_timestamp`, `cleaned_listings`, `listings_df`, `numeric_cols`, `datetime_cols`, `scrape_days`, `base_dir`, `out_dir`, `out_file`
- Abbreviated names used for specific concepts: `db` for database connections, `df` for pandas DataFrames, `conn` for connections, `f` for file handles, `z` for zipfiles
- Short loop variables: `ns`, `ew`, `n`, `s`, `e`, `w` (geographic coordinates), single letters acceptable in limited scope
- Constants using environment variables: `PG_PASSWORD`, `FS_KEY`, `TOKEN` (all caps with underscores)
- Classes use PascalCase: `_DB`, `_ListingCleaner`, `PostGIS`
- Private classes use leading underscore: `_DB` (singleton base), `_ListingCleaner`
## Code Style
- Black code formatter configured via pre-commit hooks
- Max line length: 88 characters (standard Black default)
- Line length violations ignored: E501 (line too long)
- Flake8 is primary linter with strict configuration
- Selection: B,B9,BLK,C,E,F,I,S,W,D (bugbear, black, docstring, error, warning, etc)
- Docstring convention: NumPy style
- Max complexity: 10 (cyclomatic complexity)
- Per-file ignores:
- **black** - Code formatting (runs with require_serial=true)
- **flake8** - Linting (runs with require_serial=true)
- **reorder-python-imports** - Import ordering (application-directories=src)
- **prettier** - Non-Python formatting
- **check-added-large-files** - Prevent large files
- **check-toml** - TOML file validation
- **check-yaml** - YAML validation (excludes vars.yml)
- **end-of-file-fixer** - Trailing newlines
- **trailing-whitespace-fixer** - Remove trailing whitespace
## Import Organization
- `src/wheretolive/foursquare.py`:
- `src/wheretolive/mls/parse.py`:
- No path aliases configured; absolute imports from wheretolive root
- Relative imports not used
- Multiple imports from typing separated on own lines when using single items: `from typing import Dict` on one line, `from typing import List` on next
- When multiple items imported together: `from typing import List, Optional, Tuple`
## Error Handling
- **Specific exception types** - Catch specific exceptions, not generic Exception: `dropbox.exceptions.AuthError`, `dropbox.exceptions.ApiError`, `FileNotFoundError`, `ValueError`
- **Exception chaining** - Use `from err` to preserve error context when re-raising: `raise RuntimeError(...) from err`
- **Try-finally** - Use context managers (`with` statements) for resource management
- **Detailed error messages** - Error messages include context: "Can't find isochrone file at {iso_file}", "Couldn't load Dropbox token from environment variable"
## Logging
- Centralized configuration in `src/wheretolive/logconf.py`
- Exported via `get_logger(name: str) -> logging.Logger`
- File logging to: `data/logs/{YYYY-MM-DD}_logs.txt`
- Console logging with custom formatter
- Get logger at module level: `logger = get_logger(__name__)`
- Log levels used: `INFO`, `DEBUG`, `WARNING`
- F-string formatting for messages: `logger.info(f"Attempting to parse {self._id}")`
- Common messages:
## Comments
- Complex business logic or non-obvious algorithm choices
- SQL queries and database operations: explains the operation
- API parameters: explains what each parameter controls
- Known limitations or workarounds: marks as technical debt
- `src/wheretolive/mls/scrape.py`: Complex cookie construction with comment about why it exists: `# Construct a cookie to make scraping less flaky`
- `src/wheretolive/mls/parse.py`: Data conversion logic commented: `# Want to be able to parse this as None when converting to integer`
- `src/wheretolive/isochrone.py`: SQL syntax example in comment: `# (ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}')) As wkt;`
- NumPy style docstrings required (enforced by flake8-docstrings)
- All public functions have docstrings
- Format includes:
## Type Hints
- Type hints on all function parameters (mypy strict mode enforced)
- Type hints on all return types (including `-> None`)
- Full type annotations for complex types: `List[Path]`, `Dict[str, str]`, `Optional[dt.datetime]`, `Tuple[int, int]`
- Optional parameters: `Optional[str]`, `Optional[dt.datetime]`
- Collections: `List[Dict[str, str]]`, `List[Path]`
- Union types via Optional or union syntax
- Datetime imports as: `import datetime as dt` then use `dt.date`, `dt.datetime`, `dt.date.today()`
- Strict mode enabled
- Settings in `pyproject.toml`:
## Function Design
- Functions use sensible defaults: `date: dt.date = None` (then use `dt.date.today()` if None)
- Multiple parameters with defaults use keyword arguments
- Max 10 parameters typical; complex operations split into helper functions
- Always explicit return type hints
- `-> None` for side-effect functions (logging, database updates)
- Return values via assignment: `self._cleaned = {dict}` then accessed via property
- All methods are instance methods (no class methods or static methods observed)
- Private methods prefixed with underscore
- Properties via `@property` decorator: `connection`, `metadata`
## Module Design
- Modules expose public functions and classes without prefix
- Private functions/classes use leading underscore, not exported
- `__init__.py` files document module purpose in docstring
- `__init__.py` files are minimal (one-line module docstring only)
- No public re-exports in `__init__.py` - consumers import directly from submodules
- Common utilities in `common.py`: `_find_base_dir()`, `_find_scrapes_dir()`
- Scraping in `scrape.py`: Web requests, data fetching
- Parsing in `parse.py`: Data cleaning, validation, transformation
- Ingestion in `ingest.py`: Database loading, updates
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- Layered architecture separating scraping, parsing, ingestion, and output operations
- Duplicate module structure for two data sources (MLS and Rentfaster) with identical pipeline patterns
- External service integrations (Foursquare, Dropbox, PostGIS) accessed through dedicated modules
- File-based data processing during intermediate stages (ZIP files, Parquet, JSON)
- Database-driven candidate filtering and reporting
## Layers
- Purpose: Extract raw data from external web sources and APIs
- Location: `src/wheretolive/mls/scrape.py`, `src/wheretolive/rfaster/scrape.py`
- Contains: HTTP requests, web scraping logic, data collection from MLS and Rentfaster APIs
- Depends on: External APIs, requests library, logging
- Used by: Daily runner scripts
- Purpose: Clean, validate, and transform raw scraped data into structured formats
- Location: `src/wheretolive/mls/parse.py`, `src/wheretolive/rfaster/parse.py`
- Contains: Data cleaning logic, format validation, type coercion, removal of invalid records (e.g., vacant land)
- Depends on: pandas, zipfile, JSON processing utilities
- Used by: Ingestion layer
- Purpose: Load cleaned data into PostGIS database and refresh materialized views
- Location: `src/wheretolive/mls/ingest.py`, `src/wheretolive/rfaster/ingest.py`
- Contains: Database insert logic, materialized view refresh, geometry conversion
- Depends on: SQLAlchemy, PostGIS connection, pandas
- Used by: Daily runner scripts
- Purpose: Extract filtered results and export to external storage
- Location: `src/wheretolive/mls/upload_candidates.py`, `src/wheretolive/rfaster/upload_candidates.py`
- Contains: SQL queries for candidate filtering, HTML report generation, Dropbox upload
- Depends on: PostGIS connection, pandas, Dropbox integration
- Used by: Daily runner scripts
- Purpose: Manage database connections and metadata reflection
- Location: `src/wheretolive/postgis.py`
- Contains: Base database class using singleton metadata pattern, connection management, table metadata reflection
- Depends on: SQLAlchemy, environment variables for credentials
- Used by: All layers requiring database access
- Purpose: Provide shared utilities for logging, external integrations, and spatial data
- Location: `src/wheretolive/logconf.py`, `src/wheretolive/dropbox_uploader.py`, `src/wheretolive/foursquare.py`, `src/wheretolive/isochrone.py`
- Contains: Logging configuration, Dropbox API integration, Foursquare grocery store data fetching, isochrone data loading
- Depends on: External APIs, environment variables, file system
- Used by: All layers
## Data Flow
- **Raw data:** File-based (ZIP + JSON in `/data/mls/` or `/data/rfaster/`)
- **Processed data:** Parquet files (`clean.pq`) for intermediate storage
- **Canonical state:** PostgreSQL database (`mls` and `rfaster` tables)
- **Materialized views:** Pre-filtered candidate lists by user criteria (e.g., `ian_candidates`, `parents_candidates`)
- **Output state:** HTML reports pushed to Dropbox, local copies deleted after upload
## Key Abstractions
- Purpose: Encapsulate validation and field parsing logic for individual listings
- Examples: `src/wheretolive/mls/parse.py` class `_ListingCleaner`
- Pattern: Object-oriented with methods for each field type (`_parse_bedrooms()`, `_parse_date()`, etc.)
- Purpose: Abstraction for querying, filtering, and exporting user-specific results
- Examples: `src/wheretolive/mls/upload_candidates.py:upload_candidate()`, `src/wheretolive/rfaster/upload_candidates.py:upload_candidate()`
- Pattern: Function takes `requestor_name` parameter, queries database view, generates and exports files
- Purpose: Centralize database connection management and metadata reflection
- Examples: `src/wheretolive/postgis.py` classes `_DB`, `PostGIS`
- Pattern: Singleton pattern for metadata, lazy connection creation via property
- Purpose: Centralize path resolution for scraped data directories
- Examples: `src/wheretolive/mls/common.py:_find_scrapes_dir()`, `src/wheretolive/rfaster/common.py:_find_scrapes_dir()`
- Pattern: Functions to locate data by date, returns `Path` objects
## Entry Points
- Location: `scripts/daily_mls.py`
- Triggers: Scheduled by external cron/orchestration, manually invoked during development
- Responsibilities: Orchestrate scrape → parse → ingest → export pipeline for MLS data
- Calls: `scrape_all()`, `parse_scrapes()`, `update_mls_postgis()`, `upload_candidate()` for each user
- Location: `scripts/daily_rfaster.py`
- Triggers: Scheduled by external cron/orchestration, manually invoked during development
- Responsibilities: Orchestrate scrape → parse → ingest → export pipeline for Rentfaster data
- Calls: Same pattern as MLS runner but for Rentfaster modules
- Location: `src/wheretolive/isochrone.py` (can be run as `__main__`)
- Triggers: One-time setup, ad-hoc data updates
- Responsibilities: Create/refresh isochrone tables and insert travel time geometry data
- Location: `src/wheretolive/foursquare.py` (can be run as `__main__`)
- Triggers: One-time data collection, periodic refresh
- Responsibilities: Query Foursquare API for grocery store locations across Calgary grid, export to CSV
- Location: `scripts/postgis/setup_db.py`
- Triggers: Initial database setup
- Responsibilities: Execute SQL scripts to create tables and views
## Error Handling
- Validation errors during parsing log warnings but skip invalid records (graceful degradation)
- Database connection errors raise exceptions (fail-fast)
- Dropbox authentication and upload errors raise `RuntimeError` with descriptive messages
- Missing files or directories raise `FileNotFoundError`
## Cross-Cutting Concerns
- `POSTGIS_PASS`: Database password
- `FOURSQUARE_KEY`: Foursquare API token
- `DROPBOX_KEY`: Dropbox access token
- Loaded via `python-dotenv` from `.env` file
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, or `.github/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
