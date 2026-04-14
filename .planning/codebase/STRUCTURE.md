# Codebase Structure

**Analysis Date:** 2026-04-14

## Directory Layout

```
wheretolive/
├── docs/                       # Sphinx documentation
│   ├── conf.py
│   ├── index.rst
│   ├── usage.rst
│   └── ...
├── scripts/                    # Runnable data pipeline scripts and SQL
│   ├── postgis/                # SQL scripts and database utilities
│   │   ├── create_mls.sql      # MLS table schema
│   │   ├── create_rfaster.sql  # Rentfaster table schema
│   │   ├── candidate_views.sql # User-specific MLS candidate views
│   │   ├── candidate_views_rfaster.sql  # User-specific Rentfaster views
│   │   ├── setup_db.py         # Initial database setup runner
│   │   ├── mls_commute_syntax.py       # Commute time filtering logic
│   │   ├── rfaster_commute_syntax.py   # Rentfaster commute logic
│   │   ├── floodzone.py        # Flood zone analysis
│   │   ├── grocery_distance.sql         # Grocery proximity calculations
│   │   └── ...
│   ├── daily_mls.py            # Daily MLS workflow entry point
│   ├── daily_mls.sh            # Shell wrapper for MLS runner
│   ├── daily_rfaster.py        # Daily Rentfaster workflow entry point
│   ├── daily_rfaster.sh        # Shell wrapper for Rentfaster runner
│   ├── download_osm_data.py    # OpenStreetMap data downloader
│   ├── download_transit_data.py # Transit network downloader
│   ├── make_isochrone.py       # Isochrone calculation runner
│   └── ...
├── src/wheretolive/            # Main Python package
│   ├── __init__.py             # Package initialization and logging setup
│   ├── logconf.py              # Centralized logging configuration
│   ├── postgis.py              # Database connection and metadata management
│   ├── isochrone.py            # Isochrone table setup and data loading
│   ├── foursquare.py           # Foursquare API integration for grocery stores
│   ├── dropbox_uploader.py     # Dropbox file upload utility
│   ├── setup_isochrone.sql     # SQL for isochrone table creation
│   │
│   ├── mls/                    # MLS (real estate) data pipeline
│   │   ├── __init__.py
│   │   ├── common.py           # Shared helpers (path resolution by date)
│   │   ├── scrape.py           # API scraping and raw data collection
│   │   ├── parse.py            # Data cleaning and validation
│   │   ├── ingest.py           # Database insertion and view refresh
│   │   └── upload_candidates.py # Query and export candidate results
│   │
│   └── rfaster/                # Rentfaster (rental) data pipeline
│       ├── __init__.py
│       ├── common.py           # Shared helpers (path resolution by date)
│       ├── scrape.py           # Rentfaster scraping
│       ├── parse.py            # Data cleaning and validation
│       ├── ingest.py           # Database insertion and view refresh
│       └── upload_candidates.py # Query and export candidate results
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── test_main.py            # CLI tests
│   ├── test_mls.py             # MLS module tests
│   └── test_rfaster.py         # Rentfaster module tests
│
├── notebooks/                  # Jupyter notebooks for exploration
│   └── 01_test_isochrone.ipynb
│
├── pyproject.toml              # Poetry project configuration, dependencies, build settings
├── poetry.lock                 # Locked dependency versions
├── noxfile.py                  # Nox automation (testing, linting, docs)
├── setup.yml                   # Ansible playbook for environment setup
├── setup_environment.sh        # Shell script for initial environment setup
├── README.rst                  # Project documentation
├── LICENSE.rst                 # GPL-3.0 license
├── .gitignore                  # Git exclusions
├── .pre-commit-config.yaml     # Pre-commit hooks for linting/formatting
├── .flake8                     # Flake8 linting config
└── .darglint                   # Darglint docstring linting config
```

## Directory Purposes

**docs/**
- Purpose: Sphinx-based documentation source
- Contains: RST files for API reference, usage guides, contributing guidelines
- Key files: `conf.py` (Sphinx configuration), `index.rst` (documentation home)

**scripts/**
- Purpose: Standalone executable scripts for data pipeline orchestration and database utilities
- Contains: Daily runners, SQL setup scripts, data downloaders
- Key files: `daily_mls.py`, `daily_rfaster.py`, `postgis/setup_db.py`
- Note: Not part of the installed package; run directly via Python or shell wrappers

**scripts/postgis/**
- Purpose: SQL and Python utilities for database setup and configuration
- Contains: Table creation scripts, materialized view definitions, analytical queries
- Key files: `create_mls.sql`, `create_rfaster.sql`, `candidate_views.sql`

**src/wheretolive/**
- Purpose: Main installable Python package
- Contains: Core library code for data processing, database access, external integrations
- Key files: `postgis.py` (DB layer), `logconf.py` (logging), `foursquare.py`, `dropbox_uploader.py`

**src/wheretolive/mls/**
- Purpose: Encapsulates the complete MLS (Multiple Listing Service) data pipeline
- Contains: Scraping, parsing, ingestion, and export logic specific to MLS listings
- Parallel structure to `rfaster/` module

**src/wheretolive/rfaster/**
- Purpose: Encapsulates the complete Rentfaster rental property data pipeline
- Contains: Scraping, parsing, ingestion, and export logic specific to Rentfaster rentals
- Parallel structure to `mls/` module

**tests/**
- Purpose: Test suite for library code
- Contains: Unit tests and integration tests
- Key files: `test_mls.py`, `test_rfaster.py` (module-specific), `test_main.py` (CLI tests)

**notebooks/**
- Purpose: Jupyter notebooks for exploratory analysis and ad-hoc development
- Contains: Interactive notebooks for testing features, analyzing data
- Note: Not part of the main codebase; for development use only

## Key File Locations

**Entry Points:**
- `scripts/daily_mls.py`: Main workflow for daily MLS scrape-parse-ingest-export cycle
- `scripts/daily_rfaster.py`: Main workflow for daily Rentfaster scrape-parse-ingest-export cycle
- `scripts/postgis/setup_db.py`: Initial database schema setup

**Configuration:**
- `pyproject.toml`: Python version, dependencies, tool configurations (Poetry, pytest, mypy, coverage)
- `.pre-commit-config.yaml`: Pre-commit hooks for code quality (black, flake8, mypy)
- `noxfile.py`: Nox session definitions for testing, linting, documentation building

**Core Logic:**
- `src/wheretolive/postgis.py`: Database connection and table metadata management
- `src/wheretolive/mls/scrape.py`: MLS API scraping logic
- `src/wheretolive/mls/parse.py`: MLS data parsing and validation
- `src/wheretolive/mls/ingest.py`: MLS database insertion
- `src/wheretolive/rfaster/scrape.py`: Rentfaster web scraping
- `src/wheretolive/rfaster/parse.py`: Rentfaster data parsing
- `src/wheretolive/rfaster/ingest.py`: Rentfaster database insertion

**Testing:**
- `tests/test_mls.py`: MLS module tests
- `tests/test_rfaster.py`: Rentfaster module tests
- `tests/test_main.py`: CLI entry point tests

## Naming Conventions

**Files:**
- Python modules: `snake_case.py` (e.g., `postgis.py`, `dropbox_uploader.py`)
- SQL scripts: `snake_case.sql` (e.g., `create_mls.sql`, `candidate_views.sql`)
- Test files: `test_*.py` (e.g., `test_mls.py`, `test_rfaster.py`)
- Documentation: `*.rst` (reStructuredText format)

**Directories:**
- Package directories: `snake_case` (e.g., `src/wheretolive/`, `scripts/postgis/`)
- Grouped functionality: Domain-specific subdirectories under `src/wheretolive/` (e.g., `mls/`, `rfaster/`)

**Python Classes:**
- Public classes: `PascalCase` (e.g., `PostGIS`, `_DB`)
- Private classes: Leading underscore + `PascalCase` (e.g., `_ListingCleaner`)

**Functions:**
- Module-level functions: `snake_case`, prefixed with `_` if private/internal (e.g., `_find_scrapes_dir()`)

## Where to Add New Code

**New Feature (Data Source):**
- Create parallel `src/wheretolive/new_source/` directory mirroring `mls/` and `rfaster/` structure
- Implement: `common.py` (path helpers), `scrape.py`, `parse.py`, `ingest.py`, `upload_candidates.py`
- Create corresponding SQL files in `scripts/postgis/create_new_source.sql`, `candidate_views_new_source.sql`
- Add daily runner script: `scripts/daily_new_source.py`

**New Component/Module (Shared Utility):**
- Implementation: `src/wheretolive/new_module.py` (if single file) or `src/wheretolive/new_module/__init__.py` (if package)
- Example pattern: Integrate external service → create dedicated module like `foursquare.py` or `dropbox_uploader.py`

**Utilities/Helpers:**
- Shared helpers: `src/wheretolive/` root level (e.g., `logconf.py`, `postgis.py`)
- Module-specific helpers: `src/wheretolive/{module}/common.py` (e.g., `mls/common.py`)

**Tests:**
- Unit/integration tests: `tests/test_{module}.py` (e.g., `tests/test_mls.py`)
- Follow existing pattern: pytest fixtures, CliRunner for CLI tests

**SQL Scripts:**
- Schema/table creation: `scripts/postgis/create_*.sql`
- Views and materialized views: `scripts/postgis/{feature}_*.sql` (e.g., `candidate_views.sql`)

## Special Directories

**data/** (Not in repo, created at runtime)
- Purpose: Store scraped data, logs, and cache files
- Structure:
  - `data/mls/YYYY-MM-DD/`: Daily MLS scrapes (ZIP files and cleaned Parquet)
  - `data/rfaster/YYYY-MM-DD/`: Daily Rentfaster scrapes
  - `data/logs/YYYY-MM-DD_logs.txt`: Daily log files
  - `data/foursquare/`: Grocery store data
  - `data/isochrone/`: Isochrone JSON files
- Generated: Yes (created by scripts)
- Committed: No (excluded in `.gitignore`)

**.git/**
- Purpose: Version control metadata
- Committed: Yes (standard Git folder)

**node_modules/, venv/, .venv/, __pycache__/**
- Purpose: Dependencies and runtime artifacts
- Generated: Yes
- Committed: No (excluded in `.gitignore`)

## Module Patterns

**Pipeline Modules (mls/, rfaster/):**

Each follows the same five-file pattern:

1. `__init__.py` - Package docstring only
2. `common.py` - Path helpers and date-based directory resolution
3. `scrape.py` - API/web scraping logic with request building
4. `parse.py` - Data cleaning class (`_ListingCleaner`) and validation
5. `ingest.py` - Database operations and materialized view refresh
6. `upload_candidates.py` - Query execution and export to Dropbox

This symmetrical structure enables easy addition of new data sources and test coverage.

---

*Structure analysis: 2026-04-14*
