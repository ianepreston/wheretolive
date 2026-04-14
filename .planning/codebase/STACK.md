# Technology Stack

**Analysis Date:** 2026-04-14

## Languages

**Primary:**
- Python 3.7+ - All application code, scripts, and CLI tools

## Runtime

**Environment:**
- Python 3.7+ (3.9 recommended in noxfile)
- Supported versions: 3.7, 3.8, 3.9 (per pyproject.toml)

**Package Manager:**
- Poetry
- Lockfile: `poetry.lock` (present and maintained)

## Frameworks

**Core:**
- Click 8.0.1+ - CLI command framework (`src/wheretolive/__main__.py`)
- Pydantic 1.8.2+ - Data validation and modeling
- SQLAlchemy 1.4.27+ - ORM and database abstraction (`src/wheretolive/postgis.py`)

**Data Processing:**
- Pandas 1.3.3+ - DataFrames and CSV/Parquet operations (`src/wheretolive/mls/ingest.py`, `src/wheretolive/mls/parse.py`)
- FastParquet 0.7.1+ - Parquet file serialization (used with Pandas)
- NumPy (dependency via Pandas) - Numerical grid operations (`src/wheretolive/foursquare.py`)

**Testing:**
- Pytest 6.2.4+ - Test runner and fixtures
- Coverage 6.0+ with TOML support - Code coverage reporting

**Code Quality:**
- MyPy 0.910+ - Static type checking (strict mode enabled)
- Flake8 3.9.2+ - Style linting
- Black 20.8b1+ - Code formatting
- Pre-commit 2.13.0+ - Git hook framework
- Safety 1.10.3+ - Security vulnerability scanning

**Documentation:**
- Sphinx 4.1.1+ - Documentation generation
- Sphinx-autobuild 2021.3.14+ - Live documentation server
- Sphinx-rtd-theme 1.0.0+ - ReadTheDocs theme
- Sphinx-click 3.0.1+ - CLI documentation from Click
- xdoctest 0.15.10+ - Run doctest examples

**Type Checking Helpers:**
- TypeGuard 2.12.1+ - Runtime type guards for mypy

**Additional Dev Tools:**
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

**Critical:**
- SQLAlchemy 1.4.27+ - Database ORM for PostGIS interactions
- Pandas 1.3.3+ - Data frame manipulation for ingestion pipelines
- requests 2.26.0 - HTTP client for external APIs (Foursquare, Azure Maps)
- python-dotenv 0.19.1+ - Environment variable loading from .env files

**Infrastructure:**
- psycopg2-binary 2.9.2+ - PostgreSQL/PostGIS database driver
- dropbox 11.25.0+ - Dropbox SDK for file backup and storage
- geocoder 1.38.1+ - Geocoding library (for location parsing)
- pytz (dependency via scripts) - Timezone handling for isochrone calculations

## Configuration

**Environment:**
- Loaded via `python-dotenv` (see `load_dotenv()` calls throughout codebase)
- Template: `env.j2` - Jinja2 template for environment file generation

**Build:**
- `pyproject.toml` - Poetry configuration with all dependencies, metadata, and tool config
- `noxfile.py` - Nox automation for testing, linting, type checking, and docs
- `setup.yml` - Ansible playbook for infrastructure setup
- `.pre-commit-config.yaml` - Git hook pipeline (black, flake8, reorder-python-imports, prettier)
- `.flake8` - Flake8 linting configuration (Google import style, max complexity 10)
- `.darglint` - Darglint docstring checking
- `.readthedocs.yml` - ReadTheDocs documentation build config
- `codecov.yml` - Code coverage service configuration

**Code Coverage:**
- Minimum threshold: 90% fail_under (set in pyproject.toml)
- Reporting: Combined coverage reports via `coverage combine`

**Type Checking:**
- MyPy strict mode enabled with full type checking
- All source files type-checked via nox session

## Platform Requirements

**Development:**
- Python 3.7.1+ (3.9 recommended)
- Poetry for dependency management
- Nox for task automation
- Git for version control
- Pre-commit hooks enabled

**Runtime:**
- PostgreSQL/PostGIS database server (connection string in `postgis.py`)
- API keys for external services (via environment variables)
- Dropbox API access (for file uploads)

**Production:**
- PostgreSQL with PostGIS extension
- Environment variables configured for API keys and database credentials
- Python 3.7+ runtime environment
- Network access to external APIs (Foursquare, Azure Maps)

---

*Stack analysis: 2026-04-14*
