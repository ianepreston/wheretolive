# Coding Conventions

**Analysis Date:** 2026-04-14

## Naming Patterns

**Files:**
- Module files use lowercase with underscores: `postgis.py`, `logconf.py`, `dropbox_uploader.py`
- Modules have consistent names across similar functionality: `mls/scrape.py`, `mls/parse.py`, `mls/ingest.py`, `rfaster/scrape.py`, `rfaster/parse.py`, `rfaster/ingest.py`
- Test files use `test_` prefix: `test_main.py`, `test_mls.py`, `test_rfaster.py`

**Functions:**
- Public functions use snake_case: `get_logger()`, `dropbox_save()`, `foursquare_groceries()`, `get_mls_scrape_df()`, `update_mls_postgis()`
- Private/internal functions (not exported) use leading underscore: `_find_base_dir()`, `_find_scrapes_dir()`, `_mls_cookie()`, `_mls_scrape_page()`, `_parse_bedrooms()`, `_console_logger()`, `_latlong_grid()`, `_get_fs_path()`
- Methods follow same convention: `_is_vacant_land()`, `_parse_date()`, `_parse_listing()`

**Variables:**
- Local variables use snake_case with descriptive names: `all_listings`, `raw_bedrooms`, `raw_sq_feet`, `clean_timestamp`, `cleaned_listings`, `listings_df`, `numeric_cols`, `datetime_cols`, `scrape_days`, `base_dir`, `out_dir`, `out_file`
- Abbreviated names used for specific concepts: `db` for database connections, `df` for pandas DataFrames, `conn` for connections, `f` for file handles, `z` for zipfiles
- Short loop variables: `ns`, `ew`, `n`, `s`, `e`, `w` (geographic coordinates), single letters acceptable in limited scope
- Constants using environment variables: `PG_PASSWORD`, `FS_KEY`, `TOKEN` (all caps with underscores)

**Types:**
- Classes use PascalCase: `_DB`, `_ListingCleaner`, `PostGIS`
- Private classes use leading underscore: `_DB` (singleton base), `_ListingCleaner`

## Code Style

**Formatting:**
- Black code formatter configured via pre-commit hooks
- Max line length: 88 characters (standard Black default)
- Line length violations ignored: E501 (line too long)

**Linting:**
- Flake8 is primary linter with strict configuration
- Selection: B,B9,BLK,C,E,F,I,S,W,D (bugbear, black, docstring, error, warning, etc)
- Docstring convention: NumPy style
- Max complexity: 10 (cyclomatic complexity)
- Per-file ignores:
  - `tests/*`: S101 (assert used)
  - `__init__.py`: F401 (unused imports - allow for public re-exports)

**Tools Applied via Pre-commit:**
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

**Order:**
1. **Future imports** (if any)
2. **Standard library** - Standard Python library imports
3. **Third party** - External packages (click, pydantic, requests, pandas, etc.)
4. **Local imports** - Project code (from wheretolive imports)

**Examples from codebase:**
- `src/wheretolive/foursquare.py`:
  ```python
  import os
  from pathlib import Path
  from typing import Dict
  from typing import List

  import numpy as np
  import pandas as pd
  import requests
  from dotenv import load_dotenv

  from wheretolive.logconf import get_logger
  ```

- `src/wheretolive/mls/parse.py`:
  ```python
  import datetime as dt
  import json
  import re
  import zipfile
  from pathlib import Path
  from typing import List, Optional, Tuple

  import pandas as pd
  from wheretolive.logconf import get_logger
  from wheretolive.mls.common import _find_base_dir, _find_scrapes_dir
  ```

**Path Aliases:**
- No path aliases configured; absolute imports from wheretolive root
- Relative imports not used

**Import Style:**
- Multiple imports from typing separated on own lines when using single items: `from typing import Dict` on one line, `from typing import List` on next
- When multiple items imported together: `from typing import List, Optional, Tuple`

## Error Handling

**Patterns:**
- **Specific exception types** - Catch specific exceptions, not generic Exception: `dropbox.exceptions.AuthError`, `dropbox.exceptions.ApiError`, `FileNotFoundError`, `ValueError`
- **Exception chaining** - Use `from err` to preserve error context when re-raising: `raise RuntimeError(...) from err`
- **Try-finally** - Use context managers (`with` statements) for resource management
- **Detailed error messages** - Error messages include context: "Can't find isochrone file at {iso_file}", "Couldn't load Dropbox token from environment variable"

**Example from `src/wheretolive/dropbox_uploader.py`:**
```python
TOKEN = getenv("DROPBOX_KEY")
if len(TOKEN) == 0:
    raise ValueError("Couldn't load Dropbox token from environment variable.")

try:
    dbx.users_get_current_account()
except dropbox.exceptions.AuthError as err:
    raise RuntimeError("Invalid access token; try re-generating.") from err

try:
    dbx.files_upload(...)
except dropbox.exceptions.ApiError as err:
    if err.error.is_path() and err.error.get_path().reason.is_insufficient_space():
        raise RuntimeError("ERROR: Cannot back up; insufficient space.") from err
    elif err.user_message_text:
        raise RuntimeError(err.user_message_text) from err
    else:
        raise RuntimeError(err) from err
```

## Logging

**Framework:** Standard library `logging` module

**Setup:**
- Centralized configuration in `src/wheretolive/logconf.py`
- Exported via `get_logger(name: str) -> logging.Logger`
- File logging to: `data/logs/{YYYY-MM-DD}_logs.txt`
- Console logging with custom formatter

**Patterns:**
- Get logger at module level: `logger = get_logger(__name__)`
- Log levels used: `INFO`, `DEBUG`, `WARNING`
- F-string formatting for messages: `logger.info(f"Attempting to parse {self._id}")`
- Common messages:
  - Processing starts: `logger.info(f"Attempting to parse {self._id}")`
  - Skipping logic: `logger.debug(f"{self._id} is vacant, skipping")`
  - Warnings: `logger.warning(f"No + separator for {self._id}...")`
  - Data operations: `logger.info(f"Inserted {len(mls_df):,.0f} records into MLS table")`

**Example from `src/wheretolive/mls/parse.py`:**
```python
logger = get_logger(__name__)

def parse(self):
    logger.info(f"Attempting to parse {self._id}")
    if self._is_vacant_land():
        logger.debug(f"{self._id} is vacant, skipping")
        return None
```

## Comments

**When to Comment:**
- Complex business logic or non-obvious algorithm choices
- SQL queries and database operations: explains the operation
- API parameters: explains what each parameter controls
- Known limitations or workarounds: marks as technical debt

**Examples from codebase:**
- `src/wheretolive/mls/scrape.py`: Complex cookie construction with comment about why it exists: `# Construct a cookie to make scraping less flaky`
- `src/wheretolive/mls/parse.py`: Data conversion logic commented: `# Want to be able to parse this as None when converting to integer`
- `src/wheretolive/isochrone.py`: SQL syntax example in comment: `# (ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}')) As wkt;`

**Docstring Style:**
- NumPy style docstrings required (enforced by flake8-docstrings)
- All public functions have docstrings
- Format includes:
  - One-line summary
  - Parameters section (if applicable)
  - Returns section (if applicable)
  - Notes section (if applicable)

**Example from `src/wheretolive/postgis.py`:**
```python
def reflect_full_db(self) -> sqlalchemy.MetaData:
    """Load metadata about the entire database into the object.

    Note that this can take a very long time for big databases.
    For example, for the data warehouse, expect to wait several minutes.
    Some databases, such as PeopleSoft have tens of thousands of tables, and
    using this function against them will take hours and consume significant amounts
    of memory. In most situations you will not have to use this method, and can
    simply use the ``get_table`` method to return metadata for a specific table.

    Returns
    -------
    sqlalchemy.schema.MetaData
        A metadata object with complete information of the database
    """
```

**Example from `src/wheretolive/mls/scrape.py`:**
```python
def _mls_scrape_page(
    max_results: int = 100,
    price_min: int = 0,
    price_max: int = 10_000_000,
    property_type: int = 1,
    language: str = "English",
    max_attempts_page: int = 10,
) -> List:
    """Scrape from MLS.

    Parameters
    ----------
    max_results: int, default 10
        Maximum results to return, tops out at 500
    price_min: int,default 0
        Lowest price to search
    price_max: int, default $10M
        Highest price to search
    property_type: int, default 1
        0: No preference
        1: Residential
        2: Recreational
        ...
    """
```

## Type Hints

**Usage:**
- Type hints on all function parameters (mypy strict mode enforced)
- Type hints on all return types (including `-> None`)
- Full type annotations for complex types: `List[Path]`, `Dict[str, str]`, `Optional[dt.datetime]`, `Tuple[int, int]`

**Patterns:**
- Optional parameters: `Optional[str]`, `Optional[dt.datetime]`
- Collections: `List[Dict[str, str]]`, `List[Path]`
- Union types via Optional or union syntax
- Datetime imports as: `import datetime as dt` then use `dt.date`, `dt.datetime`, `dt.date.today()`

**MyPy Configuration:**
- Strict mode enabled
- Settings in `pyproject.toml`:
  ```toml
  [tool.mypy]
  strict = true
  pretty = true
  show_column_numbers = true
  show_error_codes = true
  show_error_context = true
  ```

**Example from `src/wheretolive/mls/parse.py`:**
```python
def _parse_bedrooms(self, bed_key: str) -> Tuple[int, int]:
    """Parse above and below grade bedrooms.
    
    Parameters
    ----------
    bed_key: the raw bedrooms key from the listing
    
    Returns
    -------
    Tuple[int, int]:
        Number of above and below grade bedrooms
    """

def _parse_date(self, date_key: Optional[str]) -> Optional[dt.datetime]:
    """Read a text date into a datetime.
    
    Parameters
    ----------
    date_key: str
        The raw date
    
    Returns
    -------
    dt.datetime
        Parsed date
    """
```

## Function Design

**Size:** Not explicitly limited but typically 20-50 lines per function

**Parameters:**
- Functions use sensible defaults: `date: dt.date = None` (then use `dt.date.today()` if None)
- Multiple parameters with defaults use keyword arguments
- Max 10 parameters typical; complex operations split into helper functions

**Return Values:**
- Always explicit return type hints
- `-> None` for side-effect functions (logging, database updates)
- Return values via assignment: `self._cleaned = {dict}` then accessed via property

**Class Methods:**
- All methods are instance methods (no class methods or static methods observed)
- Private methods prefixed with underscore
- Properties via `@property` decorator: `connection`, `metadata`

**Example from `src/wheretolive/postgis.py`:**
```python
def get_table(self, table_name: str) -> sqlalchemy.Table:
    """Get a table object from the database.

    Parameters
    ----------
    table_name: str
        The name of the table to retrieve

    Returns
    -------
    sqlalchemy.Table:
        The associated table object
    """
    return sqlalchemy.Table(
        table_name, self.metadata, autoload_with=self.connection
    )
```

## Module Design

**Exports:**
- Modules expose public functions and classes without prefix
- Private functions/classes use leading underscore, not exported
- `__init__.py` files document module purpose in docstring

**Barrel Files:**
- `__init__.py` files are minimal (one-line module docstring only)
- No public re-exports in `__init__.py` - consumers import directly from submodules

**Example `src/wheretolive/mls/__init__.py`:**
```python
"""Functionality relating to MLS listings."""
```

**Pattern for module structure:**
- Common utilities in `common.py`: `_find_base_dir()`, `_find_scrapes_dir()`
- Scraping in `scrape.py`: Web requests, data fetching
- Parsing in `parse.py`: Data cleaning, validation, transformation
- Ingestion in `ingest.py`: Database loading, updates

---

*Convention analysis: 2026-04-14*
