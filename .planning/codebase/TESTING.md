# Testing Patterns

**Analysis Date:** 2026-04-14

## Test Framework

**Runner:**
- pytest 6.2.4+
- Config: No explicit pytest.ini or setup.cfg - uses pyproject.toml and default discovery

**Assertion Library:**
- Built-in assert statements (pytest's native assertion rewriting)

**Run Commands:**
```bash
nox -s tests                      # Run all tests (via nox session)
coverage run --parallel -m pytest # Run tests with coverage tracking
coverage report                   # View coverage report
coverage html                     # Generate HTML coverage report
```

**Test Execution via Nox:**
- Primary test runner is `nox` (Nox test automation)
- Python 3.9 test environment
- Dependencies installed via Poetry before tests
- Coverage.py collects coverage data in parallel mode

**Coverage Requirements:**
- Minimum coverage: 90% (fail_under in pyproject.toml)
- Missing lines reported: Yes (show_missing = true)
- Branch coverage: Yes (branch = true)
- Coverage scope: wheretolive package only (source = ["wheretolive"])

```toml
[tool.coverage.run]
branch = true
source = ["wheretolive"]

[tool.coverage.report]
show_missing = true
fail_under = 90
```

## Test File Organization

**Location:**
- Tests co-located in separate `tests/` directory (not adjacent to source)
- Pattern: `tests/test_*.py` for test files

**Naming:**
- Test files: `test_main.py`, `test_mls.py`, `test_rfaster.py`
- Test functions: `test_*` prefix: `test_main_succeeds()`, `test_mls_smoke()`, `test_rfaster_smoke()`

**Current Test Files:**
- `tests/test_main.py` - Tests CLI entry point
- `tests/test_mls.py` - Tests MLS scraper integration
- `tests/test_rfaster.py` - Tests rentfaster scraper integration
- `tests/__init__.py` - Package marker with module docstring

**Directory Structure:**
```
tests/
├── __init__.py                # Package marker
├── test_main.py              # CLI tests
├── test_mls.py               # MLS functionality
└── test_rfaster.py           # RentFaster functionality
```

## Test Structure

**Suite Organization:**

```python
# tests/test_main.py
"""Test cases for the __main__ module."""
import pytest
from click.testing import CliRunner

from wheretolive import __main__


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_main_succeeds(runner: CliRunner) -> None:
    """It exits with a status code of zero."""
    result = runner.invoke(__main__.main)
    assert result.exit_code == 0
```

**Patterns:**
- **Setup**: Fixtures defined via `@pytest.fixture` decorator
- **Teardown**: Fixtures handle cleanup via yield/context managers (none currently in use)
- **Assertions**: Direct `assert` statements - no helper assertion libraries
- **Naming**: Descriptive test names explaining what is tested and expected outcome

## Fixtures

**Test Fixtures:**
- Located in test files inline, not in conftest.py
- CliRunner fixture for testing Click CLI applications (`tests/test_main.py`)
- Type-hinted return types

**Example from `tests/test_main.py`:**
```python
@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()
```

**What Gets Tested:**
- Exit codes: `assert result.exit_code == 0`
- CLI output (not shown in current tests)
- Function return values for smoke tests

**What NOT Tested (Gaps):**
- No mocking of external APIs (Foursquare, MLS APIs)
- No database mocking for PostGIS tests
- No fixture-based test data factories
- No parametrized tests

## Test Types

**Unit Tests:**
- Currently absent - tests focus on integration level
- Would test: Individual functions like `_parse_bedrooms()`, `_parse_date()`, date parsing logic
- Approach: Direct function calls with known inputs, assert outputs

**Smoke/Integration Tests:**
- Current focus: High-level functional tests
- Scope: Verify modules can be imported and execute without crashes
- Examples:
  - `test_mls_smoke()`: Calls `get_all_listings()` from mls module
  - `test_rfaster_smoke()`: Calls `get_all_listings()` from rfaster module

**Example from `tests/test_mls.py`:**
```python
"""Test the MLS scraper.

Just a basic skeleton test for now so coverage won't complain.
"""
from wheretolive.mls import get_all_listings


def test_mls_smoke() -> None:
    """Very high level smoke test for MLS."""
    # If nothing crashes it must have worked, right?
    _ = get_all_listings()
```

**E2E Tests:**
- Not implemented
- Would require external API keys and database connections
- Would be slow to execute

**CLI Tests:**
- CLI entry point tested via Click's CliRunner
- Verifies exit code and basic functionality
- Located in `tests/test_main.py`

## Mocking

**Framework:** pytest's built-in mocking (unittest.mock) or pytest-mock (if installed)

**Current State:** No mocking observed in test files

**What Should Be Mocked (Best Practices):**
- External API calls (Foursquare API, MLS API endpoints)
- Database connections (PostGIS)
- File I/O operations
- Dropbox uploads
- Environment variables

**Pattern to Use (if implemented):**
```python
from unittest.mock import patch, MagicMock

@patch('wheretolive.mls.scrape.requests.request')
def test_mls_scrape_with_mock(mock_request):
    """Test MLS scraping with mocked API."""
    mock_request.return_value.json.return_value = {"results": [...]}
    # Test code here
```

## Common Test Patterns

**Assertion Patterns:**
- Exit code assertions: `assert result.exit_code == 0`
- Existence assertions: `assert some_value is not None`
- Type assertions: Would use `isinstance()` if testing types

**Example from `tests/test_main.py`:**
```python
def test_main_succeeds(runner: CliRunner) -> None:
    """It exits with a status code of zero."""
    result = runner.invoke(__main__.main)
    assert result.exit_code == 0
```

**Smoke Testing Pattern (Current Primary Pattern):**
```python
def test_mls_smoke() -> None:
    """Very high level smoke test for MLS."""
    # If nothing crashes it must have worked, right?
    _ = get_all_listings()
```

## Test Dependencies

**In pyproject.toml [tool.poetry.dev-dependencies]:**
- pytest = "^6.2.4"
- coverage = {extras = ["toml"], version = "^6.0"}
- xdoctest = {extras = ["colors"], version = "^0.15.10"} - Doctest runner for examples

**Type Checking in Tests:**
- mypy runs on test code (configured in noxfile.py)
- Tests must type-hint function parameters and return values

## Test Coverage

**Current Coverage:**
- Target: 90% (fail_under = 90)
- Tracking: Coverage.py with branch coverage enabled
- Report mode: show_missing = true (shows uncovered lines)

**Viewing Coverage:**
```bash
nox -s coverage                   # Generate coverage report
coverage html                     # Generate interactive HTML report in htmlcov/
coverage report --fail-under=90   # Check if coverage meets minimum
```

**Coverage Configuration in pyproject.toml:**
```toml
[tool.coverage.paths]
source = ["src", "*/site-packages"]

[tool.coverage.run]
branch = true
source = ["wheretolive"]

[tool.coverage.report]
show_missing = true
fail_under = 90
```

## Doctest Examples

**Framework:** xdoctest (executed via nox session)

**Execution:**
```bash
nox -s xdoctest              # Run all doctests
nox -s xdoctest -- all       # Run all doctests
```

**Purpose:** Examples in NumPy-style docstrings are validated

**Example (if used in codebase):**
```python
def get_logger(name: str) -> logging.Logger:
    """Get a fully configured logger for the project.
    
    Examples
    --------
    >>> logger = get_logger(__name__)
    >>> logger.info("Hello world")
    """
```

Currently docstrings exist but no Examples sections observed in codebase.

## Test Execution via Nox

**Nox Sessions Configured:**
- `pre-commit` - Linting via pre-commit hooks
- `safety` - Security scanning
- `mypy` - Type checking
- `tests` - Run pytest (primary test session)
- `coverage` - Generate coverage reports
- `xdoctest` - Validate docstring examples
- `docs-build` - Build documentation
- `docs` - Build and serve documentation

**Nox Test Session (`tests`):**
```python
@session(python=python_versions)
def tests(session: Session) -> None:
    """Run the test suite."""
    session.install(".")
    session.install("coverage[toml]", "pytest", "pygments")
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage")
```

## Test Gaps and Coverage Issues

**Untested Areas:**
- `src/wheretolive/postgis.py` - Database connection logic (requires database)
- `src/wheretolive/mls/parse.py` - Complex data parsing functions
- `src/wheretolive/mls/scrape.py` - Web scraping logic
- `src/wheretolive/rfaster/` - All rfaster module functions
- `src/wheretolive/dropbox_uploader.py` - File upload logic
- `src/wheretolive/foursquare.py` - API integration
- Error handling paths in most modules

**Risk:** Most critical business logic (data parsing, API integration, database operations) lacks unit test coverage. Tests are smoke tests only - they don't verify correctness, only that code doesn't crash.

**Test Coverage Commands:**
- 90% minimum enforced - test suite must maintain this threshold
- Failures in coverage checks block development (fail_under enforcement)

---

*Testing analysis: 2026-04-14*
