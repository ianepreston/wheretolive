# Codebase Concerns

**Analysis Date:** 2026-04-14

## Tech Debt

**Hardcoded Hostname in Database Connection:**
- Issue: Database connection string hardcoded with specific hostname `mars` instead of configurable environment variable
- Files: `src/wheretolive/postgis.py:90`
- Impact: Code is tightly coupled to specific infrastructure. Cannot deploy to different environments without code changes. Violates 12-factor app principles.
- Fix approach: Move hostname to `POSTGIS_HOST` environment variable. Construct connection string from env vars: `postgresql://postgres:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}`

**Deprecated Logger Methods:**
- Issue: Code uses `logger.warn()` which is deprecated in Python logging. Should use `logger.warning()`.
- Files: `src/wheretolive/mls/parse.py:86`, `src/wheretolive/foursquare.py:68`
- Impact: Code will break in future Python versions. Creates inconsistency with correct usage in other files.
- Fix approach: Replace all `logger.warn()` calls with `logger.warning()` (already correctly used in other places).

**Cookie Management Hardcoded in Scraper:**
- Issue: HTTP cookies are hardcoded directly in source code at `src/wheretolive/mls/scrape.py:20-40`. Cookies expire and will break scraping.
- Files: `src/wheretolive/mls/scrape.py:19-40`
- Impact: When cookies expire, scraping fails until code is manually updated. No automated refresh mechanism. Makes scraper fragile and unmaintainable.
- Fix approach: Implement cookie refresh mechanism. Consider using session management or cookie jar approach. Store cookies in external file or database with expiration tracking.

**Magic Numbers and Constants Scattered Throughout:**
- Issue: Geographic bounds, category IDs, price ranges, and other constants are hardcoded in function bodies
- Files: `src/wheretolive/foursquare.py:26-29`, `src/wheretolive/foursquare.py:60-61`, `src/wheretolive/mls/scrape.py:80-90`
- Impact: Difficult to reuse code for other regions or adjust parameters. Creates coupling between data and logic.
- Fix approach: Centralize configuration in config file or environment. Create `config.py` with geographic bounds, API categories, and other parameters.

## Known Bugs

**Date Parsing Bug in rfaster Scraper:**
- Issue: Availability date parsing assumes dates without year are in the current year or next year, but doesn't handle edge cases around year boundaries correctly
- Files: `src/wheretolive/rfaster/parse.py:57-86`
- Trigger: Processing listings during last week of December. Dates like "January 01" will be set to current year instead of next year.
- Workaround: Code attempts to add 1 year if date is before today, but logic may fail for edge cases.

**Inconsistent Timestamp Formatting:**
- Issue: MLS scraper has complex timestamp parsing with hardcoded epoch offset correction
- Files: `src/wheretolive/mls/parse.py:177-185`
- Symptoms: Comment indicates "Something about the formatting here has the epoch start wrong, this corrects it" - suggests workaround for API inconsistency
- Trigger: Whenever InsertedDateUTC field is parsed from MLS API
- Impact: If API changes timestamp format, parsing will silently fail

**Variable Naming Inconsistency in parse.py:**
- Issue: Unused variable `_` assigned but not documented
- Files: `src/wheretolive/mls/parse.py:205`
- Symptoms: Line `_ = None` serves no purpose and suggests incomplete cleanup
- Impact: Code maintainability issue, unclear intent

## Security Considerations

**Environment Variable Loaded Without Validation:**
- Risk: Code loads `POSTGIS_PASS` without checking if it's None or empty before using in connection string
- Files: `src/wheretolive/postgis.py:9`
- Current mitigation: None - if env var is missing, connection string will have malformed password
- Recommendations: 
  - Add validation: `if not PG_PASSWORD: raise ValueError("POSTGIS_PASS not set")`
  - Consider using python-dotenv with strict validation or pydantic for env var management

**Dropbox API Token Validation Inadequate:**
- Risk: Token length check is too permissive (`len(TOKEN) == 0` only checks empty, not None)
- Files: `src/wheretolive/dropbox_uploader.py:18-20`
- Current mitigation: Partial - raises error for empty string but not for None
- Recommendations: 
  - Change to: `if not TOKEN: raise ValueError(...)`
  - Consider using pydantic.SecretStr for sensitive credentials
  - Add token format validation (should be long base64-like string)

**API Keys Exposed in Log Messages:**
- Risk: Foursquare API key used in Authorization header but could leak in error messages or logging
- Files: `src/wheretolive/foursquare.py:62`
- Current mitigation: None visible
- Recommendations: 
  - Ensure API keys are never logged
  - Consider using requests-oauthlib or similar for safer header management
  - Mask credentials in error messages

**User Names Hardcoded in Scripts:**
- Risk: Dropbox upload candidates use hardcoded user names without access control
- Files: `scripts/daily_mls.py:10-12`
- Current mitigation: None
- Recommendations:
  - Move user list to environment variable or config file
  - Add permission/ownership validation before upload
  - Consider user identification mechanism

## Performance Bottlenecks

**Web Scraping Without Rate Limiting Strategy:**
- Problem: MLS scraper chunks requests by price range but uses fixed 1-second sleep between requests
- Files: `src/wheretolive/mls/scrape.py:126-187`
- Cause: Price range chunking requires multiple requests (could be dozens for large price range). Fixed sleep is arbitrary.
- Impact: Scraping can take hours and may still hit rate limits. No exponential backoff for failures.
- Improvement path:
  - Implement exponential backoff with jitter (5s initial, max 60s)
  - Add configurable delays per endpoint
  - Consider using requests-backoff library
  - Monitor actual 429/503 responses

**Duplicate Dropping Happens After Full DataFrame Creation:**
- Problem: Drop duplicates called after parsing entire result set, not before ingestion
- Files: `src/wheretolive/mls/parse.py:206`
- Cause: Parquet file created with duplicates, then dropped in memory during ingestion
- Impact: Wasted I/O operations, unnecessary memory usage, potential data loss if duplicates are critical
- Improvement path:
  - Drop duplicates during parsing before returning DataFrame
  - Add set-based deduplication during scrape to detect duplicates early
  - Consider unique constraint in database

**Full Database Metadata Reflection on Connection:**
- Problem: `reflect_full_db()` method reflects entire database which is extremely slow for large databases
- Files: `src/wheretolive/postgis.py:34-53`
- Cause: Design requires full metadata reflection; comment warns this can take hours
- Impact: Initialize any connection class triggers slow operation; blocks deployment
- Improvement path:
  - Use lazy loading for metadata
  - Implement table-specific reflection by default
  - Cache metadata with TTL

**DataFrame Conversion to Numeric/Datetime on Every Parse:**
- Problem: Converting columns to numeric/datetime types after already creating DataFrame
- Files: `src/wheretolive/mls/parse.py:207-226`, `src/wheretolive/rfaster/parse.py:248+`
- Cause: Parser builds DataFrame with strings, then converts types
- Impact: Two-pass processing, memory overhead, potential for type conversion errors
- Improvement path:
  - Use dtype specification during DataFrame creation
  - Parse numeric/date values during _ListingCleaner phase

## Fragile Areas

**MLS Data Parser (_ListingCleaner) is Complex and Brittle:**
- Files: `src/wheretolive/mls/parse.py:45-193`
- Why fragile: 
  - Depends on specific JSON structure from API with hardcoded key paths
  - No schema validation - assumes all required keys exist
  - Multiple try/except blocks with silent failures (returns None)
  - Parsing logic for bedrooms, square footage, timestamps use multiple special cases
  - Comment at line 179-182 indicates API format issue wasn't fully understood
- Safe modification: 
  - Add schema validation first (pydantic)
  - Add unit tests for each parsing function
  - Use optional chaining/safe dict access
- Test coverage: Minimal - only smoke test exists (`test_mls_smoke`)

**rfaster Scraper (_ListingCleaner) has Complex Validation Logic:**
- Files: `src/wheretolive/rfaster/parse.py:51-245`
- Why fragile:
  - Complex string parsing with regex and hardcoded patterns
  - Multiple edge case branches for bedrooms, utilities, dates
  - "plus" in sq_feet silently drops data (line 103-104)
  - Validity check uses bitwise AND instead of logical AND (line 176) - may have unexpected behavior
  - Range-based square footage is silently dropped (line 111-112)
- Safe modification:
  - Add comprehensive unit tests for parsing edge cases
  - Document why certain formats are dropped
  - Consider using pydantic validators for cleaner logic
- Test coverage: Minimal - only smoke test

**Hardcoded API Endpoint Changes Without Backward Compatibility:**
- Files: `src/wheretolive/mls/scrape.py:121`
- Why fragile: Recent commit `1f75edf` shows API endpoint switched from api1 to api2 due to 404 errors. No fallback or version detection.
- Safe modification: Add API version detection and fallback mechanism

## Scaling Limits

**Single-Threaded Scraping:**
- Current capacity: Scrapes sequential price chunks, ~1 request per second
- Limit: Calgary MLS catalog requires multiple requests; process takes hours
- Scaling path: 
  - Implement concurrent requests with asyncio or ThreadPoolExecutor
  - Add queue-based scraping with multiple workers
  - Consider background task queue (Celery/RQ)

**Materialized View Refresh Blocks Ingestion:**
- Current capacity: Single refresh per ingestion
- Limit: Comment at `src/wheretolive/mls/ingest.py:34` notes "Try breaking this into chunks"
- Scaling path:
  - Implement concurrent refreshes
  - Add incremental view updates instead of full refresh
  - Monitor refresh duration and add timeouts

## Dependencies at Risk

**Python 3.7 Support Claims Are Outdated:**
- Risk: `pyproject.toml` claims Python 3.7 support but noxfile only tests against 3.9
- Files: `pyproject.toml:22`, `noxfile.py:23`
- Impact: No guarantee code works on Python 3.7. Type hints may be incompatible. If 3.7 is truly needed, testing gap.
- Migration plan: Either drop Python 3.7 from metadata or add to CI testing

**Deprecated Package Versions:**
- Risk: `poetry.lock` is from February 2022 (2+ years old)
- Files: `poetry.lock`
- Impact: Using ancient versions of pandas, SQLAlchemy, requests with known vulnerabilities
- Migration plan: Run `poetry update` and test comprehensively; major version bumps may break API usage

**postgres adapter (psycopg2-binary) Warning:**
- Risk: Using `psycopg2-binary` which is discouraged for production
- Files: `pyproject.toml:30`
- Impact: Binary incompatibilities across platforms, not suitable for containers/deployments
- Migration plan: Switch to `psycopg2` with system libpq library, or use `psycopg3` if supporting newer PostgreSQL

**Old SQLAlchemy 1.4 (Pre-2.0):**
- Risk: SQLAlchemy 1.4 is now legacy; 2.0+ has breaking changes
- Files: `pyproject.toml:31`
- Impact: Code uses old API patterns; migration to 2.0 will require refactoring
- Migration plan: Add SQLAlchemy 2.0 compatibility mode warnings, plan migration

## Missing Critical Features

**No Error Recovery or Retry Logic:**
- Problem: Scraping failures are logged but don't trigger alerts or retries
- Impact: Silent failures - if MLS or RentFaster API is down, no daily data collected
- Solution: Add robust retry logic with exponential backoff, send failure alerts

**No Data Validation After Ingestion:**
- Problem: Data inserted into database without post-insert validation
- Impact: Corrupt or invalid data can persist in database
- Solution: Add data quality checks before and after ingestion

**No Logging Configuration for Production:**
- Problem: `logconf.py` hardcodes file-based logging to `data/logs/` directory
- Impact: In cloud deployments, logs disappear when instance terminates
- Solution: Add environment-based logging config (stdout for containers, structured JSON logging)

**No Monitoring or Alerting:**
- Problem: No metrics collection, error tracking, or alerting system
- Impact: Issues discovered only when users notice missing data
- Solution: Add error tracking (Sentry), metrics (Prometheus/StatsD), and alerting

**No Database Connection Pooling:**
- Problem: Each connection creates new engine via `sqlalchemy.create_engine()`
- Impact: Exhaust database connection limits under load
- Solution: Implement connection pooling with QueuePool

## Test Coverage Gaps

**Smoke Tests Only, No Real Coverage:**
- What's not tested: All parsing logic, scraping, database operations, data validation
- Files: `tests/test_mls.py`, `tests/test_rfaster.py` (11 lines each)
- Risk: Regressions in core scraping/parsing logic go undetected
- Priority: **High** - coverage requirement is 90% but tests are skeleton only

**No Integration Tests:**
- What's not tested: MLS->Parse->Ingest pipeline, RentFaster pipeline, database schema
- Files: None exist
- Risk: Entire data pipeline could be broken without detection
- Priority: **High** - critical user-facing functionality

**No Database Tests:**
- What's not tested: Connection handling, SQL queries, schema validation, data types
- Files: None exist
- Risk: Schema changes break silently, database operations fail at runtime
- Priority: **High**

**No Mocking in Existing Tests:**
- What's not tested: Tests actually call live scrapers/APIs
- Files: `tests/test_mls.py:10-11`, `tests/test_rfaster.py:10-11`
- Risk: Tests require internet, slow to run, brittle if APIs are down
- Priority: **Medium** - refactor to use fixtures/mocks

**No Configuration Tests:**
- What's not tested: Environment variable loading, config validation, error handling for missing secrets
- Files: None exist
- Risk: Deployment failures due to missing/invalid environment configuration
- Priority: **Medium**

---

*Concerns audit: 2026-04-14*
