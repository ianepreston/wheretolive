---
phase: 01-infrastructure-dab-setup
reviewed: 2026-04-14T00:00:00Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - .gitignore
  - databricks.yml
  - notebooks/ingestion/mls_scrape.py
  - notebooks/ingestion/rfaster_scrape.py
  - notebooks/pipeline/medallion.py
  - notebooks/setup/verify_schema.py
  - resources/job.yml
  - resources/pipeline.yml
  - resources/schema.yml
findings:
  critical: 0
  warning: 1
  info: 1
  total: 2
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-04-14
**Depth:** standard
**Files Reviewed:** 9
**Status:** issues_found

## Summary

This review covers the initial Databricks Asset Bundle (DAB) infrastructure setup: bundle configuration, Unity Catalog schema definition, placeholder notebooks for ingestion and pipeline stages, and Databricks Workflow/Pipeline resource definitions.

The codebase is minimal and well-structured for a Phase 1 scaffold. No secrets are exposed -- authentication is correctly delegated to a local Databricks CLI profile. The `.gitignore` properly excludes `.env`, `data/`, and `.databricks/` directories. The DAB variable system is used effectively in resource definitions for catalog and schema names.

Two issues were found: one warning about hardcoded values in the verify_schema notebook that will drift from the DAB-managed variables, and one informational note about the f-string SQL pattern used there.

## Warnings

### WR-01: Hardcoded catalog/schema values duplicate DAB variables and will drift

**File:** `notebooks/setup/verify_schema.py:6-7`
**Issue:** The `catalog` and `schema` values are hardcoded as string literals (`"workspace"` and `"wheretolive"`), duplicating the defaults defined in `databricks.yml` (lines 8-13) via the `${var.catalog}` and `${var.schema}` variables. If the DAB variable defaults are ever changed (e.g., for a production target), this notebook will silently use stale values. The resource definitions in `resources/schema.yml` and `resources/pipeline.yml` correctly use `${var.catalog}` and `${var.schema}`, so this notebook is the only place with hardcoded duplicates.
**Fix:** Use `dbutils.widgets` to accept the values as notebook parameters (which DAB can pass via task configuration), or read them from a shared config. For example:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Schema Setup
# MAGIC Utility notebook to verify the Unity Catalog schema exists and is accessible.

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "wheretolive", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"DESCRIBE SCHEMA {catalog}.{schema}").show()
print(f"Schema {catalog}.{schema} is accessible.")
```

This allows the job definition in `resources/job.yml` to pass `${var.catalog}` and `${var.schema}` as parameters when the notebook is invoked as a task.

## Info

### IN-01: F-string SQL construction -- safe now but fragile if inputs become dynamic

**File:** `notebooks/setup/verify_schema.py:9`
**Issue:** `spark.sql(f"DESCRIBE SCHEMA {catalog}.{schema}")` uses string interpolation to build SQL. Currently both values are hardcoded literals (lines 6-7), so there is no injection risk. However, if WR-01 is addressed by accepting these as widget parameters or external input, this pattern would become a SQL injection vector. Spark SQL DDL statements like `DESCRIBE SCHEMA` do not support parameterized queries, so the mitigation is input validation rather than parameterization.
**Fix:** If the values are ever sourced from external input, add validation to ensure they contain only valid identifier characters:

```python
import re

def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

catalog = _validate_identifier(dbutils.widgets.get("catalog"))
schema = _validate_identifier(dbutils.widgets.get("schema"))
```

---

_Reviewed: 2026-04-14_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
