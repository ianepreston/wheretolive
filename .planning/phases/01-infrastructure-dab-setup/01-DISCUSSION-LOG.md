# Phase 1: Infrastructure & DAB Setup - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-14
**Phase:** 01-infrastructure-dab-setup
**Areas discussed:** Table naming convention, DAB project layout, Placeholder scope, Existing code coexistence

---

## Table naming convention

| Option | Description | Selected |
|--------|-------------|----------|
| Layer prefix (Recommended) | bronze_mls_raw, silver_mls_listings, gold_all_listings — clear at a glance which layer a table belongs to, sorts nicely in UC browser | ✓ |
| Source prefix | mls_raw, mls_cleaned, mls_wide — groups by data source instead of layer; layer is implicit from the suffix | |
| You decide | Let Claude pick the best convention during planning based on Databricks community patterns | |

**User's choice:** Layer prefix (Recommended)
**Notes:** No additional discussion needed. Straightforward choice.

---

## DAB project layout

| Option | Description | Selected |
|--------|-------------|----------|
| Repo root (Recommended) | databricks.yml at repo root, notebooks/ and resources/ as top-level dirs alongside existing src/. The repo becomes the DAB project. | ✓ |
| Subdirectory | New databricks/ subdirectory with its own databricks.yml. Keeps old and new code clearly separated. | |
| You decide | Let Claude pick based on DAB best practices during planning | |

**User's choice:** Repo root (Recommended)
**Notes:** User reviewed the directory tree preview showing the layout.

---

## Placeholder scope

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal stubs (Recommended) | databricks.yml with resource entries referencing notebook paths. Notebooks exist but are essentially empty. Proves bundle deploy works. | ✓ |
| Working skeleton | Notebooks contain basic structure that actually runs. Proves compute and schema are wired up end-to-end. | |
| Deploy-only | Just databricks.yml and resources/ YAML. No notebook files at all. | |

**User's choice:** Minimal stubs (Recommended)
**Notes:** User reviewed the code preview showing placeholder notebook content and resource YAML.

---

## Existing code coexistence

| Option | Description | Selected |
|--------|-------------|----------|
| Leave untouched (Recommended) | Don't move, archive, or delete anything. Build new DAB structure alongside. | |
| Archive to legacy/ | Move src/wheretolive/ to legacy/wheretolive/ to signal deprecation. Keeps repo root cleaner. | ✓ |
| Delete old code | Remove src/, scripts/, tests/ and old config. Commit to the new world entirely. | |

**User's choice:** Archive to legacy/
**Notes:** User chose to archive rather than leave untouched, preferring a cleaner repo root for the DAB project.

---

## Claude's Discretion

- Exact databricks.yml configuration values
- resources/ YAML file structure
- Schema creation approach (notebook vs DAB resource)
- Git ignore patterns for DAB artifacts

## Deferred Ideas

None — discussion stayed within phase scope.
