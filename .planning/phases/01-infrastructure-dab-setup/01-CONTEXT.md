# Phase 1: Infrastructure & DAB Setup - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning

<domain>
## Phase Boundary

All Databricks resources are defined in a DAB project and a working Unity Catalog schema exists in the `workspace` catalog. This phase delivers the foundation that every subsequent phase builds on: a deployable DAB project structure, a Unity Catalog schema, and placeholder resource definitions for notebooks, pipelines, and workflows.

</domain>

<decisions>
## Implementation Decisions

### Table naming convention
- **D-01:** Use layer prefix convention for all tables within the single schema: `bronze_mls_raw`, `silver_mls_listings`, `gold_all_listings`, etc.
- **D-02:** Single schema under `workspace` catalog (no multi-schema medallion). Layer is distinguished by table name prefix, not schema name.

### DAB project layout
- **D-03:** `databricks.yml` lives at the repo root — the repo IS the DAB project.
- **D-04:** Top-level directories: `notebooks/` (with `ingestion/`, `pipeline/`, `setup/` subdirs), `resources/` (workflow and pipeline YAML), `app/` (for Phase 8).
- **D-05:** New DAB directories sit alongside the existing `src/` (which gets archived — see D-08).

### Placeholder scope
- **D-06:** Minimal stubs only. Notebook files exist with a comment/title and a `print("placeholder")` line. Resource YAML files reference these notebooks by path.
- **D-07:** The goal is proving `databricks bundle deploy` succeeds and all resource references resolve. No real scraping, transformation, or pipeline logic in Phase 1.

### Existing code coexistence
- **D-08:** Move old Python package (`src/wheretolive/`, `scripts/`, `tests/`) to `legacy/` directory. Signals deprecation while preserving reference material for later phases.
- **D-09:** Old config files at repo root (Poetry, nox, pre-commit, flake8, etc.) should also be archived or cleaned up as part of the move.

### Claude's Discretion
- Exact `databricks.yml` configuration values (bundle name, workspace URL, default cluster settings)
- How to structure the `resources/` YAML files (single file vs. split by resource type)
- Whether to include a `setup/init_catalog.py` notebook for schema creation or handle schema setup via DAB resource definitions
- Git ignore patterns for DAB artifacts (`.databricks/`, bundle state files)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project context
- `.planning/PROJECT.md` — Project vision, constraints (workspace catalog, single schema, DAB for all resources)
- `.planning/REQUIREMENTS.md` — INFRA-01 (Unity Catalog schema via DAB), INFRA-02 (DAB project structure); Out of Scope section confirms single schema and workspace catalog constraints
- `.planning/ROADMAP.md` §Phase 1 — Success criteria for this phase

### Research
- `.planning/research/STACK.md` — Recommended stack, DAB configuration patterns, notebook setup patterns
- `.planning/research/ARCHITECTURE.md` — Recommended project structure, component responsibilities, build order
- `.planning/research/PITFALLS.md` — Pitfall 4 (Unity Catalog schema design), Pitfall 1 (file-system patterns to avoid), Pitfall 10 (compute cost awareness)

### Existing code (reference for migration)
- `src/wheretolive/mls/scrape.py` — MLS scraping patterns to adapt in later phases
- `src/wheretolive/rfaster/scrape.py` — Rentfaster scraping patterns to adapt in later phases

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/wheretolive/mls/scrape.py` and `src/wheretolive/rfaster/scrape.py`: Scraping logic (API endpoints, pagination, cookie handling) will be adapted into Databricks notebooks in Phase 2. Not reusable directly in Phase 1, but informs placeholder notebook naming.
- `pyproject.toml`: Contains project metadata that may inform DAB bundle naming.

### Established Patterns
- The existing repo uses Poetry for dependency management and nox for task automation. These are being replaced by DAB and Databricks-native tooling. No patterns carry forward.
- The existing medallion-like pattern (scrape -> parse -> ingest) maps to the bronze/silver/gold layer prefix naming convention chosen.

### Integration Points
- `databricks bundle deploy` is the single deployment command. The DAB project must be configured to target the user's existing Databricks workspace.
- Unity Catalog schema creation is the key integration point — all subsequent phases write tables into this schema.

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches for DAB project structure and Unity Catalog setup.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 01-infrastructure-dab-setup*
*Context gathered: 2026-04-14*
