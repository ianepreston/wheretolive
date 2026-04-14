# Phase 1: Infrastructure & DAB Setup - Research

**Researched:** 2026-04-14
**Domain:** Databricks Asset Bundles (DAB) project structure, Unity Catalog schema creation, legacy code archival
**Confidence:** HIGH

## Summary

Phase 1 establishes the foundation for the entire Databricks migration: a Databricks Asset Bundle (DAB) project at the repo root with `databricks.yml`, a Unity Catalog schema under the `workspace` catalog, and placeholder resource definitions for notebooks, pipelines, and workflows. The existing Python package code moves to a `legacy/` directory.

The Databricks CLI (v0.290.1) is already installed with a valid `free-edition` profile pointing to `https://dbc-f4a281a2-e8e6.cloud.databricks.com/`. DAB supports schema, volume, job, pipeline, and app resources natively. The official `bundle-examples` repository from Databricks provides a `pipeline_with_schema` example that is almost exactly the pattern this phase needs: a `databricks.yml` that includes resource YAML files, a schema resource definition, and a pipeline resource referencing that schema.

**Primary recommendation:** Model the DAB project after the Databricks `bundle-examples/pipeline_with_schema` pattern -- root `databricks.yml` with `include: resources/*.yml`, separate YAML files for schema, pipeline, and job resources. Use `mode: development` for the default target to get prefixed resource names and paused schedules during iteration.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Use layer prefix convention for all tables within the single schema: `bronze_mls_raw`, `silver_mls_listings`, `gold_all_listings`, etc.
- **D-02:** Single schema under `workspace` catalog (no multi-schema medallion). Layer is distinguished by table name prefix, not schema name.
- **D-03:** `databricks.yml` lives at the repo root -- the repo IS the DAB project.
- **D-04:** Top-level directories: `notebooks/` (with `ingestion/`, `pipeline/`, `setup/` subdirs), `resources/` (workflow and pipeline YAML), `app/` (for Phase 8).
- **D-05:** New DAB directories sit alongside the existing `src/` (which gets archived -- see D-08).
- **D-06:** Minimal stubs only. Notebook files exist with a comment/title and a `print("placeholder")` line. Resource YAML files reference these notebooks by path.
- **D-07:** The goal is proving `databricks bundle deploy` succeeds and all resource references resolve. No real scraping, transformation, or pipeline logic in Phase 1.
- **D-08:** Move old Python package (`src/wheretolive/`, `scripts/`, `tests/`) to `legacy/` directory. Signals deprecation while preserving reference material for later phases.
- **D-09:** Old config files at repo root (Poetry, nox, pre-commit, flake8, etc.) should also be archived or cleaned up as part of the move.

### Claude's Discretion
- Exact `databricks.yml` configuration values (bundle name, workspace URL, default cluster settings)
- How to structure the `resources/` YAML files (single file vs. split by resource type)
- Whether to include a `setup/init_catalog.py` notebook for schema creation or handle schema setup via DAB resource definitions
- Git ignore patterns for DAB artifacts (`.databricks/`, bundle state files)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INFRA-01 | Unity Catalog schema setup in `workspace` catalog via DAB | DAB `resources.schemas` supports defining schemas with `catalog_name`, `name`, `comment`, and `grants`. Verified from bundle-examples `pipeline_with_schema` and official docs. Schema resource creates the schema on `bundle deploy`. |
| INFRA-02 | DAB project structure for all Databricks resource definitions | DAB supports `jobs`, `pipelines`, `schemas`, `volumes`, and `apps` as resource types. Multi-file config via `include: resources/*.yml` is the standard pattern. Verified from multiple Databricks official examples. |
</phase_requirements>

## Standard Stack

### Core

| Tool | Version | Purpose | Why Standard | Confidence |
|------|---------|---------|--------------|------------|
| Databricks CLI | 0.290.1 (installed) | Bundle deploy, validate, run | The deployment tool for DAB. Already installed and authenticated. | HIGH [VERIFIED: `databricks --version` on local machine] |
| databricks.yml | DAB config format | Define bundle, workspace, targets, includes | Required entry point for all DAB projects. One per repo root. | HIGH [CITED: docs.databricks.com/aws/en/dev-tools/bundles/settings] |
| Unity Catalog schema resource | DAB resource type | Create schema under `workspace` catalog | DAB-native schema creation, deployed alongside other resources. Available since CLI 0.223.0. | HIGH [VERIFIED: bundle-examples/pipeline_with_schema on GitHub] |
| Databricks Runtime | 16.4 LTS | Notebook execution runtime | Current LTS version, Spark 3.5.2. Used in job cluster definitions. | HIGH [CITED: docs.databricks.com/aws/en/release-notes/runtime/16.4lts] |

### Supporting

| Tool | Version | Purpose | When to Use |
|------|---------|---------|-------------|
| `databricks bundle validate` | CLI built-in | Validate YAML config before deploy | Before every deploy to catch config errors locally | 
| `databricks bundle deploy` | CLI built-in | Deploy resources to workspace | To create/update all Databricks resources |
| `databricks bundle destroy` | CLI built-in | Tear down deployed resources | When needing to clean up and redeploy |
| `free-edition` auth profile | ~/.databrickscfg | Authentication to workspace | All CLI operations target this profile |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| DAB schema resource | `notebooks/setup/init_catalog.py` notebook running `CREATE SCHEMA` SQL | Extra manual step; schema not managed by bundle lifecycle; DAB resource is cleaner and declarative |
| Split YAML files (one per resource type) | Single large `databricks.yml` | Single file becomes unwieldy as resources grow; split is the official recommended pattern |
| `pydabs` (Python-based DAB config) | Standard YAML DAB config | Python config adds complexity; YAML is simpler for this project's modest resource count |

**Recommendation on Claude's discretion items:**

1. **Schema creation:** Use DAB `resources.schemas` resource, not a setup notebook. The schema is created and managed by `bundle deploy`, which is cleaner and matches the official `pipeline_with_schema` example pattern. [VERIFIED: github.com/databricks/bundle-examples/pipeline_with_schema]

2. **Resource YAML structure:** Split by resource type: `resources/schema.yml`, `resources/pipeline.yml`, `resources/job.yml`. This matches the official bundle-examples pattern and keeps each file focused. [VERIFIED: bundle-examples uses this pattern]

3. **`databricks.yml` values:**
   - `bundle.name`: `wheretolive` (matches project name)
   - `workspace.host`: from `free-edition` profile (`https://dbc-f4a281a2-e8e6.cloud.databricks.com/`)
   - Target: single `dev` target with `mode: development` and `default: true`
   - Variables: `catalog: workspace`, `schema: wheretolive` (matching the single-schema decision)
   [ASSUMED: bundle name and schema name choices are reasonable conventions]

4. **Git ignore patterns:** Add `.databricks/`, `dist/`, `build/`, `*.egg-info`, `.venv/` following the official default_python template's `.gitignore`. [VERIFIED: bundle-examples/default_python/.gitignore]

## Architecture Patterns

### Recommended Project Structure

Per decisions D-03, D-04, D-05, D-08, D-09:

```
wheretolive/                          # repo root = DAB project root
├── databricks.yml                    # DAB config (D-03)
├── .gitignore                        # Updated with DAB patterns
├── CLAUDE.md                         # Project instructions (keep)
├── .planning/                        # Planning artifacts (keep)
├── notebooks/                        # Databricks notebooks (D-04)
│   ├── ingestion/
│   │   ├── mls_scrape.py            # Placeholder (D-06)
│   │   └── rfaster_scrape.py        # Placeholder (D-06)
│   ├── pipeline/
│   │   └── medallion.py             # Placeholder (D-06)
│   └── setup/
│       └── verify_schema.py         # Optional: verify schema exists
├── resources/                        # DAB resource YAML (D-04)
│   ├── schema.yml                   # Unity Catalog schema definition
│   ├── pipeline.yml                 # Lakeflow pipeline definition (placeholder)
│   └── job.yml                      # Workflow/job definition (placeholder)
├── app/                             # Databricks App (Phase 8) (D-04)
│   └── .gitkeep                     # Empty placeholder directory
├── legacy/                          # Archived old code (D-08, D-09)
│   ├── src/
│   │   └── wheretolive/             # Old Python package
│   ├── scripts/                     # Old orchestration scripts
│   ├── tests/                       # Old test suite
│   ├── docs/                        # Old Sphinx docs
│   ├── notebooks/                   # Old Jupyter notebooks
│   ├── pyproject.toml               # Old Poetry config
│   ├── noxfile.py                   # Old Nox config
│   ├── poetry.lock                  # Old lockfile
│   ├── .pre-commit-config.yaml      # Old pre-commit hooks
│   ├── .flake8                      # Old flake8 config
│   ├── .darglint                    # Old darglint config
│   ├── .readthedocs.yml             # Old RTD config
│   ├── .cookiecutter.json           # Old template config
│   ├── codecov.yml                  # Old coverage config
│   ├── ansible.cfg                  # Old Ansible config
│   ├── setup.yml                    # Old Ansible playbook
│   ├── hosts                        # Old Ansible hosts
│   ├── vars.yml                     # Old Ansible vars
│   ├── env.j2                       # Old env template
│   ├── places.json.j2              # Old places template
│   ├── setup_environment.sh         # Old setup script
│   ├── library/                     # Old Ansible library
│   ├── README.rst                   # Old readme
│   └── LICENSE.rst                  # Keep at root or copy here
│
├── LICENSE.rst                       # Keep at repo root
└── README.rst                        # Update for new project structure
```

### Pattern 1: DAB Multi-File Configuration

**What:** Root `databricks.yml` defines bundle identity, workspace connection, targets, and variables. Resource definitions live in separate YAML files under `resources/` loaded via `include`.

**When to use:** Always for this project. Standard pattern for any non-trivial DAB project.

**Example:**
```yaml
# databricks.yml (root)
# Source: github.com/databricks/bundle-examples/pipeline_with_schema
bundle:
  name: wheretolive

include:
  - resources/*.yml

variables:
  catalog:
    description: The Unity Catalog catalog to use
    default: workspace
  schema:
    description: The schema within the catalog
    default: wheretolive

targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://dbc-f4a281a2-e8e6.cloud.databricks.com/
      profile: free-edition
```

### Pattern 2: Schema Resource Definition

**What:** Define the Unity Catalog schema as a DAB resource, so it is created/managed by `bundle deploy`.

**When to use:** When the schema needs to exist before other resources (pipelines, jobs) can target it.

**Example:**
```yaml
# resources/schema.yml
# Source: github.com/databricks/bundle-examples/pipeline_with_schema/resources/schema.yml
resources:
  schemas:
    wheretolive_schema:
      catalog_name: ${var.catalog}
      name: ${var.schema}
      comment: |
        WhereToLive real estate listing aggregator.
        Single schema using table name prefixes for medallion layers:
        bronze_*, silver_*, gold_*.
```

### Pattern 3: Pipeline Resource Referencing Schema

**What:** Pipeline resource uses variable interpolation to reference the schema created by the schema resource.

**When to use:** When defining Lakeflow Spark Declarative Pipelines that write to the schema.

**Example:**
```yaml
# resources/pipeline.yml
# Source: adapted from bundle-examples/pipeline_with_schema/resources/pipeline.yml
resources:
  pipelines:
    medallion_pipeline:
      name: wheretolive_medallion
      catalog: ${var.catalog}
      target: ${var.schema}
      serverless: true
      libraries:
        - file:
            path: ../notebooks/pipeline/medallion.py
```

### Pattern 4: Job Resource with Notebook Tasks and Pipeline Task

**What:** A job resource defines the workflow DAG with notebook tasks for scraping and a pipeline task for transformations.

**When to use:** For the daily orchestration workflow that chains scraping then pipeline.

**Example:**
```yaml
# resources/job.yml
# Source: adapted from bundle-examples/default_python/resources/sample_job.job.yml
resources:
  jobs:
    daily_pipeline:
      name: wheretolive_daily
      tasks:
        - task_key: mls_scrape
          notebook_task:
            notebook_path: ../notebooks/ingestion/mls_scrape.py
        - task_key: rfaster_scrape
          notebook_task:
            notebook_path: ../notebooks/ingestion/rfaster_scrape.py
        - task_key: run_pipeline
          depends_on:
            - task_key: mls_scrape
            - task_key: rfaster_scrape
          pipeline_task:
            pipeline_id: ${resources.pipelines.medallion_pipeline.id}
```

### Pattern 5: Placeholder Notebooks

**What:** Minimal notebook stubs that prove `bundle deploy` can resolve references. No real logic.

**When to use:** Phase 1 only. Later phases replace with real implementation.

**Example:**
```python
# notebooks/ingestion/mls_scrape.py
# Databricks notebook source
# MAGIC %md
# MAGIC # MLS Scrape - Placeholder
# MAGIC This notebook will contain MLS scraping logic (Phase 2).

print("placeholder - mls_scrape")
```

### Anti-Patterns to Avoid

- **Anti-pattern: Defining schemas via SQL in a setup notebook when DAB resources can do it.** DAB schema resources are declarative and managed by the bundle lifecycle. A setup notebook requires manual execution and is not idempotent by default.
- **Anti-pattern: Putting all resources in `databricks.yml` instead of separate files.** Quickly becomes unreadable. Use `include` with resource files.
- **Anti-pattern: Using `mode: production` during initial development.** Production mode enforces stricter validation (git branch, service principal). Use `mode: development` for Phase 1. [CITED: docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes]
- **Anti-pattern: Not setting `profile` in target workspace config.** Without explicit profile, CLI uses DEFAULT profile which may point to wrong workspace.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Schema creation | SQL notebook that runs `CREATE SCHEMA IF NOT EXISTS` | DAB `resources.schemas` resource | Schema lifecycle managed by bundle; destroyed with `bundle destroy`; no manual execution needed |
| Resource cross-referencing | Hardcoded IDs or names in job/pipeline configs | DAB variable interpolation `${resources.pipelines.X.id}`, `${var.catalog}` | References resolve automatically at deploy time; no ID hunting in workspace |
| Deployment state tracking | Manual tracking of what's deployed | DAB state in `.databricks/` directory | Bundle tracks deployed resources automatically for updates and teardown |
| Legacy code preservation | Manually tracking which files are old vs. new | `legacy/` directory with git move | Git preserves history; clear separation of old and new code |

## Common Pitfalls

### Pitfall 1: Schema Resource Requires Existing Catalog

**What goes wrong:** `bundle deploy` fails because the schema resource references a catalog that doesn't exist or the user lacks permissions.
**Why it happens:** The `workspace` catalog exists on free edition by default, but the DAB resource needs the correct `catalog_name`. If the variable is wrong or the workspace catalog has a different name, deployment fails.
**How to avoid:** Verify the actual catalog name in the workspace before writing the config. Run `databricks catalogs list --profile free-edition` to confirm the catalog name is literally `workspace`.
**Warning signs:** Deploy error mentioning "catalog not found" or "permission denied on catalog".

### Pitfall 2: Development Mode Resource Naming

**What goes wrong:** Resources get prefixed with `[dev username]` in development mode, making them look different from production names. Pipeline targets and job names include the prefix.
**Why it happens:** `mode: development` automatically prefixes all resource display names. This is by design for isolation but can be confusing if you expect exact names.
**How to avoid:** Understand that development mode resources are prefixed. Don't hardcode resource names in notebooks -- use `${resources.X.id}` references. This is not a bug, it's a feature.
**Warning signs:** Looking for `wheretolive_daily` in the workspace but finding `[dev ian.preston] wheretolive_daily` instead.

### Pitfall 3: Notebook Path Resolution

**What goes wrong:** `bundle deploy` succeeds but job runs fail because notebook paths don't resolve. The YAML uses `../notebooks/ingestion/mls_scrape.py` but the actual synced path in the workspace is different.
**Why it happens:** DAB syncs files to a workspace root path. Notebook task paths must be relative to the resource YAML file that defines them, or relative to the bundle root. Getting the path wrong means the job can't find the notebook.
**How to avoid:** Use paths relative to the resource YAML file location (e.g., `../notebooks/ingestion/mls_scrape.py` from `resources/job.yml`). Test with `databricks bundle validate` before deploying. After deploy, verify with `databricks bundle run daily_pipeline` to confirm task execution finds notebooks.
**Warning signs:** Job starts but tasks fail immediately with "Notebook not found" errors.

### Pitfall 4: Forgetting to Update .gitignore for DAB Artifacts

**What goes wrong:** `.databricks/` directory (containing deploy state, lock files) gets committed to git, causing conflicts between developers or stale state on redeploy.
**Why it happens:** The existing `.gitignore` was written for the old Python project and doesn't cover DAB artifacts.
**How to avoid:** Add `.databricks/` to `.gitignore` before the first `bundle deploy`. Also add `dist/`, `build/`, `*.egg-info` following the standard DAB `.gitignore` template.
**Warning signs:** `git status` showing `.databricks/bundle/` files as untracked after deploy.

### Pitfall 5: Free Edition Outbound Internet Restriction

**What goes wrong:** Not a Phase 1 issue directly, but critical context: Free Edition restricts outbound internet to trusted domains only. Web scraping from Databricks notebooks (Phase 2) will likely be blocked.
**Why it happens:** Databricks Free Edition limits outbound network access as an abuse prevention measure.
**How to avoid:** This doesn't block Phase 1 (no external API calls needed). But the planner should note this constraint for Phase 2 planning. The scraping fallback (external execution, upload to Volume) may be needed.
**Warning signs:** HTTP requests to realtor.ca or rentfaster.ca returning connection errors or timeouts from Databricks notebooks.

### Pitfall 6: Files to Archive Overlap with Files to Keep

**What goes wrong:** Moving files to `legacy/` accidentally includes files that should stay at root (like `LICENSE.rst`, `CLAUDE.md`, `.planning/`, `.git/`).
**Why it happens:** Decision D-08/D-09 says to move old code and config files, but not everything at root is "old."
**How to avoid:** Be explicit about what moves and what stays. Keep at root: `databricks.yml`, `CLAUDE.md`, `.planning/`, `.git/`, `.gitignore`, `LICENSE.rst`. Move to `legacy/`: `src/`, `scripts/`, `tests/`, `docs/`, `notebooks/` (the old Jupyter one), `pyproject.toml`, `poetry.lock`, `noxfile.py`, `.pre-commit-config.yaml`, `.flake8`, `.darglint`, `.readthedocs.yml`, `.cookiecutter.json`, `codecov.yml`, `ansible.cfg`, `setup.yml`, `hosts`, `vars.yml`, `env.j2`, `places.json.j2`, `setup_environment.sh`, `library/`.
**Warning signs:** `databricks bundle validate` failing because it can't find files that were accidentally moved, or `.planning/` ending up in legacy.

## Code Examples

### Complete `databricks.yml`

```yaml
# Source: Pattern adapted from github.com/databricks/bundle-examples
bundle:
  name: wheretolive

include:
  - resources/*.yml

variables:
  catalog:
    description: The Unity Catalog catalog to use
    default: workspace
  schema:
    description: The schema for all wheretolive tables
    default: wheretolive

targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://dbc-f4a281a2-e8e6.cloud.databricks.com/
      profile: free-edition
```

### Complete Schema Resource

```yaml
# resources/schema.yml
# Source: adapted from github.com/databricks/bundle-examples/pipeline_with_schema
resources:
  schemas:
    wheretolive_schema:
      catalog_name: ${var.catalog}
      name: ${var.schema}
      comment: |
        WhereToLive real estate listing data.
        Uses table name prefixes for medallion layers:
        bronze_* (raw), silver_* (cleaned), gold_* (presentation-ready).
```

### Complete Pipeline Resource (Placeholder)

```yaml
# resources/pipeline.yml
resources:
  pipelines:
    medallion_pipeline:
      name: wheretolive_medallion
      catalog: ${var.catalog}
      target: ${var.schema}
      serverless: true
      libraries:
        - file:
            path: ../notebooks/pipeline/medallion.py
```

### Complete Job Resource (Placeholder)

```yaml
# resources/job.yml
resources:
  jobs:
    daily_pipeline:
      name: wheretolive_daily
      tasks:
        - task_key: mls_scrape
          notebook_task:
            notebook_path: ../notebooks/ingestion/mls_scrape.py
        - task_key: rfaster_scrape
          notebook_task:
            notebook_path: ../notebooks/ingestion/rfaster_scrape.py
        - task_key: run_pipeline
          depends_on:
            - task_key: mls_scrape
            - task_key: rfaster_scrape
          pipeline_task:
            pipeline_id: ${resources.pipelines.medallion_pipeline.id}
```

### Placeholder Notebook Format

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # [Notebook Title] - Placeholder
# MAGIC Phase 1 stub. Real implementation in Phase [N].

print("placeholder")
```

### Updated .gitignore (DAB additions)

```gitignore
# Databricks Asset Bundle artifacts
.databricks/
dist/
build/
*.egg-info

# Python
__pycache__/
.venv/

# Legacy (these patterns may no longer be needed but harmless to keep)
.mypy_cache/
/.coverage
/.coverage.*
/.nox/
/.python-version
/.pytype/
/docs/_build/

# Secrets and data
.env
data/
vault_password
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| "Databricks Asset Bundles" (DAB) | "Declarative Automation Bundles" (DAB) | 2025-2026 | Name change in docs; CLI commands unchanged (`databricks bundle ...`). Same functionality. [CITED: docs.databricks.com/aws/en/dev-tools/bundles] |
| Delta Live Tables (DLT) | Lakeflow Spark Declarative Pipelines | Mid-2025 GA | API (`import dlt`) unchanged. Pipeline resource type in YAML is still `pipelines`. `/dlt/` REST endpoints maintained for 12 months. [CITED: docs.databricks.com/aws/en/ldp/] |
| DBR 15.4 LTS | DBR 16.4 LTS (Spark 3.5.2) | May 2025 | Current long-term support runtime. Use for job compute. [CITED: docs.databricks.com/aws/en/release-notes/runtime/16.4lts] |
| Manual `.gitignore` | Standard DAB `.gitignore` template | v0.223.0+ | Official template includes `.databricks/`, `dist/`, `build/` patterns. [VERIFIED: bundle-examples/default_python/.gitignore] |

**Deprecated/outdated:**
- "DLT" terminology: Still works in code (`import dlt`) but official docs now use "Lakeflow Spark Declarative Pipelines". Pipeline resources in DAB YAML are still keyed as `pipelines`.
- DBFS file storage: Actively deprecated. Use Unity Catalog Volumes for file storage. Not relevant to Phase 1 but important context.

## Critical Discovery: Free Edition Limitations

During research, the following Free Edition limitations were discovered that affect the overall project (not just Phase 1):

| Limitation | Impact | Phase Affected |
|------------|--------|----------------|
| Outbound internet restricted to trusted domains | Web scraping from Databricks notebooks will likely fail | Phase 2 (scraping) |
| One active pipeline per pipeline type | Only one Lakeflow pipeline allowed | Phase 3-4 (pipeline) |
| One Databricks App per account | Only one app deployment allowed | Phase 8 (app) |
| Max 5 concurrent job tasks per account | Limits parallelism in workflows | Phase 5 (orchestration) |
| One SQL warehouse, max 2X-Small | Dashboard/Genie query capacity limited | Phase 6-7 |
| Lakebase NOT available | App backend must use Delta tables directly, not Lakebase | Phase 8 |
| Apps auto-stop after 24 hours | App is not persistent | Phase 8 |

[VERIFIED: docs.databricks.com/aws/en/getting-started/free-edition-limitations]

**Phase 1 is not blocked by any of these limitations.** Schema creation, bundle deploy, and resource definitions all work on Free Edition. These limitations primarily affect later phases and should be flagged in the project STATE.md.

## Assumptions Log

> List all claims tagged `[ASSUMED]` in this research. The planner and discuss-phase use this
> section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Bundle name `wheretolive` and schema name `wheretolive` are appropriate choices | Architecture Patterns | LOW -- easily changed in databricks.yml before first deploy |
| A2 | The `workspace` catalog exists and is accessible on the free-edition workspace | Architecture Patterns | MEDIUM -- if catalog has different name, schema creation fails. Mitigated by verification step in plan. |
| A3 | `serverless: true` for pipeline resource works on free edition | Code Examples | LOW -- if not, remove the field; pipeline defaults to classic compute |
| A4 | `README.rst` should remain at root and be updated (rather than moved to legacy) | Architecture Patterns | LOW -- cosmetic decision |

## Open Questions

1. **Exact catalog name on the free-edition workspace**
   - What we know: Free Edition workspaces get a `workspace` catalog automatically. Some workspaces get a catalog named after the workspace instead.
   - What's unclear: Whether the catalog is literally named `workspace` or has a different name on this specific workspace.
   - Recommendation: Add a verification task early in the plan that runs `databricks catalogs list --profile free-edition` and confirms the catalog name before writing the schema resource.

2. **Pipeline serverless support on free edition**
   - What we know: Free Edition uses serverless compute exclusively. Lakeflow pipelines support serverless mode.
   - What's unclear: Whether the `serverless: true` setting in the pipeline resource works or is the default on free edition.
   - Recommendation: Include `serverless: true` in the pipeline resource. If deploy fails, remove the field -- serverless may be the only option and thus implicit.

3. **Whether `databricks bundle deploy` creates the schema immediately or lazily**
   - What we know: Schema resources are part of the deploy cycle.
   - What's unclear: Whether the schema is created in the catalog during `bundle deploy` or only when a pipeline or job first references it.
   - Recommendation: After deploy, verify with `databricks schemas list --catalog workspace --profile free-edition` or via a setup notebook.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Databricks CLI | Bundle deploy/validate | Yes | 0.290.1 | -- |
| `free-edition` profile | Workspace authentication | Yes | N/A (valid auth) | -- |
| Databricks workspace | All resources | Yes | Free Edition | -- |
| Git | Code archival (git mv) | Yes | System git | -- |
| Python 3 | CLI tooling | Yes | System python3 | -- |

**Missing dependencies with no fallback:** None.

**Missing dependencies with fallback:** None.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Databricks CLI validation + manual deploy verification |
| Config file | `databricks.yml` (bundle config is the test config) |
| Quick run command | `databricks bundle validate --profile free-edition` |
| Full suite command | `databricks bundle deploy --target dev --profile free-edition` |

### Phase Requirements to Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INFRA-01 | Schema exists in `workspace` catalog | smoke | `databricks schemas list --catalog workspace --profile free-edition` | N/A (CLI check) |
| INFRA-02 | Bundle validates and deploys successfully | smoke | `databricks bundle validate --profile free-edition && databricks bundle deploy --target dev --profile free-edition` | N/A (CLI check) |
| INFRA-02 | Placeholder notebooks referenced correctly | integration | `databricks bundle deploy --target dev --profile free-edition` (deploy success implies refs resolve) | N/A |

### Sampling Rate
- **Per task commit:** `databricks bundle validate --profile free-edition`
- **Per wave merge:** `databricks bundle deploy --target dev --profile free-edition`
- **Phase gate:** Full deploy succeeds; schema visible in workspace; `bundle run daily_pipeline` launches (tasks may fail since notebooks are stubs, but the workflow starts)

### Wave 0 Gaps
- None -- no traditional test framework needed. Validation is done via CLI commands against the workspace.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | Yes | Databricks CLI profile authentication (`databricks-cli` auth type in `~/.databrickscfg`) |
| V3 Session Management | No | Not applicable for infrastructure setup |
| V4 Access Control | Yes | DAB schema `grants` for controlling who can access the schema |
| V5 Input Validation | No | No user input in Phase 1 |
| V6 Cryptography | No | No encryption requirements in Phase 1 |

### Known Threat Patterns for DAB/Infrastructure

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Secrets in YAML config | Information Disclosure | Never put secrets in `databricks.yml`; use Databricks Secrets or CLI profiles |
| Overly permissive schema grants | Elevation of Privilege | Use minimal grants; for personal project, default (owner-only) is sufficient |
| `.databricks/` state committed to git | Information Disclosure | Add `.databricks/` to `.gitignore` before first deploy |

## Sources

### Primary (HIGH confidence)
- [Databricks bundle-examples repository](https://github.com/databricks/bundle-examples) - `pipeline_with_schema`, `default_python`, `lakeflow_pipelines_python` examples fetched and analyzed via GitHub API
- [Databricks DAB settings docs](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) - Configuration reference
- [Databricks DAB resources docs](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) - Resource types and YAML syntax
- [Databricks DAB deployment modes](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) - Development vs production mode behavior
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) - Feature availability on free tier
- Local machine verification: `databricks --version` (v0.290.1), `databricks auth profiles` (free-edition: YES), `~/.databrickscfg` profile config

### Secondary (MEDIUM confidence)
- [Databricks Lakeflow Spark Declarative Pipelines docs](https://docs.databricks.com/aws/en/ldp/) - Current naming and API compatibility
- [DBR 16.4 LTS release notes](https://docs.databricks.com/aws/en/release-notes/runtime/16.4lts) - Current LTS version

### Tertiary (LOW confidence)
- None -- all claims verified via official sources or local tooling.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Verified CLI version, auth profiles, and bundle examples from official Databricks repository
- Architecture: HIGH - Pattern directly from official bundle-examples repo, adapted for project decisions
- Pitfalls: HIGH for Phase 1 pitfalls (verified from docs); MEDIUM for Free Edition limitation impact on later phases

**Research date:** 2026-04-14
**Valid until:** 2026-05-14 (30 days -- DAB is stable; free edition limitations may change)
