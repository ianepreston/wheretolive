---
phase: 2
slug: listing-ingestion
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-14
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | In-plan grep + CLI commands (no pytest for notebook code) |
| **Config file** | N/A — notebooks validated by deployment and manual execution |
| **Quick run command** | `grep -c "json.gz" notebooks/ingestion/rfaster_scrape.py` |
| **Full suite command** | `databricks bundle validate --profile free-edition` |
| **Estimated runtime** | ~10 seconds (local grep), ~30 seconds (bundle validate) |

---

## Sampling Rate

- **After every task commit:** Run grep-based verify commands from plan tasks
- **After every plan wave:** Run `databricks bundle validate --profile free-edition`
- **Before `/gsd-verify-work`:** Bundle validate must pass; manual notebook execution confirms end-to-end
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 2-01-01 | 01 | 1 | INGEST-02 | — | N/A | file-check | `grep -c "volume_type" resources/schema.yml` | ✅ | ⬜ pending |
| 2-01-02 | 01 | 1 | INGEST-02 | — | N/A | file-check | `grep -c "json.gz" notebooks/ingestion/rfaster_scrape.py` | ✅ | ⬜ pending |
| 2-02-01 | 02 | 1 | INGEST-01, INGEST-03 | T-02-05 | Incapsula detection logged | file-check | `grep -c "json.gz" notebooks/ingestion/mls_scrape.py` | ✅ | ⬜ pending |
| 2-03-01 | 03 | 2 | INGEST-01, INGEST-02 | — | N/A | cli | `databricks bundle validate --profile free-edition && databricks bundle deploy --profile free-edition` | ✅ | ⬜ pending |
| 2-03-02 | 03 | 2 | INGEST-03 | — | N/A | manual | Manual — run notebooks in Databricks workspace | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Existing infrastructure covers all phase requirements. No Wave 0 test stubs needed — this phase produces Databricks notebooks validated by grep-based file content checks (Plans 01, 02) and deployment CLI commands + manual notebook execution (Plan 03). Pytest is not applicable to notebook code running on Databricks compute.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| MLS scraping from Databricks cloud IPs | INGEST-03 | Requires live Databricks cluster + external MLS API | Run MLS notebook on cluster, observe output: success (JSON files), blocked (Incapsula 403), or API changed |
| Rentfaster scraping produces files | INGEST-02 | Requires live Databricks cluster + external Rentfaster API | Run Rentfaster notebook on cluster, verify .json.gz files in Volume |
| Volume files browsable in Unity Catalog UI | INGEST-01, INGEST-02 | Requires workspace UI interaction | Navigate to Catalog > workspace > schema > Volumes > ingest, verify JSON files visible |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or are formally classified as manual-only
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 not required — grep/CLI verify commands need no test stubs
- [x] No watch-mode flags
- [x] Feedback latency < 30s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
