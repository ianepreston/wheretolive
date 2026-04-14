---
phase: 01
slug: infrastructure-dab-setup
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-14
---

# Phase 01 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Databricks CLI validation + manual deploy verification |
| **Config file** | `databricks.yml` (bundle config is the test config) |
| **Quick run command** | `databricks bundle validate --profile free-edition` |
| **Full suite command** | `databricks bundle deploy --target dev --profile free-edition` |
| **Estimated runtime** | ~15 seconds (validate), ~60 seconds (deploy) |

---

## Sampling Rate

- **After every task commit:** Run `databricks bundle validate --profile free-edition`
- **After every plan wave:** Run `databricks bundle deploy --target dev --profile free-edition`
- **Before `/gsd-verify-work`:** Full deploy must succeed; schema visible in workspace
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | INFRA-02 | T-01-03 | `.databricks/` in `.gitignore` | smoke | `grep -q '.databricks/' .gitignore` | N/A | ⬜ pending |
| 01-01-02 | 01 | 1 | INFRA-02 | — | N/A | smoke | `databricks bundle validate --profile free-edition` | N/A | ⬜ pending |
| 01-01-03 | 01 | 1 | INFRA-01 | T-01-01 | No secrets in YAML | smoke | `databricks bundle deploy --target dev --profile free-edition` | N/A | ⬜ pending |
| 01-01-04 | 01 | 1 | INFRA-01 | — | N/A | smoke | `databricks schemas list --catalog workspace --profile free-edition` | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements. No test framework installation needed — validation is done via Databricks CLI commands against the workspace.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Schema visible in Unity Catalog UI | INFRA-01 | UI verification confirms workspace integration | Navigate to workspace > Catalog > workspace > wheretolive schema |
| Placeholder notebooks browsable in workspace | INFRA-02 | Visual confirmation of deployed file structure | Navigate to workspace > Workspace > Users > [user] > .bundle/wheretolive/... |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
