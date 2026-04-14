# Phase 2: Listing Ingestion - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-14
**Phase:** 02-listing-ingestion
**Areas discussed:** IP blocking fallback, Raw data format, Idempotency strategy, Volume path layout

---

## IP Blocking Fallback

| Option | Description | Selected |
|--------|-------------|----------|
| Local scrape + upload | Run MLS scraper locally, upload JSON to Volume manually or via script | |
| Proxy service | Route MLS requests through residential proxy from Databricks notebook | |
| Sample data only | Use static MLS data snapshot, defer solving IP blocking | |
| Try it first | Attempt MLS scraping from Databricks as-is, only design fallback if it actually fails | ✓ |

**User's choice:** Try it first
**Notes:** User prefers validate-first approach. If blocking is confirmed, contingency is "Local scrape + upload" (selected in follow-up question).

### Follow-up: Contingency if blocked

| Option | Description | Selected |
|--------|-------------|----------|
| Local scrape + upload | Fall back to running MLS scraper from local machine, upload JSON to Volume | ✓ |
| Sample data snapshot | Bundle representative MLS JSON snapshot in repo | |
| You decide | Claude picks simplest fallback | |

**User's choice:** Local scrape + upload

---

## Raw Data Format

| Option | Description | Selected |
|--------|-------------|----------|
| Plain JSON files | One .json per batch, Auto Loader reads natively | |
| Gzip-compressed JSON | One .json.gz per batch, Auto Loader handles gzip natively, saves storage | ✓ |
| Single JSON per source | Concatenate all batches into one large JSON per source per day | |
| You decide | Claude picks best format for SDP Auto Loader | |

**User's choice:** Gzip-compressed JSON

### Follow-up: File granularity

| Option | Description | Selected |
|--------|-------------|----------|
| One file per batch | MLS: one .json.gz per price bracket. Rentfaster: one .json.gz per page | ✓ |
| One file per source/day | Combine all listings into one .json.gz per day per source | |
| You decide | Claude picks based on Auto Loader patterns | |

**User's choice:** One file per batch

---

## Idempotency Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Delete then write | Delete entire day folder at start, write fresh. Simple, guarantees clean state | ✓ |
| Overwrite in place | Deterministic filenames overwrite same files. Stale files could linger | |
| Write to temp, then swap | Atomic rename, safest but Volume rename semantics may vary | |
| You decide | Claude picks simplest approach satisfying success criterion | |

**User's choice:** Delete then write

---

## Volume Path Layout

| Option | Description | Selected |
|--------|-------------|----------|
| Single volume, source/date folders | /Volumes/workspace/wheretolive/raw_listings/mls/2026-04-14/*.json.gz | ✓ |
| Separate volumes per source | /Volumes/workspace/wheretolive/mls_raw/ and rfaster_raw/ | |
| You decide | Claude picks layout for SDP Auto Loader + simple DAB config | |

**User's choice:** Single volume, source/date folders

### Follow-up: Volume name

| Option | Description | Selected |
|--------|-------------|----------|
| raw_listings | Matches content purpose | |
| raw_data | More generic | |
| You decide | Claude picks sensible name | |

**User's choice:** "ingest" (Other — user's established convention from other projects)
**Notes:** User said "ingest, that's my convention for landing data in other projects"

---

## Claude's Discretion

- Exact notebook implementation details (imports, error handling, retry logic)
- How to adapt legacy cookie/header strategy for MLS requests
- Batch file naming convention within day folders
- Whether to add validation step at end of each notebook
- How to structure local-scrape-and-upload fallback script if needed

## Deferred Ideas

None — discussion stayed within phase scope.
