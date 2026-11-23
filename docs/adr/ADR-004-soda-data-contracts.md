# ADR-004: Write-audit-publish audit with Soda Core v4 data contracts

- Status: Accepted
- Date: 2026-11-23
- Answers: PRD Q3 (Soda Core or GX Core)

## Context
The official-candles gate (PRD §6.2) ran DQ-11 to DQ-14 as hand-written SQL in the
candles job. The PRD asks for a data-quality tool. The audit runs in a serverless
Databricks job (Spark Connect) on the job's Spark session.

Options in November 2026:
- Soda Core v4: YAML data contracts (missing, invalid, duplicate, failed rows, metric
  checks); `soda-sparkdf` attaches to an existing Spark session and is tested on Spark
  Connect. License: Elastic License 2.0 since v4 (source-available, free to use, not OSI
  open source; forbids offering Soda Core as a hosted service). v3 remains Apache 2.0.
- GX Core 1.x: Python expectation classes, Apache 2.0, stewarded by Fivetran since May
  2026. A per-product-day count (DQ-13) needs custom work; Spark Connect support not
  confirmed.

## Decision
- Soda Core v4 (`soda-sparkdf==4.31.0`, pinned) verifies
  `contracts/candles_official_staging.yml` in the job's `audit` task
  (`databricks/jobs/soda_audit.py --enforce`); `publish` runs only if every check passes.
- DQ-13 compares against the expected product-days (products from `config/products.yml`
  times the date range), so a product with no candles at all fails.
- One row per check is appended to `ops.dq_results` (`check_id = "soda: <check name>"`,
  measured value in `observed_value`, Soda diagnostics in `details`).
- The hand-written SQL audit is removed.

## Evidence
- Local Spark 4.3 with Soda 4.31: clean data passes all 11 checks; corrupted data (negative
  volume, high < close, 1,300 candles, a duplicate minute) fails DQ-12, DQ-11, DQ-13 and
  DQ-14; a missing product fails DQ-13.
- Databricks serverless, 2026-11-21: the Soda task ran beside the SQL audit in report-only
  mode; both passed with the same results.

## Consequences
- The checks live in a reviewable contract file instead of Python.
- ELv2 allows this project's use (internal, not a hosted service); recorded here because
  the PRD asks for free tools and ELv2 is free but not open source.
- The Soda version is pinned; upgrades are deliberate.
- The gate blocking real bad data on Databricks is proven by CH-4 (Phase 4 gate).
