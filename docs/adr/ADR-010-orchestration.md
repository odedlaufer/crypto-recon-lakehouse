# ADR-010: GitHub Actions orchestrates the daily flow; Databricks does the Spark work

- Status: Accepted
- Date: 2026-11-11

## Context
PRD FR-J5 draws the daily DAG as one Databricks job: fetch candles, load candles (WAP),
gap detector, fetch backfill, gap repair, dbt build, Elementary. Two of those steps call
Coinbase REST and must run outside Databricks, because Free Edition restricts outbound
internet (ADR-001). The chain therefore leaves Databricks and returns at least twice.

## Decision
- `.github/workflows/daily-batch.yml` sequences the day: fetch candles and upload, run the
  Databricks `daily` job and wait, then `dbt build` against the SQL warehouse.
- The Databricks `daily` job holds the Spark work: the candles write-audit-publish job
  (run-job task) and a stream pipeline refresh, in parallel.
- dbt runs on the GitHub runner with the `ci_dev` target (token auth).
- Phase 3 extends the workflow: dbt build (raw status), gap detector, fetch backfill,
  gap repair, dbt build again (repaired status).

## Consequences
- Same steps as FR-J5, sequenced from GitHub instead of a single Databricks DAG.
- Running dbt before and after repair is a plain workflow step, which keeps
  `status_before_repair` honest (FR-R4).
- dbt and Elementary artifacts stay on the runner, where `docs.yml` publishes them.
- Scheduling depends on GitHub cron, which can be delayed; the workflow is idempotent
  and can be re-run for any date.
