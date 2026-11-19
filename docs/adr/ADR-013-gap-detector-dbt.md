# ADR-013: Gap detector as an incremental dbt model

- Status: Accepted
- Date: 2026-11-19
- Changes: PRD FR-J2 ("gap detector job")

## Context
FR-J2 describes the gap detector as a Databricks job. Detection is set-based SQL over
silver.trades (a trade-id jump per product, ADR-002), and gap status (open, repaired) is
a join against the same table. Since 2026-11-17 Databricks Free Edition refuses new job
runs (ADR-011), while the SQL warehouse keeps working; dbt already runs from GitHub
Actions (ADR-010).

## Decision
- `ops.gaps` is an incremental dbt model (merge on `gap_id`), with an enforced contract.
- Each run detects breaks in the last `gap_lookback_days` (3) of trades, adds new gaps,
  and re-evaluates every open gap: `repaired` once all its ids are present.
- A break inside a tracked gap (partial repair) is not a new gap, so missing ids are never
  double-counted.
- `unrepairable` is added with the backfill step, once attempts are recorded.

## Consequences
- Same lineage, docs and unit tests as reconciliation; no extra job to run or to be
  refused.
- Gap status is state kept in an incremental model, a less common dbt pattern; covered by
  unit tests for detection and status transitions.
- Breaks older than the lookback are not detected later; the daily flow runs well within
  that window.
