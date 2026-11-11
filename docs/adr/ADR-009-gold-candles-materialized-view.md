# ADR-009: Build gold.candles_stream_1m as a materialized view

- Status: Accepted
- Date: 2026-11-11

## Context
FR-S4 builds 1-minute candles from trades; FR-J3 requires repaired minutes to be
recomputed after gap backfill. A streaming aggregation with a watermark emits each minute
once and drops trades that arrive after the watermark, so backfilled trades would never
reach gold without custom recompute logic.

## Decision
`gold.candles_stream_1m` is a materialized view in the stream pipeline over
`silver.trades`, deduplicated on (product_id, trade_id), grouped by product and minute.
Open and close use `min_by` / `max_by` on trade_id; clustered by (product_id, minute_ts).

## Consequences
- Gap repair only has to add trades to silver; the next pipeline update corrects the
  affected minutes (FR-J3) with no extra code.
- Candles are as fresh as the last pipeline update, not continuous; reconciliation only
  uses minutes older than the 15-minute settlement window anyway (FR-R1).
- Refresh cost grows with history; mitigated by serverless incremental refresh and the
  90-day retention limit (PRD §3).
