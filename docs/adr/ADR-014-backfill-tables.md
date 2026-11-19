# ADR-014: Backfilled trades in their own bronze and silver tables

- Status: Accepted
- Date: 2026-11-19
- Changes: PRD §4 / FR-J3 (backfill MERGE into silver.trades with a source column)

## Context
FR-J3 merges backfilled trades into silver.trades. silver.trades is a streaming table
owned by the stream pipeline. Adding a second input to it means redefining that table's
flows, with a risk that the pipeline treats it as a new flow and reprocesses all of
bronze (duplicates in silver and a large compute cost on the Free Edition quota).

## Decision
- `bronze.trades_backfill` (Auto Loader over `rest/trades/`) and
  `silver.trades_backfill` (typed like silver.trades, `source = 'backfill'`).
- `silver.trades_backfill` deduplicates within a watermark on `fetched_at`, not
  `exchange_time`: backfill files arrive in any order, and an exchange-time watermark
  would drop older gaps as late data.
- `bronze.backfill_attempts`: one row per gap per backfill run (requested, received,
  missing), the evidence for `unrepairable`.
- Consumers read both trade tables and deduplicate on (product_id, trade_id):
  `gold.candles_stream_1m` and dbt `stg_trades`.

## Consequences
- silver.trades and its checkpoint are never touched.
- "All trades" is a union in two places (gold, dbt) instead of one table.
- A trade that arrives by both stream and backfill is counted once.
