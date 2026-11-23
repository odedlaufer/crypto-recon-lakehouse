# Chaos scenarios (PRD §8.5)

Run from GitHub: Actions → `chaos` → Run workflow → scenario.

## CH-2: kill the producer for N minutes mid-window
- Script: `chaos/ch2_kill_producer.sh` (workflow `chaos`, scenario `ch2-kill-producer`).
- What it does: cancels every active capture run, waits until they stop, waits
  `outage_minutes` (default 10), restarts capture with chaining. The run summary records
  the outage start, stop and restart times and a query for the resulting gap.
- Expected outcome: the next `daily-batch` run detects the hole as one trade-id gap per
  product in `ops.gaps`, backfills it from REST, and marks it `repaired`; the affected
  minutes show `status_before_repair = MISSING_IN_STREAM` (or a stream-side status at the
  edges) and `status_after_repair` in (MATCH, OFFICIAL_MISMATCH).
- Avoid the capture cron slots (minute 7 of 00, 05, 10, 15, 20 UTC): a backup run could
  start inside the outage.
- Result: recorded in `docs/phase-gates.md` (Phase 3 gate).

CH-1, CH-3 and CH-4 are added in Phases 4 and 5.

## CH-3: malformed messages
- Script: `chaos/ch3_malformed_messages.py` (run locally; `uv run --package crypto-recon-producer`).
- Sends 7 crafted messages through the producer's own code (parse, batch, landing records)
  and uploads them like a capture. Trade ids are far above Coinbase's and unique per run.
- Expected outcome after the next pipeline run: negative price and zero size →
  `quarantine.trades` (`dq05_price_size_positive`); side `hold` → `dq06_side_valid`;
  time +1 h → `dq07_not_in_future`; `DOGE-USD` → `dq08_product_allowed`; invalid JSON and a
  match without `trade_id` → `quarantine.ws_rejected` (`dq01_schema`); 0 rows in silver.
- Result 2026-11-23 (run 1795433910): all 7 caught by the expected rule; 0 in silver.
- Finding: a future trade time used to advance silver's dedupe watermark before the
  expectation dropped it, which would have discarded later real trades as late data.
  Fixed: such rows are removed before the watermark (they still land in quarantine).

## CH-4: corrupted official-candle file
- Script: `chaos/ch4_corrupt_candles.py inject|restore` (default day 2026-11-15, BTC-USD).
- `inject` lands a newer fetch of the day with low and high swapped in one candle; the
  `candles_official` job for that day must fail the Soda audit (DQ-11) and skip `publish`.
  `restore` lands a correct newer fetch; the job passes again.
- Result 2026-11-23: audit failed DQ-11, publish skipped, silver unchanged (BTC 00:00 kept
  high 64118.86 > low 64061.05); after restore 11/11 checks passed, publish 0 changes.
- Finding: serverless auto-optimization retried the failed audit 3 times; the audit task
  now sets `disable_auto_optimization: true` (a failed gate is deterministic).
