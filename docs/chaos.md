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
