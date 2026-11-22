# Phase gates

Each phase ends with a gate that must pass before the next phase starts (PRD §9).

## Phase 0: Setup (passed 2026-11-10)
Gate: empty bundle deploys; dbt connects.
- `databricks bundle deploy -t dev` and `-t prd` succeed (catalogs `crypto_dev`, `crypto_prd`).
- `dbt debug` against `crypto_dev`: all checks passed.
- Deferred: Slack workspace and webhook, moved to the start of Phase 4.

## Phase 1: Stream ingestion (passed 2026-11-10)
Gate: 1 hour of live trades in silver with zero duplicates; Q1 and Q2 answered.

Capture 2026-11-10 14:44:22 to 15:44:18 UTC (producer run `920d1296632047cfa6859da142bd1a3c`),
uploaded to `/Volumes/crypto_dev/raw/landing`, loaded by the `crypto-recon-stream-dev` pipeline.

| Product | Minutes covered | Trades | Distinct ids | Id span | Breaks |
| --- | --- | --- | --- | --- | --- |
| BTC-USD | 59.9 | 42,281 | 42,281 | 42,281 | 0 |
| ETH-USD | 59.9 | 31,312 | 31,312 | 31,312 | 0 |
| SOL-USD | 59.9 | 11,314 | 11,314 | 11,314 | 0 |

- Producer: 95,696 messages, 95,695 landed, 0 rejected, 0 reconnects, 0 trade gaps,
  0 heartbeat gaps, 830 files flushed and uploaded, 0 left locally.
- Zero duplicates and zero missing trade ids in silver.
- Q1 answered in [ADR-002](adr/ADR-002-gap-detection.md): trade ids are contiguous per product.
- Q2 answered in [ADR-003](adr/ADR-003-backfill-strategy.md): REST trades reach months back by trade-id cursor.

## Phase 2: Batch and reconciliation (passed 2026-11-19)
Gate: one full day reconciled; status mix visible in a query.

### Built and verified
- Official candles fetcher and backfill CLI: 2026-11-10 = 5 requests, 1,440 candles per product.
- Candles write-audit-publish job: DQ-11 to DQ-14 all pass for 2026-11-09 and 2026-11-10
  (1,440 of 1,440 minutes per product); re-runs are idempotent (0 inserted, 0 updated).
- `gold.candles_stream_1m` as a materialized view ([ADR-009](adr/ADR-009-gold-candles-materialized-view.md)).
- `fct_reconciliation_1m` with before/after-repair statuses; 3 dbt unit tests pass.
- Daily orchestration from GitHub Actions ([ADR-010](adr/ADR-010-orchestration.md)):
  manual runs green end to end (fetch, Databricks daily job, dbt PASS=17).

### Findings
**Stream candles reproduce official candles.** Interior minutes of the Phase 1 gate capture
(2026-11-10 14:45 to 15:43 UTC): 58 of 59 minutes match exactly (OHLC equal, volume within
0.01%) for each of BTC-USD, ETH-USD and SOL-USD.

**One official candle contradicts Coinbase's own trades.** The remaining minute, 15:20 UTC,
mismatches for all three products. The stream candle equals Coinbase REST trades for that
minute; the official candle does not, and was unchanged when re-fetched.

| Product | Stream = REST trades (volume) | Official candle (volume) |
| --- | --- | --- |
| BTC-USD | 9.05326740 (491 trades) | 2.45754029 |
| ETH-USD | 164.90581577 (326 trades) | 73.17267681 |
| SOL-USD | 2144.68203455 (144 trades) | 1006.77400261 |

**Reconciliation of 2026-11-10** (1,440 minutes per product): about 1,350 MISSING_IN_STREAM
per product (no capture running), 73 to 75 MATCH, 14 to 16 PRICE_MISMATCH and up to 1
VOLUME_DRIFT. The mismatches are mostly partially captured first and last minutes of the
short test captures that day, plus 15:20.

**Q5 (late corrections), first data points.** 2026-11-10 fetched at about 08:10 and again at
about 10:47 UTC on 2026-11-11: 0 of 4,320 minutes changed. The daily workflow now re-fetches
the previous day on every run, so evidence accumulates for ADR-011.

### Open
- Capture schedule: the first scheduled slot (2026-11-11 10:07 UTC) did not start a run;
  a manual 345-minute run was started instead. Next slot to confirm: 15:07 UTC.
- daily-batch schedule: first scheduled run due 2026-11-12 00:37 UTC.
- Gate: needs a fully captured UTC day; earliest 2026-11-12, reconciled 2026-11-13.

### Gate result (2026-11-19)
Three fully captured days reconciled (`missing_in_stream = 0`, `missing_in_batch` at most 3):

| Day | BTC-USD match | ETH-USD match | SOL-USD match |
| --- | --- | --- | --- |
| 2026-11-13 | 96.74% | 96.67% | 96.67% |
| 2026-11-14 | 98.19% | 98.47% | 98.13% |
| 2026-11-15 | 96.39% | 96.39% | 96.04% |

Days before 2026-11-13 were only partly captured (missed cron slots); continuity came
from self-chaining capture (ADR-008 amendment).

### Why match is below 99.5%: diagnosis of 2026-11-14
- No duplicates in silver (rows = distinct trade ids for all products).
- **Stream gaps:** 13 trade-id breaks of 1 to 3 seconds (BTC 220 ids, ETH 13, SOL 3).
  They explain exactly the VOLUME_DRIFT minutes; repairable from REST (Phase 3).
- **Official-candle errors:** the PRICE_MISMATCH minutes (about 20 per product, mostly
  pairs of consecutive minutes hitting all products at once) have no stream gap. Over
  each 2-minute event the official volume is lower than the stream's for every product
  (15 to 58% lower), so trades are missing from the official candles, not shifted
  between minutes. Same pattern as 2026-11-10 15:20, where REST trades matched the
  stream exactly. Not repairable; handled by ADR-012.

### Updates to the open items
- Capture continuity: solved by self-chaining (ADR-008 amendment); fully captured from
  2026-11-13.
- daily-batch: refused by Databricks since 2026-11-17 ("Triggering new runs ... disabled
  temporarily"), caused by Volume API operations from many small files; fixed in
  ADR-011, waiting for the limit to lift. Official candles for 2026-11-16 to 2026-11-18
  to be re-run once allowed.

## Phase 3: Gap detection and self-healing (passed 2026-11-22)
Gate: CH-2 (kill the producer for 10 minutes) produces a gap that is detected and repaired
automatically.

CH-2 run 2026-11-19 (`chaos` workflow): capture cancelled 12:21:42 UTC, stopped 12:22:10,
restarted 12:32:11. The next scheduled `daily-batch` run detected, backfilled and
reconciled it with no manual step:

| Product | Gap (trade ids) | Status | Minutes before repair → after repair |
| --- | --- | --- | --- |
| BTC-USD | 14,438 | repaired | 10 MISSING_IN_STREAM → MATCH; 2 PRICE_MISMATCH (edges) → MATCH |
| ETH-USD | 1,764 | repaired | 10 MISSING_IN_STREAM → MATCH; 2 PRICE_MISMATCH (edges) → MATCH |
| SOL-USD | 1,538 | repaired | 10 MISSING_IN_STREAM → MATCH; 2 PRICE_MISMATCH (edges) → MATCH |

### State on 2026-11-22
- `ops.gaps`: 108 repaired (1,334,852 trades), 0 open, 0 unrepairable.
- Completeness (ADR-012 definitions):

| Day | Raw | Repaired | Match rate |
| --- | --- | --- | --- |
| 2026-11-13 | 99.954% | 100% | 96.736% |
| 2026-11-14 | 99.745% | 100% | 98.519% |
| 2026-11-15 | 100% | 100% | 96.273% |
| 2026-11-16 | 99.907% | 100% | 97.824% |
| 2026-11-17 | 99.838% | 100% | 98.750% |
| 2026-11-18 | 99.931% | 100% | 96.435% |
| 2026-11-19 | 99.051% (CH-2) | 100% | 97.940% |
| 2026-11-20 | 99.907% | 100% | 98.981% |
| 2026-11-21 | 99.977% | 100% | 98.912% |

The gap between repaired completeness and match rate is Coinbase's official candles
disagreeing with its own trades (OFFICIAL_MISMATCH, ADR-012).
