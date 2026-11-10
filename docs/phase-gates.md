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
