# ADR-002: Detect gaps by trade-id continuity per product

- Status: Accepted
- Date: 2026-11-10
- Answers: PRD Q1 (is trade_id contiguous per product on the matches channel?)

## Context
FR-J2 detects gaps from breaks in trade_id continuity, which only works if trade ids are
contiguous per product. The PRD's fallback (§10.1) was heartbeat `last_trade_id` plus
candle-level reconciliation only.

## Evidence
- Stream: `silver.trades` in dev, uninterrupted capture 2026-11-10 13:19:15 to 13:37:15 UTC.
  Per product, ordered by trade_id, every step was exactly 1.

  | Product | Trades | Distinct ids | Id span | Breaks |
  | --- | --- | --- | --- | --- |
  | BTC-USD | 18,290 | 18,290 | 18,290 | 0 |
  | ETH-USD | 13,101 | 13,101 | 13,101 | 0 |
  | SOL-USD | 4,965 | 4,965 | 4,965 | 0 |

- REST: all 18 pages sampled by `scripts/probes/q2_rest_trades_depth.py` (3 products,
  depths from 10^3 to 10^8 trades back) had contiguous ids (ADR-003).
- The producer's heartbeat check reported 0 trade gaps for the same run.

## Decision
- Primary method: per product, order `silver.trades` by trade_id; every step greater
  than 1 is a gap with `from_trade_id = previous + 1` and `to_trade_id = next - 1`.
- Secondary method: a heartbeat whose `last_trade_id` is above the highest trade id
  received for that product marks a tail gap (trades announced but never received).
- Gaps are expressed as trade-id ranges, which is exactly the backfill contract (ADR-003).

## Consequences
- Detection is exact: missing count = id range size; no time-based guessing.
- The PRD fallback (heartbeat plus candle-level only) is not needed.
- Follow-up for Phase 2: the PRD captures in windows, so the periods between capture
  windows also appear as trade-id gaps. Whether these are backfilled or excluded as
  "not captured" must be decided with the reconciliation scope and the raw-completeness
  SLO definition.
