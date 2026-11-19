# ADR-012: Classify official-candle errors as OFFICIAL_MISMATCH

- Status: Accepted
- Date: 2026-11-19
- Changes: PRD FR-R3 (a sixth status) and the completeness SLO definitions (§2)

## Context
On 2026-11-14 (fully captured), about 1.4% of minutes per product were PRICE_MISMATCH
with no trade-id gap in the stream. Over each event the official volume was 15 to 58%
lower than the stream's for every product, and on 2026-11-10 15:20 Coinbase's REST trades
matched the stream exactly. Official candles therefore sometimes miss trades. Gap repair
cannot change those minutes, so "repaired completeness >= 99.9%" would measure Coinbase's
candle errors, not this pipeline.

## Decision
- A stream minute is provably complete when its trade ids have no internal gap
  (`trade_count = last_trade_id - first_trade_id + 1`) and connect to the previous and next
  stream minutes (ADR-002). A minute without a stream neighbour on either side is not
  provable.
- New status `OFFICIAL_MISMATCH`: the minute is not a MATCH (or the official candle is
  missing) but the stream minute is provably complete.
- PRICE_MISMATCH, VOLUME_DRIFT and MISSING_IN_BATCH now mean the stream is not provably
  complete: candidates for gap repair.
- Completeness (raw and repaired) = (MATCH + OFFICIAL_MISMATCH) / reconciled minutes.
  Match rate = MATCH / reconciled minutes is still reported, as the agreement with
  Coinbase's official candles.

## Consequences
- Completeness measures what the pipeline controls; official-candle errors become a
  measured finding instead of a hidden penalty.
- No REST calls are needed to classify; the proof is trade-id continuity.
- `fct_reconciliation_1m` needs one full refresh so existing minutes are classified with
  the new rule (safe before any repair has run).
