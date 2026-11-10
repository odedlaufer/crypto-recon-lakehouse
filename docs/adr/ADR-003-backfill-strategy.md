# ADR-003: Backfill gaps from REST trades by trade-id cursor

- Status: Accepted
- Date: 2026-11-10
- Answers: PRD Q2 (how far back can REST trades be paginated?)

## Context
Gaps in the WebSocket stream are repaired from `GET /products/{id}/trades` (FR-B4). The PRD
listed shallow pagination as a risk (§10.1). Coinbase documents cursor pagination with
`before`/`after` and a maximum of 1,000 results per request, but not how far back it reaches.

## Evidence
`scripts/probes/q2_rest_trades_depth.py`, run 2026-11-10 13:33 UTC, jumping back from the
latest trade id by 10^3 to 10^8 trades:

| Product | 10^6 trades back | 10^8 trades back |
| --- | --- | --- |
| BTC-USD | 42.5 h | 4,317 h (about 180 days) |
| ETH-USD | 100.3 h | 7,342 h (about 306 days) |
| SOL-USD | 247.7 h | 10,396 h (about 433 days) |

- `after=X` returns the trades with ids immediately below X (`after=1118144398` returned
  1118144298 to 1118144397), so the cursor is the trade id itself.
- All 18 sampled pages of 100 trades had contiguous trade ids.

## Decision
- Backfill each gap directly by its trade-id range: request `after = to_trade_id + 1` with
  `limit = 1000`, and continue with `after = <lowest id received>` until `from_trade_id`
  is covered. No sequential paging from the latest trade.
- Run gap repair once a day in the daily job (FR-J5), as the PRD plans. History depth is
  months, so there is no need to run it hourly.
- Mark a gap `unrepairable` only when REST does not return the missing ids (a hole in
  Coinbase's own history) or keeps failing after retries; never because of age.

## Consequences
- The PRD §10.1 risk "REST trades pagination is shallow" is retired.
- Repair cost scales with gap size: about 1 request per 1,000 missing trades.
- Trade-id ranges are the backfill contract, so gap detection must produce
  (from_trade_id, to_trade_id); this depends on trade ids being contiguous (ADR-002, Q1).
