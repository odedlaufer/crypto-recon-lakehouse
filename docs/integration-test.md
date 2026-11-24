# Integration test (PRD §8.4)

Replays one recorded hour of real capture through the real system in its own catalog
(`crypto_it`, bundle target `it`) and checks the result. Deterministic: the same fixture
always gives the same gaps, counts and status mix.

## Fixture
- 2026-11-21 10:00 to 11:00 UTC from the dev landing Volume: 34,375 BTC, 7,132 ETH, 4,800
  SOL trades, heartbeats, and the day's official-candle fetches (`tests/integration/fixture/`,
  2.7 MB gzipped).
- Three known gaps removed: 5 BTC ids (minute 20), 3 ETH (minute 35), 2 SOL (minute 50).
- `expected.json`: removed ids, trades per product, all gaps. Built by
  `tests/integration/build_fixture.py` (instructions in its docstring).

## Run
- Locally: `tests/integration/run_it.sh` (CLI profile `personal`, dbt target `it`).
- GitHub: Actions → `integration-test` → Run workflow.
- Stages: upload fixture → stream pipeline → candles job (Soda gate) → `dbt build
  --full-refresh` pinned to the fixture's day (unit tests excluded; they run in dev) →
  checks. No REST backfill, so the gaps stay open.

## Checks (`check_it.py`)
1. Silver trades per product equal the fixture's trades.
2. `ops.gaps` equals exactly the fixture's gaps, all `open`.
3. Each gap shows in reconciliation: a stream-side minute, or less stream volume than
   the official candle.
4. The status mix of the fixture hour and the day's official candle counts equal
   `golden.json` (recorded once with `RECORD_GOLDEN=1`; review any change before committing).

## Findings from the first runs (2026-11-24)
- The Soda audit assumed `ops.dq_results` existed (it used to be created by the old SQL
  audit); in a fresh catalog the gate crashed. It now creates the table if missing.
- The 2-trade SOL gap changed that minute's volume by -0.001%, within the MATCH tolerance
  (FR-R5): candle reconciliation alone cannot see such a gap; trade-id gap detection did
  (ADR-002).
