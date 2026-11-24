"""Integration-test checks (PRD §8.4): compare crypto_it with expected.json and golden.json.

expected.json comes from the fixture builder (trade counts, the gaps it created).
golden.json is the recorded status mix and candle counts of a known-good run
(--record-golden writes it; review the diff before committing a change to it).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from databricks.sdk.service.sql import StatementState

from crypto_recon_common.volume import connect_workspace

HERE = Path(__file__).resolve().parent
EXPECTED = HERE / "expected.json"
GOLDEN = HERE / "golden.json"
COMPLETE = ("MATCH", "OFFICIAL_MISMATCH")


class Warehouse:
    def __init__(self, statements, warehouse_id: str, catalog: str) -> None:
        self._statements, self._warehouse_id, self._catalog = (
            statements,
            warehouse_id,
            catalog,
        )

    def rows(self, sql: str) -> list[list[str]]:
        response = self._statements.execute_statement(
            statement=sql,
            warehouse_id=self._warehouse_id,
            catalog=self._catalog,
            wait_timeout="50s",
        )
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            response = self._statements.get_statement(response.statement_id)
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"query failed: {response.status.error}\n{sql}")
        return (response.result.data_array if response.result else None) or []


def check_trades(wh: Warehouse, expected: dict) -> list[str]:
    rows = wh.rows(
        "SELECT product_id, count(DISTINCT trade_id) FROM silver.trades GROUP BY product_id"
    )
    actual = {p: int(n) for p, n in rows}
    return (
        [] if actual == expected["trades"] else [f"silver trades {actual} != {expected['trades']}"]
    )


def check_gaps(wh: Warehouse, expected: dict) -> list[str]:
    rows = wh.rows(
        "SELECT product_id, from_trade_id, to_trade_id, missing_count, status FROM ops.gaps"
    )
    actual = sorted((p, int(f), int(t), int(m)) for p, f, t, m, _ in rows)
    wanted = sorted(
        (g["product_id"], g["from_trade_id"], g["to_trade_id"], g["missing_count"])
        for g in expected["gaps"]
    )
    errors = [] if actual == wanted else [f"gaps {actual} != {wanted}"]
    not_open = [r for r in rows if r[4] != "open"]
    if not_open:
        errors.append(f"gaps not open (no backfill in the test): {not_open}")
    return errors


def check_gap_minutes(wh: Warehouse) -> list[str]:
    """Each gap must show in reconciliation: a stream-side minute, or less stream volume.

    A small gap can stay within the MATCH volume tolerance (FR-R5) and be invisible to
    candle reconciliation; removed trades still lower the stream's volume in their minute.
    That is why gaps are detected by trade-id continuity (ADR-002), not by reconciliation.
    The exact status mix is pinned by golden.json.
    """
    rows = wh.rows(
        """SELECT g.gap_id, g.product_id, f.status_before_repair, f.volume_delta_pct
           FROM ops.gaps AS g
           JOIN marts.fct_reconciliation_1m AS f
             ON f.product_id = g.product_id
            AND f.minute_ts BETWEEN date_trunc('MINUTE', g.gap_start_time)
                                AND date_trunc('MINUTE', g.gap_end_time)"""
    )
    shows: dict[tuple[str, str], bool] = {}
    for gap_id, product, status, volume_delta in rows:
        lower_volume = volume_delta is not None and float(volume_delta) < 0
        key = (gap_id, product)
        shows[key] = shows.get(key, False) or status not in COMPLETE or lower_volume
    if not shows:
        return ["no reconciled minutes found for the gaps"]
    return [f"gap not visible in reconciliation: {k}" for k, ok in shows.items() if not ok]


def snapshot(wh: Warehouse, day: str, hour: int) -> dict:
    start = f"{day} {hour:02d}:00:00"
    statuses = wh.rows(
        f"""SELECT product_id, status_before_repair, count(*)
            FROM marts.fct_reconciliation_1m
            WHERE minute_ts >= '{start}' AND minute_ts < '{start}'::timestamp + INTERVAL 1 HOUR
            GROUP BY ALL ORDER BY ALL"""  # noqa: S608 (values from expected.json)
    )
    candles = wh.rows(
        f"""SELECT product_id, count(*) FROM silver.candles_official
            WHERE to_date(minute_ts) = '{day}' GROUP BY ALL ORDER BY ALL"""  # noqa: S608
    )
    return {
        "status_mix_in_fixture_hour": {f"{p} {s}": int(n) for p, s, n in statuses},
        "official_candles_for_day": {p: int(n) for p, n in candles},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", default="crypto_it")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--databricks-profile", default=None)
    parser.add_argument("--record-golden", action="store_true")
    args = parser.parse_args()

    expected = json.loads(EXPECTED.read_text())
    workspace = connect_workspace(args.databricks_profile)
    wh = Warehouse(workspace.statement_execution, args.warehouse_id, args.catalog)

    results = {
        "silver trades = fixture trades": check_trades(wh, expected),
        "gaps = the gaps in the fixture, all open": check_gaps(wh, expected),
        "each gap shows in reconciliation (stream-side or lower volume)": check_gap_minutes(wh),
    }
    current = snapshot(wh, expected["day"], expected["hour"])
    if args.record_golden:
        GOLDEN.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
        print(f"recorded {GOLDEN.name}: {json.dumps(current, sort_keys=True)}")
    elif not GOLDEN.exists():
        results["golden.json present"] = ["missing: run once with RECORD_GOLDEN=1"]
    else:
        golden = json.loads(GOLDEN.read_text())
        results["status mix and candles = golden.json"] = (
            [] if current == golden else [f"{current} != {golden}"]
        )

    failed = 0
    for name, errors in results.items():
        print(f"{'PASS' if not errors else 'FAIL'} {name}")
        for error in errors:
            print(f"    {error}")
        failed += bool(errors)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
