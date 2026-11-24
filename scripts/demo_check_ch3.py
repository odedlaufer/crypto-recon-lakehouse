"""Show where a CH-3 run's poisoned messages ended up (used by scripts/demo.sh).

    uv run --package crypto-recon-common python scripts/demo_check_ch3.py --run-id 1795433910 \
        --warehouse-id ID --databricks-profile personal
"""

from __future__ import annotations

import argparse
import os
import sys

from crypto_recon_common.sql import query
from crypto_recon_common.volume import connect_workspace

TRADE_ID_BASE = 9_000_000_000_000  # same as chaos/ch3_malformed_messages.py
EXPECTED = 7  # 5 trades in quarantine.trades + 2 messages in quarantine.ws_rejected


def check(run, run_id: int) -> tuple[list[tuple[str, str]], int]:
    """Rows caught for this run as (where, rule), and how many reached silver."""
    low, high = TRADE_ID_BASE + run_id * 100, TRADE_ID_BASE + run_id * 100 + 99
    caught = [
        ("quarantine.trades", row["rule_failed"])
        for row in run(
            f"SELECT trade_id, rule_failed FROM quarantine.trades "  # noqa: S608
            f"WHERE trade_id BETWEEN {low} AND {high} ORDER BY trade_id"
        )
    ]
    caught += [
        ("quarantine.ws_rejected", row["rule_failed"].split(":")[0])
        for row in run(
            "SELECT rule_failed FROM quarantine.ws_rejected "  # noqa: S608
            f"WHERE payload LIKE '%chaos-ch3-{run_id}%'"
        )
    ]
    [leaked] = run(
        f"SELECT count(*) AS n FROM silver.trades WHERE trade_id BETWEEN {low} AND {high}"  # noqa: S608
    )
    return caught, int(leaked["n"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--catalog", default="crypto_dev")
    parser.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--databricks-profile", default=None)
    args = parser.parse_args()
    statements = connect_workspace(args.databricks_profile).statement_execution

    def run(sql: str) -> list[dict]:
        return query(statements, args.warehouse_id, args.catalog, sql)

    caught, leaked = check(run, args.run_id)
    print(f"\n  {'caught in':<24} rule")
    for where, rule in caught:
        print(f"  {where:<24} {rule}")
    print(f"\n  caught {len(caught)} of {EXPECTED}; reached silver.trades: {leaked}")
    ok = len(caught) == EXPECTED and leaked == 0
    print("  RESULT: every poisoned message was stopped" if ok else "  RESULT: NOT as expected")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
