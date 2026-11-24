"""Export the numbers the public site shows (web/) as small JSON files.

Runs in the docs workflow after each daily-batch run; reads the marts and ops tables
through the SQL warehouse, so no credentials ever reach the browser.

    uv run --package crypto-recon-common python scripts/export_site_data.py \
        --out site/data --databricks-profile personal
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from crypto_recon_common.sql import query
from crypto_recon_common.volume import connect_workspace

SLOS_SQL = """
WITH completeness AS (
  SELECT round(100 * sum(complete_minutes_after)  / sum(minutes), 3) AS repaired_completeness_pct,
         round(100 * sum(complete_minutes_before) / sum(minutes), 3) AS raw_completeness_pct,
         round(100 * sum(match_minutes)           / sum(minutes), 3) AS match_rate_pct
  FROM marts.mart_completeness_hourly
  WHERE hour_ts >= current_date() - INTERVAL 7 DAYS
),
newest_trade AS (
  SELECT max(exchange_time) AS exchange_time, max(_ingested_at) AS ingested_at,
         count_if(exchange_time >= current_date() - INTERVAL 7 DAYS) AS trades_7d
  FROM silver.trades
),
quarantined AS (
  SELECT count(*) AS rows_7d FROM quarantine.trades
  WHERE exchange_time >= current_date() - INTERVAL 7 DAYS
),
producer AS (
  SELECT round(percentile(latency_p95_ms, 0.95), 1) AS latency_p95_ms FROM ops.producer_metrics
  WHERE window_start >= current_date() - INTERVAL 7 DAYS
),
repairs AS (
  SELECT round(percentile((unix_timestamp(repaired_at) - unix_timestamp(detected_at)) / 3600.0,
                          0.95), 2) AS gap_repair_p95_h
  FROM ops.gaps WHERE status = 'repaired' AND repaired_at >= current_date() - INTERVAL 7 DAYS
),
gate AS (
  SELECT round(100 * count_if(all_passed) / count(*), 1) AS batch_gate_pass_pct
  FROM (SELECT run_id, bool_and(passed) AS all_passed FROM ops.dq_results
        WHERE checked_at >= current_date() - INTERVAL 7 DAYS GROUP BY run_id) AS audit_runs
),
pipeline AS (
  SELECT round(100 * count_if(succeeded) / count(*), 1) AS pipeline_success_pct
  FROM ops.pipeline_runs WHERE started_at >= current_date() - INTERVAL 7 DAYS
)
SELECT c.repaired_completeness_pct, c.raw_completeness_pct, c.match_rate_pct,
       round((unix_timestamp(n.ingested_at) - unix_timestamp(n.exchange_time)) / 60.0, 1)
         AS freshness_at_load_min,
       p.latency_p95_ms, r.gap_repair_p95_h,
       round(100 * q.rows_7d / n.trades_7d, 4) AS quarantine_rate_pct,
       g.batch_gate_pass_pct, pl.pipeline_success_pct
FROM completeness c
CROSS JOIN newest_trade n CROSS JOIN quarantined q
CROSS JOIN producer p CROSS JOIN repairs r CROSS JOIN gate g CROSS JOIN pipeline pl
"""

TOTALS_SQL = """
SELECT
  (SELECT count(*) FROM silver.trades) AS stream_trades,
  (SELECT count(*) FROM silver.trades_backfill) AS backfilled_trades,
  (SELECT count(*) FROM ops.gaps WHERE status = 'repaired') AS gaps_repaired,
  (SELECT coalesce(sum(missing_count), 0) FROM ops.gaps WHERE status = 'repaired')
    AS trades_recovered,
  (SELECT count(*) FROM ops.gaps WHERE status = 'open') AS gaps_open,
  (SELECT count(*) FROM ops.gaps WHERE status = 'unrepairable') AS gaps_unrepairable,
  (SELECT count(*) FROM marts.fct_reconciliation_1m) AS minutes_reconciled,
  (SELECT count_if(status_after_repair = 'OFFICIAL_MISMATCH') FROM marts.fct_reconciliation_1m)
    AS official_mismatch_minutes,
  (SELECT cast(min(to_date(minute_ts)) AS STRING) FROM marts.fct_reconciliation_1m)
    AS first_day,
  (SELECT cast(max(to_date(minute_ts)) AS STRING) FROM marts.fct_reconciliation_1m)
    AS last_day
"""

COMPLETENESS_SQL = """
SELECT cast(to_date(hour_ts) AS STRING) AS day,
       round(100 * sum(complete_minutes_before) / sum(minutes), 3) AS raw_pct,
       round(100 * sum(complete_minutes_after)  / sum(minutes), 3) AS repaired_pct,
       round(100 * sum(match_minutes)           / sum(minutes), 3) AS match_rate_pct,
       sum(official_mismatch_minutes) AS official_mismatch_minutes
FROM marts.mart_completeness_hourly
GROUP BY 1 ORDER BY 1
"""

GAPS_SQL = """
SELECT cast(to_date(gap_start_time) AS STRING) AS day,
       count(*) AS gaps,
       sum(missing_count) AS missing_trades,
       count_if(status = 'repaired') AS repaired
FROM ops.gaps
GROUP BY 1 ORDER BY 1
"""

MARKET_SQL = """
SELECT product_id, cast(day AS STRING) AS day, round(vwap, 2) AS vwap,
       trade_count, round(realized_vol_pct, 3) AS realized_vol_pct
FROM marts.mart_market_daily
ORDER BY day, product_id
"""

# PRD §2 targets, as the site shows them (ADR-011 and ADR-012 adjustments noted).
SLO_TARGETS = {
    "repaired_completeness_pct": {
        "label": "Repaired completeness",
        "unit": "%",
        "op": ">=",
        "target": 99.9,
    },
    "raw_completeness_pct": {
        "label": "Raw completeness",
        "unit": "%",
        "op": None,
        "target": None,
    },
    "match_rate_pct": {
        "label": "Match with official candles",
        "unit": "%",
        "op": None,
        "target": None,
    },
    "freshness_at_load_min": {
        "label": "Freshness at load",
        "unit": "min",
        "op": "<=",
        "target": 30,
    },
    "latency_p95_ms": {"label": "Producer latency p95", "unit": "ms", "op": "<", "target": 90000},
    "gap_repair_p95_h": {"label": "Gap repair time p95", "unit": "h", "op": "<", "target": 24},
    "quarantine_rate_pct": {"label": "Quarantine rate", "unit": "%", "op": "<", "target": 0.1},
    "batch_gate_pass_pct": {
        "label": "Batch gate pass rate",
        "unit": "%",
        "op": ">=",
        "target": 100,
    },
    "pipeline_success_pct": {
        "label": "Pipeline success rate",
        "unit": "%",
        "op": ">=",
        "target": 95,
    },
}


def _num(value: str | None) -> float | None:
    return None if value is None else float(value)


def _met(value: float | None, op: str | None, target: float | None) -> bool | None:
    if value is None or op is None:
        return None
    return {">=": value >= target, "<=": value <= target, "<": value < target}[op]


def build_slos(row: dict[str, str | None]) -> list[dict]:
    slos = []
    for key, spec in SLO_TARGETS.items():
        value = _num(row.get(key))
        slos.append(
            {"key": key, **spec, "value": value, "met": _met(value, spec["op"], spec["target"])}
        )
    return slos


def numeric(rows: list[dict], keep_text: tuple[str, ...]) -> list[dict]:
    return [{k: (v if k in keep_text else _num(v)) for k, v in row.items()} for row in rows]


def export(run, out: Path, now: datetime) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    totals = numeric(run(TOTALS_SQL), ("first_day", "last_day"))[0]
    files = {
        "summary.json": {
            "generated_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "slos": build_slos(run(SLOS_SQL)[0]),
            "totals": totals,
        },
        "completeness_daily.json": numeric(run(COMPLETENESS_SQL), ("day",)),
        "gaps_daily.json": numeric(run(GAPS_SQL), ("day",)),
        "market_daily.json": numeric(run(MARKET_SQL), ("product_id", "day")),
    }
    written = []
    for name, payload in files.items():
        path = out / name
        path.write_text(json.dumps(payload, separators=(",", ":")) + "\n")
        written.append(path)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("site/data"))
    parser.add_argument("--catalog", default="crypto_dev")
    parser.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--databricks-profile", default=None)
    args = parser.parse_args()
    if not args.warehouse_id:
        parser.error("--warehouse-id or DATABRICKS_WAREHOUSE_ID is required")

    statements = connect_workspace(args.databricks_profile).statement_execution

    def run(sql: str) -> list[dict]:
        return query(statements, args.warehouse_id, args.catalog, sql)

    for path in export(run, args.out, datetime.now(UTC)):
        print(f"wrote {path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
