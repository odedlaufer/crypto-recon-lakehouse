"""Audit staged official candles with a Soda data contract (PRD §6.2, ADR-004).

Verifies contracts/candles_official_staging.yml against silver.candles_official_staging
and appends one row per check to ops.dq_results. With --enforce, raises when any check
fails, so the job stops before `publish`.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta

import yaml
from pyspark.sql import SparkSession
from soda_core.contracts import verify_contract_locally
from soda_sparkdf import SparkDataFrameDataSource

DATA_SOURCE = "crypto_recon"  # first part of the contract's dataset name


class AuditFailedError(RuntimeError):
    """At least one contract check failed or was not evaluated; nothing is published."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--products-config", required=True)
    parser.add_argument("--start", default="", help="YYYY-MM-DD; empty means yesterday UTC")
    parser.add_argument("--end", default="", help="YYYY-MM-DD; empty means --start")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--enforce", action="store_true", help="Fail the task on a failed check")
    args = parser.parse_args()
    args.start = (
        date.fromisoformat(args.start) if args.start else datetime.now(UTC).date() - timedelta(1)
    )
    args.end = date.fromisoformat(args.end) if args.end else args.start
    return args


def main() -> None:
    args = _parse_args()
    with open(args.products_config, encoding="utf-8") as f:
        products = list(yaml.safe_load(f)["products"])

    spark = SparkSession.builder.getOrCreate()
    data_source = SparkDataFrameDataSource.from_existing_session(session=spark, name=DATA_SOURCE)
    result = verify_contract_locally(
        data_sources=[data_source],
        contract_file_path=args.contract,
        variables={
            "CATALOG": args.catalog,
            "START_DATE": args.start.isoformat(),
            "END_DATE": args.end.isoformat(),
            "PRODUCTS": ",".join(products),
        },
        publish=False,
    )

    staging = f"{args.catalog}.silver.candles_official_staging"
    checked_at = datetime.now(UTC)
    rows, failed = [], []
    for verification in result.contract_verification_results:
        for check_result in verification.check_results:
            name = check_result.check.name
            passed = check_result.is_passed
            observed = check_result.threshold_value
            print(f"{'PASS' if passed else 'FAIL'} {name} observed={observed}")
            if not passed:
                failed.append(name)
            rows.append(
                (
                    args.run_id,
                    checked_at,
                    f"soda: {name}",
                    staging,
                    f"{args.start}..{args.end}",
                    passed,
                    None if observed is None else float(observed),
                    "contract",
                    json.dumps(check_result.diagnostic_metric_values, default=str),
                )
            )

    if not rows:
        failed.append("no check results")
    schema = spark.table(f"{args.catalog}.ops.dq_results").schema
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(
            f"{args.catalog}.ops.dq_results"
        )
    print(f"contract verification ok: {result.is_ok}; failed: {failed}")
    if args.enforce and (failed or not result.is_ok):
        raise AuditFailedError(f"write-audit-publish gate failed, not publishing: {failed}")


if __name__ == "__main__":
    main()
