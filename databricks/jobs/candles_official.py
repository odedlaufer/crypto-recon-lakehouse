"""Official candles: bronze -> staging -> publish (FR-J1, PRD §6.2 write-audit-publish).

Each step runs as its own job task. The audit between `stage` and `publish` is the Soda
contract in databricks/jobs/soda_audit.py (ADR-004): a failed audit stops the job before
`publish` and leaves `silver.candles_official_staging` in place for debugging.

    --step load_bronze  Auto Loader (availableNow): landing/rest/candles -> bronze.candles_official
    --step stage        latest fetch per (product, minute) for the date range -> staging
    --step publish      MERGE staging -> silver.candles_official
"""

from __future__ import annotations

import argparse
import re
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STEPS = ("load_bronze", "stage", "publish")
IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=STEPS, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--start", default="", help="YYYY-MM-DD; empty means yesterday UTC")
    parser.add_argument("--end", default="", help="YYYY-MM-DD; empty means --start")
    args = parser.parse_args()

    if not IDENTIFIER.fullmatch(args.catalog):
        parser.error(f"invalid catalog name: {args.catalog!r}")
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    args.start = date.fromisoformat(args.start) if args.start else yesterday
    args.end = date.fromisoformat(args.end) if args.end else args.start
    if args.start > args.end:
        parser.error(f"--start {args.start} is after --end {args.end}")
    return args


def load_bronze(spark: SparkSession, catalog: str) -> None:
    landing = f"/Volumes/{catalog}/raw/landing/rest/candles/"
    checkpoint = f"/Volumes/{catalog}/raw/checkpoints/bronze_candles_official"
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "candles_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema("fetched_at STRING, product_id STRING, granularity INT, raw STRING")
        .load(landing)
        .select(
            F.col("raw").alias("payload"),
            "product_id",
            "granularity",
            F.to_timestamp("fetched_at").alias("fetched_at"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            "_rescued_data",
        )
        .writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.candles_official")
    )
    query.awaitTermination()
    print(f"bronze.candles_official: {query.lastProgress}")


def stage(spark: SparkSession, catalog: str, start: date, end: date) -> None:
    # Rows are selected by the file's dt= partition (not by parsed time), so a row that
    # fails to parse still reaches staging with NULLs and is caught by DQ-12.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog}.silver.candles_official_staging AS
        WITH parsed AS (
          SELECT product_id,
                 from_json(payload, 'ARRAY<DECIMAL(38, 18)>') AS v,
                 fetched_at,
                 _source_file
          FROM {catalog}.bronze.candles_official
          WHERE regexp_extract(_source_file, 'dt=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1)
                BETWEEN '{start}' AND '{end}'
        ),
        candles AS (
          SELECT product_id,
                 timestamp_seconds(CAST(v[0] AS BIGINT)) AS minute_ts,
                 v[3] AS open, v[2] AS high, v[1] AS low, v[4] AS close, v[5] AS volume,
                 fetched_at, _source_file
          FROM parsed
        ),
        ranked AS (
          SELECT *, row_number() OVER (
                   PARTITION BY product_id, minute_ts
                   ORDER BY fetched_at DESC, _source_file DESC) AS rn
          FROM candles
        )
        SELECT product_id, minute_ts, open, high, low, close, volume, fetched_at, _source_file
        FROM ranked WHERE rn = 1
    """)
    rows = spark.table(f"{catalog}.silver.candles_official_staging").count()
    print(f"staging: {rows} candles for {start} to {end}")


def publish(spark: SparkSession, catalog: str) -> None:
    target = f"{catalog}.silver.candles_official"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
          product_id STRING, minute_ts TIMESTAMP,
          open DECIMAL(38, 18), high DECIMAL(38, 18), low DECIMAL(38, 18),
          close DECIMAL(38, 18), volume DECIMAL(38, 18),
          fetched_at TIMESTAMP, _source_file STRING, _published_at TIMESTAMP
        ) CLUSTER BY (product_id, minute_ts)
    """)
    result = spark.sql(f"""
        MERGE INTO {target} AS t
        USING {catalog}.silver.candles_official_staging AS s
        ON t.product_id = s.product_id AND t.minute_ts = s.minute_ts
        WHEN MATCHED AND NOT (t.open <=> s.open AND t.high <=> s.high AND t.low <=> s.low
                              AND t.close <=> s.close AND t.volume <=> s.volume) THEN UPDATE SET
          open = s.open, high = s.high, low = s.low, close = s.close, volume = s.volume,
          fetched_at = s.fetched_at, _source_file = s._source_file,
          _published_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
          (product_id, minute_ts, open, high, low, close, volume,
           fetched_at, _source_file, _published_at)
          VALUES (s.product_id, s.minute_ts, s.open, s.high, s.low, s.close, s.volume,
                  s.fetched_at, s._source_file, current_timestamp())
    """).first()
    print(f"publish: {result.asDict()}")


def main() -> None:
    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()
    if args.step == "load_bronze":
        load_bronze(spark, args.catalog)
    elif args.step == "stage":
        stage(spark, args.catalog, args.start, args.end)
    else:
        publish(spark, args.catalog)


if __name__ == "__main__":
    main()
