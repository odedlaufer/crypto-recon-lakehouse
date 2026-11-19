"""bronze.backfill_attempts: one row per gap per backfill run (FR-J3).

Records how many of a gap's ids REST returned; the evidence for `unrepairable` in ops.gaps.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
LANDING_PATH = spark.conf.get("landing_path")

ATTEMPT_SCHEMA = (
    "fetched_at STRING, gap_id STRING, product_id STRING, from_trade_id BIGINT, "
    "to_trade_id BIGINT, requested BIGINT, received BIGINT, missing BIGINT"
)


@dp.table(
    name="bronze.backfill_attempts",
    comment="One row per gap per backfill run: requested, received and missing from REST.",
    table_properties={"quality": "bronze"},
)
def backfill_attempts():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "attempts_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(ATTEMPT_SCHEMA)
        .load(f"{LANDING_PATH}/rest/trades_attempts/")
        .select(
            F.to_timestamp("fetched_at").alias("fetched_at"),
            "gap_id",
            "product_id",
            "from_trade_id",
            "to_trade_id",
            "requested",
            "received",
            "missing",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_rescued_data"),
        )
    )
