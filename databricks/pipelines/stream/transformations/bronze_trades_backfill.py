"""bronze.trades_backfill: one row per trade fetched from REST for a gap (FR-B4, FR-J3).

Auto Loader reads the backfill files written by `crypto_recon_fetchers backfill`. Each line
is {"fetched_at", "product_id", "gap_id", "raw": "<REST trade JSON>"}.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
LANDING_PATH = spark.conf.get("landing_path")


@dp.table(
    name="bronze.trades_backfill",
    comment="One row per trade fetched from Coinbase REST to repair a gap.",
    table_properties={"quality": "bronze"},
)
def trades_backfill():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "trades_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema("fetched_at STRING, product_id STRING, gap_id STRING, raw STRING")
        .load(f"{LANDING_PATH}/rest/trades/")
        .select(
            F.col("raw").alias("payload"),
            "product_id",
            "gap_id",
            F.to_timestamp("fetched_at").alias("fetched_at"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_rescued_data"),
        )
    )
