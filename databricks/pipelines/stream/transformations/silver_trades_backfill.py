"""silver.trades_backfill: backfilled trades, typed like silver.trades (FR-J3, ADR-014).

Kept separate from silver.trades so the stream's streaming table is never redefined.
Consumers (gold candles, dbt) read both and deduplicate on (product_id, trade_id).

The watermark is on fetched_at, not exchange_time: backfill files arrive in any order
(an old gap can be fetched after a newer one), and an exchange_time watermark would drop
older gaps as late data.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

REST_TRADE_SCHEMA = "trade_id BIGINT, side STRING, size STRING, price STRING, time STRING"
DECIMAL = "DECIMAL(38, 18)"


@dp.table(
    name="silver.trades_backfill",
    comment="One row per trade fetched from REST to repair a gap; source = backfill.",
    table_properties={"quality": "silver"},
)
def trades_backfill():
    return (
        spark.readStream.table("bronze.trades_backfill")
        .select(
            F.from_json("payload", REST_TRADE_SCHEMA).alias("t"),
            "product_id",
            "gap_id",
            "fetched_at",
            "_ingested_at",
            "_source_file",
        )
        .select(
            F.col("product_id"),
            F.col("t.trade_id").alias("trade_id"),
            F.col("t.price").cast(DECIMAL).alias("price"),
            F.col("t.size").cast(DECIMAL).alias("size"),
            F.col("t.side").alias("side"),
            F.to_timestamp("t.time").alias("exchange_time"),
            F.lit("backfill").alias("source"),
            "gap_id",
            "fetched_at",
            "_ingested_at",
            "_source_file",
        )
        .withWatermark("fetched_at", "1 hour")
        .dropDuplicatesWithinWatermark(["product_id", "trade_id"])
    )
