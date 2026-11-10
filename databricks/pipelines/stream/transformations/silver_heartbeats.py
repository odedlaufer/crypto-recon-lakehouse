"""silver.heartbeats: one row per heartbeat, used for gap detection (FR-S2, FR-J2)."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

HEARTBEAT_SCHEMA = (
    "type STRING, sequence BIGINT, last_trade_id BIGINT, product_id STRING, time STRING"
)


@dp.table(
    name="silver.heartbeats",
    comment="One row per Coinbase heartbeat (one per second per product).",
    table_properties={"quality": "silver"},
)
def heartbeats():
    return (
        spark.readStream.table("bronze.ws_events")
        .where(F.col("channel") == "heartbeat")
        .select(F.from_json("payload", HEARTBEAT_SCHEMA).alias("h"), "received_at", "_ingested_at")
        .select(
            F.col("h.product_id").alias("product_id"),
            F.col("h.sequence").alias("sequence"),
            F.col("h.last_trade_id").alias("last_trade_id"),
            F.to_timestamp("h.time").alias("exchange_time"),
            "received_at",
            "_ingested_at",
        )
        .withWatermark("exchange_time", "15 minutes")
        .dropDuplicatesWithinWatermark(["product_id", "exchange_time"])
    )
