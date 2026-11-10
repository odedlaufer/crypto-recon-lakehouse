"""silver.trades: one row per trade, parsed, typed and deduplicated (FR-S2, FR-S3).

`last_match` is treated as a normal trade. Duplicates on (product_id, trade_id) are
dropped within a 15-minute watermark on exchange_time. Prices and sizes are parsed from
their JSON strings into DECIMAL(38, 18) so no precision is lost.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

MATCH_SCHEMA = (
    "type STRING, trade_id BIGINT, sequence BIGINT, maker_order_id STRING, "
    "taker_order_id STRING, time STRING, product_id STRING, size STRING, price STRING, "
    "side STRING"
)
DECIMAL = "DECIMAL(38, 18)"


@dp.table(
    name="silver.trades",
    comment="One row per Coinbase trade; source = stream or backfill.",
    table_properties={"quality": "silver"},
)
def trades():
    return (
        spark.readStream.table("bronze.ws_events")
        .where(F.col("channel") == "matches")
        .select(
            F.from_json("payload", MATCH_SCHEMA).alias("m"),
            "received_at",
            "_ingested_at",
            "_source_file",
        )
        .select(
            F.col("m.product_id").alias("product_id"),
            F.col("m.trade_id").alias("trade_id"),
            F.col("m.price").cast(DECIMAL).alias("price"),
            F.col("m.size").cast(DECIMAL).alias("size"),
            F.col("m.side").alias("side"),
            F.to_timestamp("m.time").alias("exchange_time"),
            F.col("m.sequence").alias("sequence"),
            F.col("m.type").alias("message_type"),
            F.lit("stream").alias("source"),
            "received_at",
            "_ingested_at",
            "_source_file",
        )
        .withWatermark("exchange_time", "15 minutes")
        .dropDuplicatesWithinWatermark(["product_id", "trade_id"])
    )
