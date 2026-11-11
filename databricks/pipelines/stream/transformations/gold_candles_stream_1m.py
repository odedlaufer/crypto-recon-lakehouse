"""gold.candles_stream_1m: 1-minute candles built from silver.trades (FR-S4, ADR-009).

A materialized view, recomputed on each pipeline update, so trades added later by gap
repair (FR-J3) automatically correct the affected minutes. Per product and minute:
open = price of the lowest trade_id, close = price of the highest, plus high, low,
volume (sum of size), trade_count and the first and last trade_id.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.materialized_view(
    name="gold.candles_stream_1m",
    comment="1-minute OHLCV candles built from silver.trades (stream and backfill).",
    table_properties={"quality": "gold"},
    cluster_by=["product_id", "minute_ts"],
)
def candles_stream_1m():
    return (
        spark.read.table("silver.trades")
        # Safety net: a trade counted once even if it reached silver twice.
        .dropDuplicates(["product_id", "trade_id"])
        .groupBy("product_id", F.date_trunc("minute", "exchange_time").alias("minute_ts"))
        .agg(
            F.min_by("price", "trade_id").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "trade_id").alias("close"),
            F.sum("size").alias("volume"),
            F.count("*").alias("trade_count"),
            F.min("trade_id").alias("first_trade_id"),
            F.max("trade_id").alias("last_trade_id"),
        )
    )
