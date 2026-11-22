"""gold.candles_stream_1m: 1-minute candles built from all known trades (FR-S4, ADR-009).

A materialized view over silver.trades (stream) and silver.trades_backfill (REST repair,
ADR-014), recomputed on each pipeline update, so backfilled trades correct the affected
minutes automatically (FR-J3). Per product and minute: open = price of the lowest
trade_id, close = price of the highest, plus high, low, volume (sum of size), trade_count
and the first and last trade_id.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

TRADE_COLUMNS = ["product_id", "trade_id", "price", "size", "exchange_time"]


@dp.materialized_view(
    name="gold.candles_stream_1m",
    comment="1-minute OHLCV candles built from stream and backfilled trades.",
    table_properties={"quality": "gold"},
    cluster_by=["product_id", "minute_ts"],
)
@dp.expect_all_or_drop(
    {
        "dq15_trade_ids_ordered": "first_trade_id <= last_trade_id",
        "dq15_has_trades": "trade_count > 0",
    }
)
def candles_stream_1m():
    stream = spark.read.table("silver.trades").select(*TRADE_COLUMNS)
    backfill = spark.read.table("silver.trades_backfill").select(*TRADE_COLUMNS)
    return (
        stream.unionByName(backfill)
        # A trade counts once, whether it arrived by stream, backfill or both.
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
