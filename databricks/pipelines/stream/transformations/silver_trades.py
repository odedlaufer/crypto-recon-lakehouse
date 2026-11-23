"""silver.trades and quarantine.trades (FR-S2, FR-S3, FR-S5, PRD §6.1, §6.3).

Every trade from bronze is parsed and typed once, then checked against the trade rules:
- silver.trades keeps the rows that pass every rule (expectations drop the others), then
  deduplicates on (product_id, trade_id) within a 15-minute watermark on exchange_time.
- quarantine.trades keeps the rows that fail any rule, with the failed rules listed in
  `rule_failed`, so no row is removed without a record of why (§6).

`last_match` is treated as a normal trade. Prices and sizes are parsed from their JSON
strings into DECIMAL(38, 18) so no precision is lost.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

MATCH_SCHEMA = (
    "type STRING, trade_id BIGINT, sequence BIGINT, maker_order_id STRING, "
    "taker_order_id STRING, time STRING, product_id STRING, size STRING, price STRING, "
    "side STRING"
)
DECIMAL = "DECIMAL(38, 18)"
ALLOWED_PRODUCTS = [p.strip() for p in spark.conf.get("allowed_products").split(",")]
_ALLOWED_SQL = ", ".join(f"'{p}'" for p in ALLOWED_PRODUCTS)

# Rule name -> condition that a valid trade satisfies (PRD §6.1).
TRADE_RULES = {
    "parsed": "trade_id IS NOT NULL AND exchange_time IS NOT NULL "
    "AND price IS NOT NULL AND size IS NOT NULL",
    "dq05_price_size_positive": "price > 0 AND size > 0",
    "dq06_side_valid": "side IN ('buy', 'sell')",
    "dq07_not_in_future": "exchange_time <= received_at + INTERVAL 5 SECONDS",
    "dq08_product_allowed": f"product_id IN ({_ALLOWED_SQL})",
}


def _parsed_trades() -> DataFrame:
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
    )


@dp.table(
    name="silver.trades",
    comment="One row per Coinbase trade; source = stream or backfill.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(TRADE_RULES)
def trades():
    # A future trade time would advance the watermark and make later real trades look late,
    # so the dedupe would drop them. Remove such rows before the watermark; quarantine.trades
    # still records them under dq07_not_in_future.
    return (
        _parsed_trades()
        .where(f"exchange_time IS NULL OR ({TRADE_RULES['dq07_not_in_future']})")
        .withWatermark("exchange_time", "15 minutes")
        .dropDuplicatesWithinWatermark(["product_id", "trade_id"])
    )


@dp.table(
    name="quarantine.trades",
    comment="Trades rejected by a rule in PRD §6.1; rule_failed lists the failed rules.",
    table_properties={"quality": "quarantine"},
)
def quarantine_trades():
    failed = [
        F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.lit(name))
        for name, condition in TRADE_RULES.items()
    ]
    return (
        _parsed_trades()
        .withColumn("rule_failed", F.concat_ws(",", *failed))
        .where(F.col("rule_failed") != "")
        .withColumn("_quarantined_at", F.current_timestamp())
    )
