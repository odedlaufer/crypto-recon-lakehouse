"""bronze.ws_events: one row per raw WebSocket message, as landed by the producer (FR-S1).

Auto Loader reads the producer's JSONL files from the landing Volume. Each line is
{"received_at": ..., "raw": "<message exactly as received>"}; `raw` becomes `payload`.
Only the matches and heartbeat channels are read here; rejected messages and producer
metrics land under their own channels and are loaded separately.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
LANDING_PATH = spark.conf.get("landing_path")


@dp.table(
    name="bronze.ws_events",
    comment="One row per raw Coinbase WebSocket message (matches, heartbeat).",
    table_properties={"quality": "bronze"},
)
def ws_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "{matches,heartbeat}_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema("received_at STRING, raw STRING")
        .load(f"{LANDING_PATH}/ws/")
        .select(
            F.col("raw").alias("payload"),
            F.regexp_extract("_metadata.file_path", r"/ws/([^/]+)/", 1).alias("channel"),
            F.to_timestamp("received_at").alias("received_at"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_rescued_data"),
        )
    )
