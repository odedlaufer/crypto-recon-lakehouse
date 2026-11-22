"""ops.producer_metrics: one row per producer metrics window (FR-O2, FR-P6).

The producer writes a record every 30 seconds to the `_metrics` channel; Auto Loader
reads those files from the landing Volume.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
LANDING_PATH = spark.conf.get("landing_path")

METRICS_SCHEMA = (
    "run_id STRING, window_start STRING, window_end STRING, window_seconds DOUBLE, "
    "messages BIGINT, msgs_per_sec DOUBLE, landed BIGINT, rejected BIGINT, "
    "reconnects BIGINT, trade_gaps BIGINT, heartbeat_gaps BIGINT, "
    "latency_p50_ms DOUBLE, latency_p95_ms DOUBLE"
)


@dp.table(
    name="ops.producer_metrics",
    comment="One row per producer metrics window: throughput, latency, reconnects, gaps.",
    table_properties={"quality": "ops"},
)
def producer_metrics():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "metrics_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(METRICS_SCHEMA)
        .load(f"{LANDING_PATH}/ws/")
        .select(
            "run_id",
            F.to_timestamp("window_start").alias("window_start"),
            F.to_timestamp("window_end").alias("window_end"),
            "window_seconds",
            "messages",
            "msgs_per_sec",
            "landed",
            "rejected",
            "reconnects",
            "trade_gaps",
            "heartbeat_gaps",
            "latency_p50_ms",
            "latency_p95_ms",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            "_rescued_data",
        )
    )
