"""quarantine.ws_rejected: messages the producer rejected (DQ-01, PRD §6.3).

The producer writes messages that fail its schema check to the `_rejected` channel with
the validation error; they are kept here with the reason, never dropped silently.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
LANDING_PATH = spark.conf.get("landing_path")


@dp.table(
    name="quarantine.ws_rejected",
    comment="WebSocket messages rejected by the producer's schema check (DQ-01).",
    table_properties={"quality": "quarantine"},
)
def ws_rejected():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "rejected_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema("received_at STRING, raw STRING, error STRING")
        .load(f"{LANDING_PATH}/ws/")
        .select(
            F.col("raw").alias("payload"),
            F.to_timestamp("received_at").alias("received_at"),
            F.concat(F.lit("dq01_schema: "), F.col("error")).alias("rule_failed"),
            F.current_timestamp().alias("_quarantined_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            "_rescued_data",
        )
    )
