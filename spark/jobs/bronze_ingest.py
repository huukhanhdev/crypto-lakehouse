"""Bronze job — raw CDC envelopes, append-only.

Reads every `crypto.public.*` topic from Redpanda and appends the raw Debezium
envelope (op, ts_ms, lsn, Kafka coordinates, and the verbatim after/before JSON)
into one Iceberg table per source table. Nothing is deduped or updated here —
Bronze is the immutable log the rest of the lakehouse can be rebuilt from.

Note: foreachBatch gives at-least-once semantics, so a rare batch retry could
duplicate raw rows in Bronze. That is acceptable for a raw landing tier (Silver
is the deduped/current tier); see docs/DECISIONS.md.
"""

from __future__ import annotations

from pyspark.sql import functions as F

import common


def _write_bronze(batch_df, _epoch_id):
    # date derived from the CDC event time; used as the ingestion-date partition
    rows = batch_df.withColumn(
        "ingest_date", F.to_date(F.timestamp_millis(F.col("ts_ms")))
    ).select(
        "op", "ts_ms", "source_lsn", "symbol_id",
        "kafka_topic", "kafka_partition", "kafka_offset",
        "ingest_date", "after", "before", "src_table",
    )
    rows = rows.persist()
    try:
        for t in common.TABLES:
            part = rows.filter(F.col("src_table") == t).drop("src_table")
            if part.take(1):  # skip empty appends (no wasteful snapshots)
                part.writeTo(f"{common.BRONZE_NS}.{t}").append()
    finally:
        rows.unpersist()


def start(spark):
    """Register the Bronze streaming query and return it."""
    decoded = common.read_cdc_stream(spark)
    return (
        decoded.writeStream
        .foreachBatch(_write_bronze)
        .option("checkpointLocation", f"{common.CHECKPOINT_ROOT}/bronze")
        .trigger(processingTime="10 seconds")
        .queryName("bronze")
        .start()
    )
