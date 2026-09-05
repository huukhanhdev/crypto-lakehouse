"""Shared config, schemas, DDL, and Debezium decoding for the Spark jobs.

Both the Bronze and Silver jobs run inside one SparkSession (local mode) to fit
laptop RAM — see docs/DECISIONS.md. This module holds what they share.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --- catalog / topic constants ---------------------------------------------
CATALOG = "lake"
BRONZE_NS = f"{CATALOG}.bronze"
SILVER_NS = f"{CATALOG}.silver"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC_PATTERN = r"crypto\.public\..*"        # the 4 CDC topics, not heartbeats
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/app/checkpoints")

# source tables carried through the pipeline
TABLES = ["symbols", "exchanges", "trades", "orderbook_levels"]
FACT_TABLES = ["trades", "orderbook_levels"]  # bronze partitions these by symbol

DEC = "decimal(38,18)"

# --- after/before payload schemas (Debezium, schemas.enable=false) ----------
# decimal.handling.mode=string  -> numerics arrive as strings
# time.precision.mode=connect   -> timestamps arrive as epoch millis (long)
TRADES_SCHEMA = StructType([
    StructField("trade_id", LongType()),
    StructField("symbol_id", LongType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
    StructField("quote_qty", StringType()),
    StructField("is_buyer_maker", BooleanType()),
    StructField("trade_time", LongType()),
    StructField("ingested_at", LongType()),
])

OB_SCHEMA = StructType([
    StructField("symbol_id", LongType()),
    StructField("side", StringType()),
    StructField("price_level", StringType()),
    StructField("quantity", StringType()),
    StructField("last_update_id", LongType()),
    StructField("updated_at", LongType()),
])

SYMBOLS_SCHEMA = StructType([
    StructField("symbol_id", LongType()),
    StructField("symbol", StringType()),
    StructField("base_asset", StringType()),
    StructField("quote_asset", StringType()),
    StructField("status", StringType()),
    StructField("tick_size", StringType()),
    StructField("step_size", StringType()),
    StructField("created_at", LongType()),
    StructField("updated_at", LongType()),
])

EXCHANGES_SCHEMA = StructType([
    StructField("exchange_id", LongType()),
    StructField("name", StringType()),
    StructField("region", StringType()),
    StructField("updated_at", LongType()),
])


# --- DDL --------------------------------------------------------------------
def create_namespaces_and_tables(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NS}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NS}")

    # Bronze: uniform raw-envelope schema, one table per source table.
    for t in TABLES:
        part = "(ingest_date, symbol_id)" if t in FACT_TABLES else "(ingest_date)"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_NS}.{t} (
                op              string,
                ts_ms           bigint,
                source_lsn      bigint,
                symbol_id       bigint,
                kafka_topic     string,
                kafka_partition int,
                kafka_offset    bigint,
                ingest_date     date,
                after           string,
                before          string
            ) USING iceberg PARTITIONED BY {part}
        """)

    # Silver: cleaned, typed, current-state.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_NS}.trades (
            trade_id       bigint,
            symbol_id      bigint,
            price          {DEC},
            quantity       {DEC},
            quote_qty      {DEC},
            is_buyer_maker boolean,
            trade_time     timestamp,
            ingested_at    timestamp
        ) USING iceberg PARTITIONED BY (days(trade_time), symbol_id)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_NS}.orderbook_levels (
            symbol_id      bigint,
            side           string,
            price_level    {DEC},
            quantity       {DEC},
            last_update_id bigint,
            updated_at     timestamp
        ) USING iceberg PARTITIONED BY (symbol_id, side)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_NS}.symbols (
            symbol_id   bigint,
            symbol      string,
            base_asset  string,
            quote_asset string,
            status      string,
            tick_size   {DEC},
            step_size   {DEC},
            created_at  timestamp,
            updated_at  timestamp
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_NS}.exchanges (
            exchange_id bigint,
            name        string,
            region      string,
            updated_at  timestamp
        ) USING iceberg
    """)


# --- Debezium decode --------------------------------------------------------
def decode(df: DataFrame) -> DataFrame:
    """Turn a raw Kafka stream into common CDC columns.

    Keeps `after`/`before` as raw JSON strings (Bronze stores them verbatim;
    Silver parses them per-table with the schemas above). Drops tombstones.
    """
    v = F.col("value").cast("string")
    return (
        df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            v.alias("value"),
        )
        .filter(F.col("value").isNotNull())  # drop delete tombstones
        .withColumn("src_table", F.regexp_replace("kafka_topic", r"^crypto\.public\.", ""))
        .withColumn("op", F.get_json_object("value", "$.op"))
        .withColumn("ts_ms", F.get_json_object("value", "$.ts_ms").cast("bigint"))
        .withColumn("source_lsn", F.get_json_object("value", "$.source.lsn").cast("bigint"))
        .withColumn("after", F.get_json_object("value", "$.after"))
        .withColumn("before", F.get_json_object("value", "$.before"))
        .withColumn(
            "symbol_id",
            F.coalesce(
                F.get_json_object("value", "$.after.symbol_id"),
                F.get_json_object("value", "$.before.symbol_id"),
            ).cast("bigint"),
        )
    )


def read_cdc_stream(spark, starting_offsets: str = None, max_per_trigger: int = 20000):
    """Read the CDC topics from Redpanda as a decoded streaming DataFrame.

    `starting_offsets` defaults to $STARTING_OFFSETS (env), else "earliest" so a
    fresh run backfills the whole log. Set STARTING_OFFSETS=latest to smoke-test
    against only live data without replaying millions of historical events.
    """
    if starting_offsets is None:
        starting_offsets = os.getenv("STARTING_OFFSETS", "earliest")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_per_trigger)
        .load()
    )
    return decode(raw)
