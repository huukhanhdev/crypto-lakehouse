"""Phase 1 entrypoint — one SparkSession hosting Bronze + Silver streams.

Running both jobs under a single local-mode driver keeps the laptop RAM budget
in check (one JVM instead of two). The Bronze and Silver logic live in their own
modules (bronze_ingest.py / silver_upsert.py); this file wires the session,
creates the Iceberg namespaces/tables, starts both queries, and blocks.
"""

from __future__ import annotations

import os
import sys

# make sibling modules importable regardless of spark-submit's cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402

import bronze_ingest  # noqa: E402
import common  # noqa: E402
import silver_upsert  # noqa: E402


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("crypto-lakehouse")
        # Iceberg + REST catalog over MinIO (S3FileIO, bundled in the image)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{common.CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{common.CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{common.CATALOG}.uri", os.getenv("REST_URI", "http://rest:8181"))
        .config(f"spark.sql.catalog.{common.CATALOG}.warehouse", "s3://warehouse/")
        .config(f"spark.sql.catalog.{common.CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{common.CATALOG}.s3.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config(f"spark.sql.catalog.{common.CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", common.CATALOG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    common.create_namespaces_and_tables(spark)
    print(">>> namespaces + tables ready", flush=True)

    bronze_q = bronze_ingest.start(spark)
    silver_q = silver_upsert.start(spark)
    print(f">>> streaming started: bronze={bronze_q.id} silver={silver_q.id}", flush=True)

    # block until either query dies; surface its exception if so
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
