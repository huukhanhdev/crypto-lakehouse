"""Silver job — cleaned, typed, *current-state* tables via MERGE INTO.

This is the technical centrepiece. Where Bronze keeps every CDC event forever,
Silver keeps only what is true *now*:

  trades           immutable -> insert-only MERGE (dedup on (symbol_id, trade_id))
  orderbook_levels the core   -> MERGE that UPDATEs matched levels, INSERTs new
                                 ones, and DELETEs a level when it empties
                                 (op='d' or quantity=0). One row per live level,
                                 so the table stays *bounded* while Bronze grows.
  symbols/exchanges dims       -> MERGE upsert (current state)

Everything runs in foreachBatch: dedup the micro-batch to the latest row per key,
then a single MERGE. MERGE is keyed and idempotent, so restarting from the
checkpoint never double-applies — Silver has no duplicates.
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import common


def _parse(df, col, schema):
    """Parse a JSON string column into a struct with the given schema."""
    return F.from_json(F.col(col), schema)


def _merge_orderbook(spark, batch):
    a = _parse(batch, "after", common.OB_SCHEMA)
    b = _parse(batch, "before", common.OB_SCHEMA)
    rows = batch.select(
        F.coalesce(a["symbol_id"], b["symbol_id"]).alias("symbol_id"),
        F.coalesce(a["side"], b["side"]).alias("side"),
        F.coalesce(a["price_level"], b["price_level"]).cast(common.DEC).alias("price_level"),
        a["quantity"].cast(common.DEC).alias("quantity"),
        F.coalesce(a["last_update_id"], b["last_update_id"]).alias("last_update_id"),
        common.to_ts(F.coalesce(a["updated_at"], b["updated_at"])).alias("updated_at"),
        (
            (F.col("op") == "d") | (F.coalesce(a["quantity"], F.lit("0")).cast(common.DEC) == 0)
        ).alias("deleted"),
        F.col("kafka_offset"),
    ).filter(F.col("symbol_id").isNotNull() & F.col("price_level").isNotNull())

    # keep the latest event per (symbol, side, price) in this batch
    w = Window.partitionBy("symbol_id", "side", "price_level").orderBy(
        F.col("last_update_id").desc_nulls_last(), F.col("kafka_offset").desc()
    )
    latest = rows.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "kafka_offset")
    # global temp view -> visible regardless of foreachBatch's session cloning
    latest.createOrReplaceGlobalTempView("ob_batch")

    spark.sql(f"""
        MERGE INTO {common.SILVER_NS}.orderbook_levels t
        USING global_temp.ob_batch s
        ON t.symbol_id = s.symbol_id AND t.side = s.side AND t.price_level = s.price_level
        WHEN MATCHED AND s.deleted THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            t.quantity = s.quantity,
            t.last_update_id = s.last_update_id,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED AND NOT s.deleted THEN INSERT
            (symbol_id, side, price_level, quantity, last_update_id, updated_at)
            VALUES (s.symbol_id, s.side, s.price_level, s.quantity, s.last_update_id, s.updated_at)
    """)


def _merge_trades(spark, batch):
    a = _parse(batch, "after", common.TRADES_SCHEMA)
    rows = (
        batch.filter(F.col("op").isin("c", "r"))  # trades are only inserted/snapshotted
        .select(
            a["trade_id"].alias("trade_id"),
            a["symbol_id"].alias("symbol_id"),
            a["price"].cast(common.DEC).alias("price"),
            a["quantity"].cast(common.DEC).alias("quantity"),
            a["quote_qty"].cast(common.DEC).alias("quote_qty"),
            a["is_buyer_maker"].alias("is_buyer_maker"),
            common.to_ts(a["trade_time"]).alias("trade_time"),
            common.to_ts(a["ingested_at"]).alias("ingested_at"),
        )
        .filter(F.col("trade_id").isNotNull())
        .dropDuplicates(["symbol_id", "trade_id"])
    )
    rows.createOrReplaceGlobalTempView("tr_batch")

    # insert-only merge -> idempotent across restarts (no duplicate trades)
    spark.sql(f"""
        MERGE INTO {common.SILVER_NS}.trades t
        USING global_temp.tr_batch s
        ON t.symbol_id = s.symbol_id AND t.trade_id = s.trade_id
        WHEN NOT MATCHED THEN INSERT
            (trade_id, symbol_id, price, quantity, quote_qty, is_buyer_maker, trade_time, ingested_at)
            VALUES (s.trade_id, s.symbol_id, s.price, s.quantity, s.quote_qty,
                    s.is_buyer_maker, s.trade_time, s.ingested_at)
    """)


def _merge_dim(spark, batch, table, schema, key):
    a = _parse(batch, "after", schema)
    b = _parse(batch, "before", schema)
    cols = [f.name for f in schema.fields]
    ts_cols = {"created_at", "updated_at"}
    dec_cols = {"tick_size", "step_size"}

    def col_expr(name):
        c = F.coalesce(a[name], b[name])
        if name in ts_cols:
            return common.to_ts(c).alias(name)
        if name in dec_cols:
            return c.cast(common.DEC).alias(name)
        return c.alias(name)

    rows = batch.select(
        *[col_expr(c) for c in cols],
        (F.col("op") == "d").alias("deleted"),
        F.col("kafka_offset"),
    ).filter(F.col(key).isNotNull())

    w = Window.partitionBy(key).orderBy(F.col("kafka_offset").desc())
    latest = rows.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "kafka_offset")
    latest.createOrReplaceGlobalTempView("dim_batch")

    set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)
    spark.sql(f"""
        MERGE INTO {common.SILVER_NS}.{table} t
        USING global_temp.dim_batch s
        ON t.{key} = s.{key}
        WHEN MATCHED AND s.deleted THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND NOT s.deleted THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


def _write_silver():
    def _fn(batch_df, _epoch_id):
        # foreachBatch hands us a batch bound to a *cloned* session; temp views
        # must be registered and queried on that same session, not the outer one.
        spark = batch_df.sparkSession
        batch = batch_df.persist()
        try:
            if batch.take(1):
                ob = batch.filter(F.col("src_table") == "orderbook_levels")
                if ob.take(1):
                    _merge_orderbook(spark, ob)
                tr = batch.filter(F.col("src_table") == "trades")
                if tr.take(1):
                    _merge_trades(spark, tr)
                sy = batch.filter(F.col("src_table") == "symbols")
                if sy.take(1):
                    _merge_dim(spark, sy, "symbols", common.SYMBOLS_SCHEMA, "symbol_id")
                ex = batch.filter(F.col("src_table") == "exchanges")
                if ex.take(1):
                    _merge_dim(spark, ex, "exchanges", common.EXCHANGES_SCHEMA, "exchange_id")
        finally:
            batch.unpersist()

    return _fn


def start(spark):
    """Register the Silver streaming query and return it."""
    decoded = common.read_cdc_stream(spark)
    return (
        decoded.writeStream
        .foreachBatch(_write_silver())
        .option("checkpointLocation", f"{common.CHECKPOINT_ROOT}/silver")
        .trigger(processingTime="15 seconds")
        .queryName("silver")
        .start()
    )
