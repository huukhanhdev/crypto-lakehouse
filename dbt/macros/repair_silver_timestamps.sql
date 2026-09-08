{#
  One-off Phase-1 data repair. The Silver Spark job originally parsed the
  Debezium `timestamptz` columns as epoch-millis longs, but Debezium emits them
  as ISO-8601 ZonedTimestamp *strings* — so every timestamp landed NULL in Silver.

  The Spark code is now fixed (common.to_ts). This macro rebuilds silver.trades in
  place from Bronze (the replayable raw log) via Trino, re-deriving the timestamps
  with from_iso8601_timestamp — no Spark run, minimal disk. It also compacts the
  ~2.3k tiny streaming files into a handful and preserves the partition spec.

  A GROUP BY + max_by(...) dedup (keep the latest CDC row per key) is used instead
  of a window function so the 3.9M-row rebuild fits Trino's memory.

  Only silver.trades is rebuilt: the Gold layer needs trade timestamps for OHLCV.
  Orderbook uses last_update_id (already populated) for freshness, and the dims
  don't carry timestamps into Gold — so those tables need no repair.

  Run once:  dbt run-operation repair_silver_timestamps
#}
{% macro repair_silver_timestamps() %}

  {% set trades_sql %}
    create or replace table iceberg.silver.trades
    with (partitioning = array['day(trade_time)', 'symbol_id'])
    as
    select
      trade_id,
      symbol_id,
      max_by(price, kafka_offset)          as price,
      max_by(quantity, kafka_offset)       as quantity,
      max_by(quote_qty, kafka_offset)      as quote_qty,
      max_by(is_buyer_maker, kafka_offset) as is_buyer_maker,
      max_by(trade_time, kafka_offset)     as trade_time,
      max_by(ingested_at, kafka_offset)    as ingested_at
    from (
      select
        cast(json_extract_scalar(after, '$.trade_id')  as bigint)         as trade_id,
        cast(json_extract_scalar(after, '$.symbol_id') as bigint)         as symbol_id,
        cast(json_extract_scalar(after, '$.price')     as decimal(38,18)) as price,
        cast(json_extract_scalar(after, '$.quantity')  as decimal(38,18)) as quantity,
        cast(json_extract_scalar(after, '$.quote_qty') as decimal(38,18)) as quote_qty,
        cast(json_extract_scalar(after, '$.is_buyer_maker') as boolean)   as is_buyer_maker,
        cast(from_iso8601_timestamp(json_extract_scalar(after, '$.trade_time'))
             as timestamp(6) with time zone)                             as trade_time,
        cast(from_iso8601_timestamp(json_extract_scalar(after, '$.ingested_at'))
             as timestamp(6) with time zone)                             as ingested_at,
        kafka_offset
      from iceberg.bronze.trades
      where after is not null and op in ('c', 'r')
    )
    group by symbol_id, trade_id
  {% endset %}
  {% do run_query(trades_sql) %}
  {{ log("rebuilt silver.trades from Bronze with correct timestamps", info=True) }}

{% endmacro %}
