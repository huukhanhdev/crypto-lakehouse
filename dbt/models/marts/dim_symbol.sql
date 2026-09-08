-- SCD2 symbol dimension, surfaced from the snapshot with friendly validity cols.
-- One row per (symbol_id, version); is_current flags the live version.
select
    dbt_scd_id      as symbol_version_key,   -- surrogate key per version
    symbol_id,                               -- natural key
    symbol,
    base_asset,
    quote_asset,
    status,
    tick_size,
    step_size,
    dbt_valid_from  as valid_from,
    dbt_valid_to    as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_symbol') }}
