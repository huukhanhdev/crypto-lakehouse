{% snapshot snap_symbol %}
{{
  config(
    unique_key='symbol_id',
    strategy='check',
    check_cols=['status'],
    target_schema='gold'
  )
}}
-- SCD2 on symbol status. `check` compares `status` between runs; a change closes
-- the old version (dbt_valid_to set) and opens a new one. Flipping a symbol's
-- status in Postgres and re-running Silver + `dbt snapshot` yields a 2nd version.
select
    symbol_id,
    symbol,
    base_asset,
    quote_asset,
    status,
    tick_size,
    step_size,
    updated_at
from {{ source('silver', 'symbols') }}
{% endsnapshot %}
