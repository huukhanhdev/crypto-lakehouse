-- Current symbol reference data. `status` (TRADING / BREAK / HALT) is the SCD2
-- driver captured by the snap_symbol snapshot.
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
