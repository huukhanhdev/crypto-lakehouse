-- The current, bounded order book maintained by Silver's MERGE INTO (only live
-- price levels survive). `side` is 'bid' or 'ask'.
select
    symbol_id,
    lower(side) as side,
    price_level,
    quantity,
    last_update_id,
    updated_at
from {{ source('silver', 'orderbook_levels') }}
