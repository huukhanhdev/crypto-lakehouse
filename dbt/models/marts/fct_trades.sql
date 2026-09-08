-- Grain: one row per trade. Conformed date_key / time_key are computed inline
-- (no join needed — the trade already carries its timestamp).
select
    t.trade_id,
    t.symbol_id,
    cast(date_format(t.trade_time, '%Y%m%d') as integer)      as date_key,
    (hour(t.trade_time) * 60 + minute(t.trade_time))          as time_key,
    t.price,
    t.quantity,
    t.quote_qty,
    t.taker_side,
    t.is_buyer_maker,
    t.trade_time
from {{ ref('stg_trades') }} t
