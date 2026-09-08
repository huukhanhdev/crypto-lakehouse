-- Grain: symbol × 1-minute bar. Classic OHLCV rolled up from fct_trades.
-- open/close use min_by/max_by on trade_time so they reflect the first/last
-- print in the minute regardless of arrival order.
with base as (
    select
        symbol_id,
        date_trunc('minute', trade_time) as bar_minute,
        price,
        quantity,
        quote_qty,
        trade_time
    from {{ ref('fct_trades') }}
)
select
    symbol_id,
    bar_minute,
    cast(date_format(bar_minute, '%Y%m%d') as integer)      as date_key,
    (hour(bar_minute) * 60 + minute(bar_minute))            as time_key,
    min_by(price, trade_time)  as open,
    max(price)                 as high,
    min(price)                 as low,
    max_by(price, trade_time)  as close,
    sum(quantity)              as volume,
    sum(quote_qty)             as quote_volume,
    count(*)                   as trade_count
from base
group by symbol_id, bar_minute
