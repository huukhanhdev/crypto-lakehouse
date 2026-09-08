-- One OHLCV bar per (symbol, minute).
select symbol_id, bar_minute, count(*) as n
from {{ ref('fct_ohlcv_1m') }}
group by symbol_id, bar_minute
having count(*) > 1
