-- Binance trade ids are unique per symbol, not globally — enforce the composite.
select symbol_id, trade_id, count(*) as n
from {{ ref('fct_trades') }}
group by symbol_id, trade_id
having count(*) > 1
