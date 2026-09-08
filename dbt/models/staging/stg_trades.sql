-- One row per trade, typed and with the taker side derived.
-- Binance `is_buyer_maker=true` => the buyer sat on the book, so the taker (the
-- aggressor that moved the tape) was the seller => a "sell" print.
select
    trade_id,
    symbol_id,
    price,
    quantity,
    quote_qty,
    is_buyer_maker,
    case when is_buyer_maker then 'sell' else 'buy' end as taker_side,
    trade_time
from {{ source('silver', 'trades') }}
