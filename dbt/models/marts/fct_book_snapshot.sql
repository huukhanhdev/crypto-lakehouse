-- Grain: one row per symbol = the current top-of-book, derived from the bounded
-- live order book (Silver's MERGE INTO output).
--
-- The book is maintained from Binance depth *diffs* without periodic re-snapshot,
-- so far-from-mid levels can go stale and even cross (a known limitation the spec
-- flags). Taking min(ask)/max(bid) over ALL levels would therefore report a
-- crossed spread. Instead we take the *most recently updated* level on each side
-- via max_by(price, last_update_id) — the coherent instantaneous top-of-book,
-- which yields a non-negative spread (dbt-tested).
with book as (
    select symbol_id, side, price_level, last_update_id
    from {{ ref('stg_orderbook_levels') }}
),
agg as (
    select
        symbol_id,
        max(last_update_id) as book_version,
        max_by(
            case when side = 'bid' then price_level end,
            case when side = 'bid' then last_update_id end
        ) as best_bid,
        max_by(
            case when side = 'ask' then price_level end,
            case when side = 'ask' then last_update_id end
        ) as best_ask,
        count(case when side = 'bid' then 1 end) as bid_levels,
        count(case when side = 'ask' then 1 end) as ask_levels
    from book
    group by symbol_id
)
select
    symbol_id,
    book_version,
    best_bid,
    best_ask,
    best_ask - best_bid as spread,
    case
        when best_bid > 0
        then cast(best_ask - best_bid as double) / cast(best_bid as double) * 100
    end as spread_pct,
    bid_levels,
    ask_levels
from agg
