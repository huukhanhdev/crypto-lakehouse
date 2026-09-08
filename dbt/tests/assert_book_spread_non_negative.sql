-- A well-formed book never has the ask below the bid. Any row here fails.
select symbol_id, best_bid, best_ask, spread
from {{ ref('fct_book_snapshot') }}
where spread < 0
