-- Exactly one live version per symbol in the SCD2 dim.
select symbol_id, count(*) as n_current
from {{ ref('dim_symbol') }}
where is_current
group by symbol_id
having count(*) <> 1
