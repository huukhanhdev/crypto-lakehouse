-- SCD2 validity windows must not overlap: a version's valid_to may equal the
-- next version's valid_from (contiguous) but never exceed it.
with v as (
    select
        symbol_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by symbol_id order by valid_from
        ) as next_valid_from
    from {{ ref('dim_symbol') }}
)
select symbol_id, valid_from, valid_to, next_valid_from
from v
where valid_to is not null
  and next_valid_from is not null
  and valid_to > next_valid_from
