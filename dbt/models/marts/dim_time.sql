-- Minute-of-day dim (1440 rows), for intraday charts. time_key = minutes since
-- midnight, conformed with fct_trades / fct_ohlcv_1m.
with minutes as (
    select m
    from unnest(sequence(0, 1439)) as t(m)
)
select
    m                                as time_key,      -- 0..1439
    cast(m / 60 as integer)          as hour,
    cast(m % 60 as integer)          as minute,
    format('%02d:%02d', m / 60, m % 60) as hh_mm,
    case when m < 12 * 60 then 'AM' else 'PM' end as am_pm
from minutes
