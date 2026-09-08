-- Calendar dim spanning the range of observed trades. date_key is YYYYMMDD int,
-- conformed with fct_trades / fct_ohlcv_1m.
with bounds as (
    select
        cast(min(trade_time) as date) as d0,
        cast(max(trade_time) as date) as d1
    from {{ ref('stg_trades') }}
),
days as (
    select d
    from bounds
    cross join unnest(sequence(bounds.d0, bounds.d1, interval '1' day)) as t(d)
)
select
    cast(date_format(d, '%Y%m%d') as integer) as date_key,
    d                                          as date_day,
    year(d)                                    as year,
    month(d)                                   as month,
    day(d)                                     as day_of_month,
    day_of_week(d)                             as day_of_week,   -- 1=Mon..7=Sun
    date_format(d, '%W')                       as day_name,
    week(d)                                    as week_of_year
from days
