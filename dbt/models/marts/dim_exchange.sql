select
    exchange_id as exchange_key,
    exchange_id,
    name        as exchange_name,
    region
from {{ ref('stg_exchanges') }}
