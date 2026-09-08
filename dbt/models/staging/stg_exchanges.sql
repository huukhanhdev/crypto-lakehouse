select
    exchange_id,
    name,
    region,
    updated_at
from {{ source('silver', 'exchanges') }}
