-- Official 1-minute candles, one row per product-minute.
select
    product_id,
    minute_ts,
    open,
    high,
    low,
    close,
    volume
from {{ source('silver', 'candles_official') }}
