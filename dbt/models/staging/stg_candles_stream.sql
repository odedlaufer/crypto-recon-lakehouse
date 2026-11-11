-- Stream-built 1-minute candles, one row per product-minute (single candle builder, ADR-009).
select
    product_id,
    minute_ts,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    first_trade_id,
    last_trade_id
from {{ source('gold', 'candles_stream_1m') }}
