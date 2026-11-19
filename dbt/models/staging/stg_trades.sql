-- Trades from the stream (and, after Phase 3 repair, from backfill), one row per trade.
select
    product_id,
    trade_id,
    exchange_time
from {{ source('silver', 'trades') }}
