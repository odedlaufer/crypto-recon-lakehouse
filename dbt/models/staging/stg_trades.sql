-- Every known trade: stream plus REST backfill (ADR-014). A trade can appear in both;
-- consumers deduplicate on (product_id, trade_id).
select product_id, trade_id, exchange_time, price, size, 'stream' as source
from {{ source('silver', 'trades') }}

union all

select product_id, trade_id, exchange_time, price, size, 'backfill' as source
from {{ source('silver', 'trades_backfill') }}
