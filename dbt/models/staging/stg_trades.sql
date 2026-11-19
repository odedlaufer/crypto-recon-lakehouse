-- Every known trade: stream plus REST backfill (ADR-014). A trade can appear in both;
-- consumers count distinct (product_id, trade_id).
select product_id, trade_id, exchange_time, 'stream' as source
from {{ source('silver', 'trades') }}

union all

select product_id, trade_id, exchange_time, 'backfill' as source
from {{ source('silver', 'trades_backfill') }}
