-- Stream vs official candles per product-minute (FR-R2), with a status (FR-R3).
-- Every minute either side has; scope and history are applied in fct_reconciliation_1m.
with joined as (
    select
        coalesce(s.product_id, o.product_id) as product_id,
        coalesce(s.minute_ts, o.minute_ts) as minute_ts,
        s.minute_ts is not null as has_stream,
        o.minute_ts is not null as has_official,
        s.open as stream_open,
        s.high as stream_high,
        s.low as stream_low,
        s.close as stream_close,
        s.volume as stream_volume,
        s.trade_count as stream_trade_count,
        o.open as official_open,
        o.high as official_high,
        o.low as official_low,
        o.close as official_close,
        o.volume as official_volume
    from {{ ref('stg_candles_stream') }} as s
    full outer join {{ ref('stg_candles_official') }} as o
        on s.product_id = o.product_id
        and s.minute_ts = o.minute_ts
)

select
    *,
    {{ reconciliation_status() }} as status
from joined
