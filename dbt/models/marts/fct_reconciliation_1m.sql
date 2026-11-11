{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['product_id', 'minute_ts'],
        merge_update_columns=[
            'status_after_repair', 'open_delta', 'high_delta', 'low_delta', 'close_delta',
            'volume_delta_pct', 'stream_trade_count', 'last_reconciled_at'
        ],
        on_schema_change='fail',
    )
}}

-- One row per reconciled product-minute (FR-R1 to FR-R4).
-- status_before_repair is set when a minute is first reconciled and never updated
-- (it is not in merge_update_columns); status_after_repair is refreshed on every run.

with settled as (
    -- FR-R1: reconcile only minutes past the settlement window, and only within days for
    -- which official candles are published, so no minute is first judged before its
    -- official data exists.
    select
        product_id,
        least(
            date_trunc('DAY', max(minute_ts)) + interval 1 day,
            current_timestamp() - interval {{ var('settlement_minutes') }} minutes
        ) as reconcile_until
    from {{ ref('stg_candles_official') }}
    group by product_id
),

candidates as (
    select r.*
    from {{ ref('int_reconciliation_1m') }} as r
    inner join settled as s on r.product_id = s.product_id
    where r.minute_ts >= to_timestamp('{{ var("reconcile_start_date") }}')
      and r.minute_ts < s.reconcile_until
),

{% if is_incremental() %}
already_reconciled as (
    select product_id, minute_ts from {{ this }}
),

in_scope as (
    -- Re-reconcile recent days (repairs, late official corrections) plus any minute not
    -- reconciled yet (e.g. an older day whose official candles were backfilled later).
    select c.*
    from candidates as c
    left join already_reconciled as a
        on c.product_id = a.product_id and c.minute_ts = a.minute_ts
    where c.minute_ts >= current_timestamp() - interval {{ var('reconcile_lookback_days') }} days
       or a.product_id is null
)
{% else %}
in_scope as (
    select * from candidates
)
{% endif %}

select
    product_id,
    minute_ts,
    status as status_before_repair,
    status as status_after_repair,
    cast(stream_open - official_open as decimal(38, 18)) as open_delta,
    cast(stream_high - official_high as decimal(38, 18)) as high_delta,
    cast(stream_low - official_low as decimal(38, 18)) as low_delta,
    cast(stream_close - official_close as decimal(38, 18)) as close_delta,
    cast((stream_volume - official_volume) / official_volume * 100 as double) as volume_delta_pct,
    cast(stream_trade_count as bigint) as stream_trade_count,
    current_timestamp() as first_reconciled_at,
    current_timestamp() as last_reconciled_at
from in_scope
