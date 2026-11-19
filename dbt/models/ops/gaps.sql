{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='gap_id',
        merge_update_columns=['present_count', 'status', 'repaired_at', 'last_checked_at'],
        on_schema_change='fail',
    )
}}

-- FR-J2, ADR-002, ADR-013: one row per gap in trade ids per product.
-- Detection scans the last gap_lookback_days of trades. Every run also re-checks the
-- gaps that are still open: a gap is repaired once all of its ids are in silver.trades.
-- A newly detected break inside a known open gap (a partial repair) is not a new gap.

with recent_trades as (
    select product_id, trade_id, exchange_time
    from {{ ref('stg_trades') }}
    where exchange_time >= current_timestamp() - interval {{ var('gap_lookback_days') }} days
),

steps as (
    select
        product_id,
        lag(trade_id) over (partition by product_id order by trade_id) as prev_id,
        lag(exchange_time) over (partition by product_id order by trade_id) as prev_time,
        trade_id as next_id,
        exchange_time as next_time
    from recent_trades
),

detected as (
    select
        md5(concat_ws('|', product_id, cast(prev_id + 1 as string), cast(next_id - 1 as string)))
            as gap_id,
        product_id,
        prev_id + 1 as from_trade_id,
        next_id - 1 as to_trade_id,
        next_id - prev_id - 1 as missing_count,
        prev_time as gap_start_time,
        next_time as gap_end_time,
        current_timestamp() as detected_at
    from steps
    where next_id - prev_id > 1
),

{% if is_incremental() %}
known as (
    select * from {{ this }}
),

new_gaps as (
    -- Skip breaks that fall inside a gap we already track (e.g. partly repaired).
    select d.*
    from detected as d
    left join known as k
        on d.product_id = k.product_id
        and d.from_trade_id >= k.from_trade_id
        and d.to_trade_id <= k.to_trade_id
    where k.gap_id is null
),

candidates as (
    select
        gap_id, product_id, from_trade_id, to_trade_id, missing_count,
        gap_start_time, gap_end_time, detected_at
    from known
    where status = 'open'
    union all
    select * from new_gaps
),
{% else %}
candidates as (
    select * from detected
),
{% endif %}

present as (
    select c.gap_id, count(distinct t.trade_id) as present_count
    from candidates as c
    inner join {{ ref('stg_trades') }} as t
        on t.product_id = c.product_id
        and t.trade_id between c.from_trade_id and c.to_trade_id
        and t.exchange_time between c.gap_start_time and c.gap_end_time
    group by c.gap_id
)

select
    c.gap_id,
    c.product_id,
    cast(c.from_trade_id as bigint) as from_trade_id,
    cast(c.to_trade_id as bigint) as to_trade_id,
    cast(c.missing_count as bigint) as missing_count,
    c.gap_start_time,
    c.gap_end_time,
    c.detected_at,
    cast(coalesce(p.present_count, 0) as bigint) as present_count,
    case
        when coalesce(p.present_count, 0) >= c.missing_count then 'repaired'
        else 'open'
    end as status,
    case
        when coalesce(p.present_count, 0) >= c.missing_count then current_timestamp()
    end as repaired_at,
    current_timestamp() as last_checked_at
from candidates as c
left join present as p on c.gap_id = p.gap_id
