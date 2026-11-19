{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='gap_id',
        merge_update_columns=['present_count', 'status', 'repaired_at', 'last_checked_at'],
        on_schema_change='fail',
    )
}}

-- FR-J2, FR-J3, ADR-002, ADR-013: one row per gap in trade ids per product.
-- Detection scans the last gap_lookback_days of trades. Every run also re-checks the
-- gaps that are still open:
--   repaired      all of the gap's ids are now known (stream or backfill)
--   unrepairable  the latest backfill attempt found ids missing from REST, and every id
--                 REST did return is already loaded
--   open          otherwise
-- A newly detected break inside a known gap (a partial repair) is not a new gap.

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

latest_attempt as (
    select gap_id, received, missing
    from (
        select
            gap_id,
            received,
            missing,
            row_number() over (partition by gap_id order by fetched_at desc) as attempt_rank
        from {{ ref('stg_backfill_attempts') }}
    ) as ranked
    where attempt_rank = 1
),

present_counts as (
    select c.gap_id, count(distinct t.trade_id) as present_count
    from candidates as c
    inner join {{ ref('stg_trades') }} as t
        on t.product_id = c.product_id
        and t.trade_id between c.from_trade_id and c.to_trade_id
        -- The trade-id range decides; the time window only narrows the scan. It is widened
        -- because REST and WebSocket can timestamp the same trade 1 microsecond apart.
        and t.exchange_time between c.gap_start_time - interval 1 minute
            and c.gap_end_time + interval 1 minute
    group by c.gap_id
),

evaluated as (
    select
        c.*,
        coalesce(p.present_count, 0) as ids_present,
        a.missing as rest_missing
    from candidates as c
    left join present_counts as p on c.gap_id = p.gap_id
    left join latest_attempt as a on c.gap_id = a.gap_id
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
    cast(c.ids_present as bigint) as present_count,
    case
        when c.ids_present >= c.missing_count then 'repaired'
        when c.rest_missing > 0 and c.ids_present >= c.missing_count - c.rest_missing
            then 'unrepairable'
        else 'open'
    end as status,
    case
        when c.ids_present >= c.missing_count then current_timestamp()
    end as repaired_at,
    current_timestamp() as last_checked_at
from evaluated as c
