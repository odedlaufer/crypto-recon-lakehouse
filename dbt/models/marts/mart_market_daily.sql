{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['product_id', 'day'],
        on_schema_change='fail',
    )
}}

-- PRD §4 / §7 market view: one row per product and complete UTC day, from every known
-- trade (stream + backfill, deduplicated on trade id) and, for volatility, the 1-minute
-- candles. Incremental runs recompute the last market_lookback_days days, so repaired
-- gaps reach the mart.

with scope as (
    select
        {% if is_incremental() %}
        date_sub(current_date(), {{ var('market_lookback_days') }}) as first_day,
        {% else %}
        to_date('{{ var("reconcile_start_date") }}') as first_day,
        {% endif %}
        date_sub(current_date(), 1) as last_day
),

trades as (
    select
        t.product_id,
        t.trade_id,
        any_value(t.exchange_time) as exchange_time,
        any_value(t.price) as price,
        any_value(t.size) as size
    from {{ ref('stg_trades') }} as t
    cross join scope as s
    where t.exchange_time >= s.first_day
      and t.exchange_time < date_add(s.last_day, 1)
    group by t.product_id, t.trade_id
),

daily as (
    select
        product_id,
        to_date(exchange_time) as day,
        min_by(price, trade_id) as open,
        max(price) as high,
        min(price) as low,
        max_by(price, trade_id) as close,
        sum(size) as volume,
        sum(price * size) as notional,
        count(*) as trade_count,
        count(distinct date_trunc('MINUTE', exchange_time)) as minutes_with_trades
    from trades
    group by product_id, to_date(exchange_time)
),

minute_returns as (
    select
        c.product_id,
        to_date(c.minute_ts) as day,
        ln(
            cast(c.close as double)
            / cast(lag(c.close) over (partition by c.product_id order by c.minute_ts) as double)
        ) as log_return
    from {{ ref('stg_candles_stream') }} as c
    cross join scope as s
    -- one extra day so the first minute of first_day has a previous close
    where c.minute_ts >= date_sub(s.first_day, 1)
      and c.minute_ts < date_add(s.last_day, 1)
),

volatility as (
    select
        r.product_id,
        r.day,
        stddev_samp(r.log_return) * sqrt(1440) * 100 as realized_vol_pct
    from minute_returns as r
    cross join scope as s
    where r.day >= s.first_day
    group by r.product_id, r.day
)

select
    d.product_id,
    d.day,
    d.open,
    d.high,
    d.low,
    d.close,
    cast(d.notional / d.volume as decimal(38, 8)) as vwap,
    d.volume,
    cast(d.notional as decimal(38, 8)) as notional_usd,
    cast(d.trade_count as bigint) as trade_count,
    cast(d.minutes_with_trades as bigint) as minutes_with_trades,
    cast(v.realized_vol_pct as double) as realized_vol_pct
from daily as d
left join volatility as v on d.product_id = v.product_id and d.day = v.day
