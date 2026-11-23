{{ config(severity='warn', tags=['post_repair']) }}

-- PRD §2 / §7, ADR-012: repaired completeness per product-day must meet the SLO.
-- Checks the last completeness_alert_days days. Tagged post_repair: daily-batch runs it
-- only after gap repair, so a gap that repair fixes never raises an alert.
select
    product_id,
    to_date(hour_ts) as day,
    sum(minutes) as minutes,
    sum(complete_minutes_after) as complete_minutes,
    round(100 * sum(complete_minutes_after) / sum(minutes), 3) as repaired_completeness_pct
from {{ ref('mart_completeness_hourly') }}
where hour_ts >= current_date() - interval {{ var('completeness_alert_days') }} days
group by product_id, to_date(hour_ts)
having 100 * sum(complete_minutes_after) / sum(minutes) < {{ var('completeness_slo_pct') }}
