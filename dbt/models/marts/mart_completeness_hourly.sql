{{ config(materialized='table') }}

-- PRD §2 headline metrics per product-hour (ADR-012 definitions):
--   completeness = minutes the pipeline got right (MATCH or OFFICIAL_MISMATCH) / minutes
--   raw          = before gap repair, repaired = after gap repair
--   match rate   = MATCH / minutes (agreement with Coinbase's official candles)

with minutes as (
    select
        product_id,
        date_trunc('HOUR', minute_ts) as hour_ts,
        status_before_repair in ('MATCH', 'OFFICIAL_MISMATCH') as complete_before,
        status_after_repair in ('MATCH', 'OFFICIAL_MISMATCH') as complete_after,
        status_after_repair = 'MATCH' as match_after,
        status_after_repair = 'OFFICIAL_MISMATCH' as official_mismatch_after,
        last_reconciled_at
    from {{ ref('fct_reconciliation_1m') }}
)

select
    product_id,
    hour_ts,
    count(*) as minutes,
    count_if(complete_before) as complete_minutes_before,
    count_if(complete_after) as complete_minutes_after,
    count_if(match_after) as match_minutes,
    count_if(official_mismatch_after) as official_mismatch_minutes,
    cast(round(100 * count_if(complete_before) / count(*), 4) as double) as raw_completeness_pct,
    cast(round(100 * count_if(complete_after) / count(*), 4) as double)
        as repaired_completeness_pct,
    cast(round(100 * count_if(match_after) / count(*), 4) as double) as match_rate_pct,
    max(last_reconciled_at) as last_reconciled_at
from minutes
group by product_id, hour_ts
