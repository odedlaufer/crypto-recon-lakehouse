{{ config(severity='warn') }}

-- PRD §7: any gap REST could not fill is reported (FR-J3).
select gap_id, product_id, from_trade_id, to_trade_id, missing_count, present_count, detected_at
from {{ ref('gaps') }}
where status = 'unrepairable'
