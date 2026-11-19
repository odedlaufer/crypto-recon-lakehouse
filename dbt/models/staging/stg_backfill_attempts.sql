-- One row per gap per backfill run: how many of the gap's ids REST returned (FR-J3).
select
    gap_id,
    product_id,
    from_trade_id,
    to_trade_id,
    requested,
    received,
    missing,
    fetched_at
from {{ source('bronze', 'backfill_attempts') }}
