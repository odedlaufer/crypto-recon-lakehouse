{{ config(materialized='table') }}

-- FR-O4: expectation pass/fail counts per pipeline update, dataset and rule (PRD §6.1).
-- A flow reports progress several times per update; the counts are summed.

with flow_events as (
    select
        update_id,
        event_ts,
        from_json(
            details:flow_progress.data_quality.expectations,
            'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
        ) as expectations
    from {{ ref('stg_pipeline_events') }}
    where event_type = 'flow_progress'
      and details:flow_progress.data_quality.expectations is not null
),

exploded as (
    select update_id, event_ts, explode(expectations) as e
    from flow_events
)

select
    update_id,
    e.dataset as dataset,
    e.name as expectation,
    cast(sum(e.passed_records) as bigint) as passed_records,
    cast(sum(e.failed_records) as bigint) as failed_records,
    max(event_ts) as last_event_ts
from exploded
group by update_id, e.dataset, e.name
