{{ config(materialized='table') }}

-- FR-O1: one row per Databricks job run and stream pipeline update.
-- Jobs come from system.lakeflow; pipeline updates from the pipeline event log.

with pipeline_updates as (
    select
        update_id as run_id,
        min(event_ts) as started_at,
        max(event_ts) as ended_at,
        max_by(details:update_progress.state, event_ts) as status
    from {{ ref('stg_pipeline_events') }}
    where event_type = 'update_progress'
    group by update_id
),

pipeline_rows as (
    select
        update_id as run_id,
        sum(
            case when flow_name not like '%.quarantine.%'
                then cast(details:flow_progress.metrics.num_output_rows as bigint) end
        ) as rows_written,
        sum(
            case when flow_name like '%.quarantine.%'
                then cast(details:flow_progress.metrics.num_output_rows as bigint) end
        ) as rows_quarantined
    from {{ ref('stg_pipeline_events') }}
    where event_type = 'flow_progress'
    group by update_id
),

job_names as (
    select job_id, max_by(name, change_time) as job_name
    from {{ source('system_lakeflow', 'jobs') }}
    group by job_id
),

job_runs as (
    select
        cast(t.run_id as string) as run_id,
        coalesce(n.job_name, cast(t.job_id as string)) as job_name,
        min(t.period_start_time) as started_at,
        max(t.period_end_time) as ended_at,
        max_by(t.result_state, t.period_end_time) as status
    from {{ source('system_lakeflow', 'job_run_timeline') }} as t
    left join job_names as n on t.job_id = n.job_id
    -- Only this target's jobs: the system tables cover every job in the workspace,
    -- including the integration-test (it) target's.
    where n.job_name like 'crypto-recon-%-{{ var("bundle_target") }}'
    group by t.run_id, t.job_id, n.job_name
)

select
    u.run_id,
    'crypto-recon-stream' as job_name,
    'pipeline' as run_type,
    u.started_at,
    u.ended_at,
    cast(unix_timestamp(u.ended_at) - unix_timestamp(u.started_at) as double) as duration_s,
    u.status,
    u.status = 'COMPLETED' as succeeded,
    coalesce(r.rows_written, 0) as rows_written,
    coalesce(r.rows_quarantined, 0) as rows_quarantined
from pipeline_updates as u
left join pipeline_rows as r on u.run_id = r.run_id

union all

select
    run_id,
    job_name,
    'job' as run_type,
    started_at,
    ended_at,
    cast(unix_timestamp(ended_at) - unix_timestamp(started_at) as double) as duration_s,
    status,
    status = 'SUCCEEDED' as succeeded,
    cast(null as bigint) as rows_written,
    cast(null as bigint) as rows_quarantined
from job_runs
