-- The stream pipeline's event log (FR-O4). event_log() on any table of the pipeline
-- returns the whole pipeline's log; silver.trades is used as the handle.
select
    id as event_id,
    timestamp as event_ts,
    event_type,
    origin.update_id as update_id,
    origin.flow_name as flow_name,
    details
from event_log(table({{ source('silver', 'trades') }}))
