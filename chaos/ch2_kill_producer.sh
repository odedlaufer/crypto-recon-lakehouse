#!/usr/bin/env bash
# CH-2 (PRD §8.5): stop the producer for OUTAGE_MINUTES mid-window, then restart capture.
# Expected outcome: the hole becomes a trade-id gap in ops.gaps; the next daily-batch run
# backfills it from REST and its minutes move to MATCH or OFFICIAL_MISMATCH after repair.
#
# Needs: GH_TOKEN (actions: write), GH_REPO, gh and jq (preinstalled on GitHub runners).
set -euo pipefail

OUTAGE_MINUTES="${OUTAGE_MINUTES:-10}"
POLL_SECONDS="${POLL_SECONDS:-10}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

active_capture_runs() {
  gh run list --workflow capture.yml --limit 50 --json databaseId,status \
    | jq -r '.[] | select(.status != "completed") | .databaseId'
}

outage_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "outage start: $outage_start"

for run_id in $(active_capture_runs); do
  echo "cancelling capture run $run_id"
  gh run cancel "$run_id"
done

# Cancellation is asynchronous: wait until no capture run is still active.
for _ in $(seq 1 60); do
  remaining="$(active_capture_runs | wc -l | tr -d ' ')"
  if [[ "$remaining" == "0" ]]; then
    break
  fi
  echo "waiting for $remaining capture run(s) to stop"
  sleep "$POLL_SECONDS"
done
if [[ -n "$(active_capture_runs)" ]]; then
  echo "capture runs are still active; aborting without restarting" >&2
  exit 1
fi
stopped_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "all capture runs stopped at $stopped_at; outage of $OUTAGE_MINUTES minutes"

sleep $(( OUTAGE_MINUTES * 60 ))

gh workflow run capture.yml --ref main -f duration_minutes=345 -f chain=true
restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "capture restarted at $restarted_at"

cat >> "$SUMMARY" <<SUMMARY_EOF
## CH-2: producer stopped for ${OUTAGE_MINUTES} minutes

| Event | UTC |
| --- | --- |
| Outage started (cancel requested) | ${outage_start} |
| All capture runs stopped | ${stopped_at} |
| Capture restarted | ${restarted_at} |

After the next daily-batch run, check:

\`\`\`sql
SELECT product_id, gap_start_time, gap_end_time, missing_count, present_count, status
FROM crypto_dev.ops.gaps
WHERE gap_start_time BETWEEN to_timestamp('${outage_start}') - INTERVAL 15 MINUTES
                         AND to_timestamp('${restarted_at}')
ORDER BY product_id;
\`\`\`
SUMMARY_EOF
