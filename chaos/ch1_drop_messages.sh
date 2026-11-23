#!/usr/bin/env bash
# CH-1 (PRD §8.5): capture for CH1_MINUTES while dropping DROP_PCT% of trades at random.
# Expected outcome: the dropped trades become many small trade-id gaps in ops.gaps; the
# next daily-batch run backfills them from REST and the affected minutes move to MATCH
# (or OFFICIAL_MISMATCH) after repair.
#
# Stops the regular capture first (otherwise its overlapping run would still receive
# every trade and hide the drops), and restarts it afterwards.
# Needs: GH_TOKEN (actions: write), GH_REPO, DATABRICKS_HOST/TOKEN, the producer installed.
set -euo pipefail

DROP_PCT="${DROP_PCT:-5}"
CH1_MINUTES="${CH1_MINUTES:-20}"
VOLUME_PATH="${VOLUME_PATH:-/Volumes/crypto_dev/raw/landing}"
POLL_SECONDS="${POLL_SECONDS:-10}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

active_capture_runs() {
  gh run list --workflow capture.yml --limit 50 --json databaseId,status \
    | jq -r '.[] | select(.status != "completed") | .databaseId'
}

for run_id in $(active_capture_runs); do
  echo "cancelling capture run $run_id"
  gh run cancel "$run_id"
done
for _ in $(seq 1 60); do
  [[ -z "$(active_capture_runs)" ]] && break
  sleep "$POLL_SECONDS"
done
if [[ -n "$(active_capture_runs)" ]]; then
  echo "capture runs are still active; aborting" >&2
  exit 1
fi

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "CH-1 capture: ${CH1_MINUTES} min, dropping ${DROP_PCT}% of trades, from $started_at"
mkdir -p data
uv run --no-sync python -m crypto_recon_producer \
  --duration-seconds $(( CH1_MINUTES * 60 )) \
  --volume-path "$VOLUME_PATH" \
  --chaos-drop-pct "$DROP_PCT" \
  --log-file data/ch1.log || echo "producer exited with an error; restarting capture anyway"
ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

gh workflow run capture.yml --ref main -f duration_minutes=345 -f chain=true
echo "regular capture restarted"

stats="$(grep -o 'ProducerStats([^)]*)' data/ch1.log | tail -1 || true)"
dropped="$(grep -o 'chaos_dropped=[0-9]*' <<< "$stats" | cut -d= -f2 || true)"

cat >> "$SUMMARY" <<SUMMARY_EOF
## CH-1: ${DROP_PCT}% of trades dropped for ${CH1_MINUTES} minutes

| Event | Value |
| --- | --- |
| Chaos capture started (UTC) | ${started_at} |
| Chaos capture ended (UTC) | ${ended_at} |
| Trades dropped on purpose | ${dropped:-unknown} |
| Producer stats | \`${stats:-not found}\` |

After the next daily-batch run, check:

\`\`\`sql
SELECT product_id, count(*) AS gaps, sum(missing_count) AS missing_trades,
       count_if(status = 'repaired') AS repaired
FROM crypto_dev.ops.gaps
WHERE gap_start_time BETWEEN to_timestamp('${started_at}') AND to_timestamp('${ended_at}')
GROUP BY product_id ORDER BY product_id;
\`\`\`
The sum of missing_trades should be close to ${dropped:-the dropped count} (a few more at the
start and end, where capture was switched over).
SUMMARY_EOF
