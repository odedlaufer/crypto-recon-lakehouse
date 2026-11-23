#!/usr/bin/env bash
# Capture watchdog (ADR-008 amendment 2): restart capture when no capture run is active.
#
# The self-chaining capture can stop without any run failing (for example when every
# active run is cancelled at once), so no failure alert fires. This script runs on a
# frequent schedule; when no capture run is queued or in progress, and no chaos scenario
# is running, it starts a new capture window and reports it for a Slack alert.
#
# Needs: GH_TOKEN (actions: write), GH_REPO, gh and jq (preinstalled on GitHub runners).
set -euo pipefail

OUTPUT="${GITHUB_OUTPUT:-/dev/null}"

active_runs() {
  gh run list --workflow "$1" --limit 30 --json status \
    | jq '[.[] | select(.status != "completed")] | length'
}

captures="$(active_runs capture.yml)"
if [[ "$captures" -gt 0 ]]; then
  echo "capture is running ($captures active run(s)); nothing to do"
  echo "restarted=false" >> "$OUTPUT"
  exit 0
fi

if [[ "$(active_runs chaos.yml)" -gt 0 ]]; then
  echo "a chaos scenario is running; leaving capture stopped on purpose"
  echo "restarted=false" >> "$OUTPUT"
  exit 0
fi

last_run="$(gh run list --workflow capture.yml --limit 1 --json updatedAt,conclusion \
  | jq -r 'if length == 0 then "none" else .[0] | "\(.updatedAt) (\(.conclusion))" end')"

gh workflow run capture.yml --ref main -f duration_minutes=345 -f chain=true
echo "no capture run was active (last capture run ended $last_run); started a new one"
{
  echo "restarted=true"
  echo "last_run=$last_run"
} >> "$OUTPUT"
