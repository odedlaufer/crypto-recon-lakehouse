#!/usr/bin/env bash
# Start the next capture window (ADR-008), unless another capture run will keep
# capturing for at least MIN_REMAINING_SECONDS. That guard lets a cron-started run and a
# chained run merge back into a single chain instead of multiplying.
#
# Needs: GH_TOKEN (actions: write), GITHUB_RUN_ID, gh and jq (preinstalled on runners).
set -euo pipefail

WINDOW_SECONDS="${WINDOW_SECONDS:-20700}"              # 345 minutes
MIN_REMAINING_SECONDS="${MIN_REMAINING_SECONDS:-1800}"  # 30 minutes
NOW="${NOW:-$(date -u +%s)}"

others="$(gh run list --workflow capture.yml --limit 20 \
  --json databaseId,status,startedAt)"

covered="$(jq \
  --argjson me "$GITHUB_RUN_ID" \
  --argjson now "$NOW" \
  --argjson window "$WINDOW_SECONDS" \
  --argjson min "$MIN_REMAINING_SECONDS" '
  [ .[]
    | select(.databaseId != $me)
    | select(.status == "in_progress" or .status == "queued"
             or .status == "waiting" or .status == "pending" or .status == "requested")
    | if .status == "in_progress"
      then ((.startedAt | fromdateiso8601) + $window - $now) >= $min
      else true
      end
  ] | any' <<<"$others")"

if [[ "$covered" == "true" ]]; then
  echo "another capture run covers the next $((MIN_REMAINING_SECONDS / 60))+ minutes; not handing off"
  exit 0
fi

echo "handing off: starting the next capture window"
gh workflow run capture.yml --ref main -f duration_minutes=345 -f chain=true
