#!/usr/bin/env bash
# Live demo (make demo): preflight, refresh the site, a live failure caught on screen, open
# everything, and a talk track. `make demo ARGS=--quick` skips the live failure.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-personal}"
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-8330a90a8089093e}"
VOLUME_PATH="/Volumes/crypto_dev/raw/landing"
SITE="https://odedlaufer.github.io/crypto-recon-lakehouse/"
REPO="https://github.com/odedlaufer/crypto-recon-lakehouse"
DASHBOARD="https://dbc-c6f5458b-d402.cloud.databricks.com/dashboardsv3/01f1e730db4011ce95f2a899b3fcfa8e/published"
QUICK=0
# gh calls in this script act as the repo owner, whatever account is active in gh
# (e.g. a work account). Needs: gh auth login with the owner account once.
export GH_REPO="odedlaufer/crypto-recon-lakehouse"
if owner_token="$(gh auth token -h github.com -u odedlaufer 2> /dev/null)"; then
  export GH_TOKEN="$owner_token"
fi
[[ "${1:-}" == "--quick" ]] && QUICK=1

say() { printf '\n\033[1;32m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

say "1. Preflight"
databricks current-user me --profile "$PROFILE" > /dev/null && note "Databricks: reachable"
capture="$(gh run list --workflow capture.yml --limit 1 --json status --jq '.[0].status')"
note "capture: ${capture} (in_progress = live capture running)"
batch="$(gh run list --workflow daily-batch.yml --limit 1 --json conclusion,createdAt \
  --jq '.[0] | "\(.conclusion) at \(.createdAt)"')"
note "last daily-batch: ${batch}"

say "2. Refreshing the public site in the background (docs workflow)"
if gh workflow run docs.yml --ref main > /dev/null 2>&1; then
  note "started; the site updates in about 5 minutes"
else
  note "could not start it: gh must be logged in as the repo owner (gh auth switch -u odedlaufer)"
fi

if [[ $QUICK -eq 0 ]]; then
  say "3. Live failure: inject 7 poisoned messages into the real pipeline"
  out="$(uv run --package crypto-recon-producer python chaos/ch3_malformed_messages.py \
    --volume-path "$VOLUME_PATH" --databricks-profile "$PROFILE")"
  run_id="$(sed -n 's/^CH-3 run \([0-9]*\):.*/\1/p' <<< "$out")"
  sed -n '1,9p' <<< "$out" | sed 's/^/   /'
  note "running the stream pipeline (about 2 minutes)..."
  mkdir -p data
  if ! databricks bundle run stream -t dev --profile "$PROFILE" > data/demo-pipeline.log 2>&1; then
    note "pipeline failed; see data/demo-pipeline.log"
    exit 1
  fi
  note "pipeline finished (log: data/demo-pipeline.log)"
  uv run --package crypto-recon-common python scripts/demo_check_ch3.py --run-id "$run_id" \
    --warehouse-id "$WAREHOUSE_ID" --databricks-profile "$PROFILE"
fi

say "4. Opening the site, dashboard, repository and data-quality report"
for url in "$SITE" "$DASHBOARD" "$REPO" "${SITE}elementary/"; do
  open "$url" 2> /dev/null || xdg-open "$url" 2> /dev/null || note "$url"
done

say "5. Talk track (about 5 minutes)"
cat <<'TALK'
   1. Site hero: "Live exchange feeds drop data. This pipeline proves its data is complete,
      and repairs it when it is not. Raw vs repaired, live numbers."
   2. Headline chart: point at the outage and CH-2 dips: "each one repaired by the next run."
   3. Just now (step 3): "I injected 7 poisoned messages into the live pipeline;
      each was caught by its exact rule and none reached the tables."
   4. How it works: capture, refine, reconcile, detect by trade id, repair.
   5. Decisions: free-tier constraints, the quota fix, why trade ids beat candle checks.
   6. Dashboard: the 9 SLOs. Repo: tests, chaos scripts, integration test, ADRs.
TALK
