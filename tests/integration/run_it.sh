#!/usr/bin/env bash
# Integration test (PRD §8.4): replay the recorded fixture through the real pipeline in the
# `it` target (catalog crypto_it) and check the results against expected.json/golden.json.
#
#   tests/integration/run_it.sh                    # locally (dbt target it, CLI profile personal)
#   IT_DBT_TARGET=ci_it DATABRICKS_PROFILE= ...    # in CI (token from the environment)
#   RECORD_GOLDEN=1 tests/integration/run_it.sh    # first run: record the status mix
#
# Idempotent: the fixture files keep their names, so re-uploading them is a no-op for Auto
# Loader, and dbt runs with --full-refresh pinned to the fixture's day.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BUNDLE_TARGET="${IT_BUNDLE_TARGET:-it}"
DBT_TARGET="${IT_DBT_TARGET:-it}"
CATALOG="${IT_CATALOG:-crypto_it}"
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-8330a90a8089093e}"
PROFILE="${DATABRICKS_PROFILE-personal}"
profile_args=()
[[ -n "$PROFILE" ]] && profile_args=(--profile "$PROFILE")
DAY="$(python3 -c "import json;print(json.load(open('$HERE/expected.json'))['day'])")"

echo "== 1/5 upload fixture for $DAY to $CATALOG"
upload="$(mktemp -d)"
trap 'rm -rf "$upload"' EXIT
(cd "$HERE/fixture" && find . -name '*.jsonl.gz' | while read -r f; do
  mkdir -p "$upload/$(dirname "$f")"
  gunzip -c "$f" > "$upload/${f%.gz}"
done)
databricks fs cp -r --overwrite "$upload" "dbfs:/Volumes/$CATALOG/raw/landing" ${profile_args[@]+"${profile_args[@]}"} > /dev/null
echo "uploaded $(find "$upload" -type f | wc -l | tr -d ' ') files"

cd "$ROOT"
echo "== 2/5 stream pipeline"
databricks bundle run stream -t "$BUNDLE_TARGET" ${profile_args[@]+"${profile_args[@]}"}

echo "== 3/5 official candles (write-audit-publish)"
databricks bundle run candles_official -t "$BUNDLE_TARGET" ${profile_args[@]+"${profile_args[@]}"} \
  --params "start_date=$DAY,end_date=$DAY"

echo "== 4/5 dbt build pinned to $DAY"
(cd dbt && uv run --group dbt dbt build --target "$DBT_TARGET" --full-refresh --exclude test_type:unit \
  --vars "{gap_lookback_days: 100000, market_lookback_days: 100000, reconcile_start_date: '$DAY'}")

echo "== 5/5 checks"
record=()
[[ -n "${RECORD_GOLDEN:-}" ]] && record=(--record-golden)
uv run --package crypto-recon-common python "$HERE/check_it.py" --catalog "$CATALOG" --warehouse-id "$WAREHOUSE_ID" \
  ${PROFILE:+--databricks-profile "$PROFILE"} ${record[@]+"${record[@]}"}
