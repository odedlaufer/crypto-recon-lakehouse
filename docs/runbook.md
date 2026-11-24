# Runbook

Operational procedures. Every command here has been run against this project.
Environments (ADR-006, ADR-015): `dev` (catalog `crypto_dev`) is the live environment;
`it` (catalog `crypto_it`) is the integration test; `prd` (catalog `crypto_prd`) is
validated on every merge but not deployed or run.

## Rotate the Databricks token (CI)
The GitHub `dev` environment secret `DATABRICKS_TOKEN` is a personal access token with a
90-day lifetime (ADR-008). Current token created 2026-11-10: rotate before 2027-02-08.

1. Databricks UI: avatar → Settings → Developer → Access tokens → Manage →
   Generate new token. Comment `github-actions-capture`, lifetime 90 days. Copy it once;
   never paste it anywhere else.
2. Test it locally without echoing or saving it to shell history:
   ```bash
   read -s DBX_TOKEN
   DATABRICKS_HOST=https://dbc-c6f5458b-d402.cloud.databricks.com DATABRICKS_TOKEN=$DBX_TOKEN \
     databricks current-user me | grep '"userName"'
   unset DBX_TOKEN
   ```
3. GitHub → Settings → Environments → `dev` → update secret `DATABRICKS_TOKEN`.
4. Run the `capture` workflow manually with `duration_minutes: 3`; check it authenticates.
5. Revoke the old token in the Databricks UI and update the dates in ADR-008 and here.

Local development is not affected: the CLI and dbt use the OAuth profile `personal`.

## Re-run the daily processing for a date
Processes the given day plus the day before (fetch candles, WAP, stream refresh, dbt):
GitHub → Actions → `daily-batch` → Run workflow → `date: YYYY-MM-DD`.

Locally, step by step:
```bash
make backfill START=2026-11-09 END=2026-11-10
databricks bundle run daily -t dev --params start_date=2026-11-09,end_date=2026-11-10
make dbt ARGS="build"
```
Every step is idempotent: re-running a published day inserts and updates nothing.

## Backfill official candles for a range
```bash
make backfill START=2026-11-01 END=2026-11-07
databricks bundle run candles_official -t dev --params start_date=2026-11-01,end_date=2026-11-07
```
The audit (DQ-11 to DQ-14) checks every product-day; a failure stops before publishing,
keeps `silver.candles_official_staging` for debugging and sends a failure email.

## Capture manually
- GitHub: Actions → `capture` → Run workflow → `duration_minutes` (1 to 345).
- Laptop (keep it awake and plugged in):
  ```bash
  caffeinate -i make run-producer \
    ARGS="--duration-seconds 3600 --volume-path /Volumes/crypto_dev/raw/landing --log-file data/capture.log"
  ```
  Stop early with Ctrl+C: the producer flushes, uploads and writes final metrics.

## Refresh the stream pipeline
```bash
databricks bundle run stream -t dev
```
Loads new landed files into bronze and silver and refreshes `gold.candles_stream_1m`.

## Set up a fresh clone
```bash
make setup     # uv sync (all packages + dbt) and the git pre-commit hook
make validate  # bundle config
make dbt ARGS="deps"
```
`make setup` installs the hook with the global git config hidden, because pre-commit
refuses to install while a global `core.hooksPath` is set.

## Bootstrap a new environment
1. Create the catalog in the UI: Catalog → Create catalog → name → Use default storage.
   Free Edition only allows Default Storage catalogs from the UI (ADR-006).
2. Deploy schemas, Volumes, pipeline and jobs: `databricks bundle deploy -t <target>`.

## Health checks (SQL)
Duplicates and dropped trades in a time window (expect `trades = distinct_ids = id_span`):
```sql
WITH w AS (
  SELECT product_id, trade_id FROM crypto_dev.silver.trades
  WHERE received_at BETWEEN '<from>' AND '<to>'
)
SELECT product_id, count(*) AS trades, count(DISTINCT trade_id) AS distinct_ids,
       max(trade_id) - min(trade_id) + 1 AS id_span
FROM w GROUP BY product_id;
```

Reconciliation status mix:
```sql
SELECT product_id, status_before_repair, status_after_repair, count(*) AS minutes
FROM crypto_dev.marts.fct_reconciliation_1m
GROUP BY ALL ORDER BY product_id, minutes DESC;
```

## First dbt build in a fresh environment
Incremental unit tests read column types from the existing table (`this`), so on a fresh
target the first build must create the tables without tests:
```bash
make dbt ARGS="run"
make dbt ARGS="build"
```

## Deploy
Every merge to main runs the `deploy` workflow: `bundle validate -t prd`, then
`bundle deploy -t dev` (ADR-015). A change that would delete or recreate a resource (for
example the dashboard) is not auto-approved and makes the workflow fail; review it, then
apply it by hand:
```bash
make deploy                    # TARGET=dev by default; answer the prompt
```

## Gaps: inspect and repair by hand
The daily-batch run detects and repairs gaps on its own. To look or act in between:
```bash
uv run --package crypto-recon-fetchers python -m crypto_recon_fetchers backfill \
  --warehouse-id 8330a90a8089093e --databricks-profile personal --dry-run      # list open gaps
uv run --package crypto-recon-fetchers python -m crypto_recon_fetchers backfill \
  --warehouse-id 8330a90a8089093e --databricks-profile personal \
  --volume-path /Volumes/crypto_dev/raw/landing                              # fetch and land
databricks bundle run stream -t dev
make dbt ARGS="build"
```
Nearby gaps are fetched as one id range (one request, one file); at most `--max-gaps` (5,000)
gaps and `--max-trades` ids per run, oldest first. Status per gap:
```sql
SELECT status, count(*) AS gaps, sum(missing_count) AS missing_trades
FROM crypto_dev.ops.gaps GROUP BY status;
```
`unrepairable` means the REST API no longer returns those ids; it raises a data alert.

## Replay quarantined data
- **A trade rule was wrong** (`quarantine.trades`): fix the rule in
  `databricks/pipelines/stream/transformations/`, deploy, then rebuild the two tables from
  bronze. This re-reads all of bronze (about 30 million rows), so do it on purpose:
  ```bash
  databricks bundle run stream -t dev --full-refresh silver.trades,quarantine.trades
  ```
  Gold candles and dbt pick up the corrected trades on their next run.
- **The producer rejected valid messages** (`quarantine.ws_rejected`, raw payload kept): not
  automated. Fix the parser, then land the payloads as new files in
  `/Volumes/crypto_dev/raw/landing/ws/matches/` in the producer's format (see
  `chaos/ch3_malformed_messages.py` for how files are written), and refresh the pipeline.

## Capture stopped
The `capture-watchdog` workflow (every 30 minutes) starts a capture window if none is running
or queued and no chaos run is active, and posts to Slack when it had to. To start one now:
Actions → `capture` → Run workflow (`duration_minutes: 345`, `chain: true`). Any trades missed
in between become gaps and are repaired by the next daily-batch run.

## Daily quota refused (Free Edition)
Symptom: pipeline or job runs are refused or never start, with a quota message. Compute stops
for the day; nothing is lost. Landed files wait in the Volume and the next run loads them
(Auto Loader keeps track). Avoid extra manual runs that day. Cause and the fix that removed most
of the load: ADR-011.

## Rotate the Slack webhook
Slack app `crypto-recon-alerts` → Incoming Webhooks → add a new webhook to `#crypto-alerts`,
then GitHub → Settings → Environments → `dev` → secret `SLACK_WEBHOOK_URL`. Test it:
```bash
read -s SLACK_WEBHOOK_URL; export SLACK_WEBHOOK_URL
uv run --package crypto-recon-common python -m crypto_recon_common.notify "webhook test"
unset SLACK_WEBHOOK_URL
```
Then remove the old webhook in the Slack app.

## Chaos tests
- CH-1 (drop trades) and CH-2 (kill the producer): Actions → `chaos` → scenario.
- CH-3 (malformed messages) and CH-4 (corrupted official candles): local scripts in `chaos/`.
Procedures, expected outcomes and results: [chaos.md](chaos.md).

## Integration test
`tests/integration/run_it.sh` locally, or Actions → `integration-test`. Uses the `it` target
and catalog `crypto_it`; details in [integration-test.md](integration-test.md). After an
intended change to the results, re-record with `RECORD_GOLDEN=1` and review the diff of
`tests/integration/golden.json`.

## Site, demo and video
- The `docs` workflow publishes https://odedlaufer.github.io/crypto-recon-lakehouse/ (site with
  live numbers, dbt docs, Elementary report) after every daily-batch run.
- `make demo` (or `make demo ARGS=--quick`): preflight, live CH-3 caught on screen, site refresh,
  opens everything. Its `gh` calls use the repo owner's token, so the owner account must be
  logged in to gh once (`gh auth login`); the active gh account does not matter.
- `make demo-video` re-records `web/media/demo.mp4`; `make screenshots` refreshes `docs/img/`.
  Both need `uv run --group demo playwright install chromium` once and ffmpeg for the video.
