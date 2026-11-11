# Runbook

Operational procedures. Every command here has been run against this project.
Environments: `dev` (catalog `crypto_dev`) and `prd` (catalog `crypto_prd`), ADR-006.

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
