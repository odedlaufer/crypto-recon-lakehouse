# ADR-008: Capture in GitHub Actions with overlapping windows, authenticated by a PAT

- Status: Accepted
- Date: 2026-11-10

## Context
Phase 2 needs a full UTC day of stream data, which a laptop cannot provide. GitHub-hosted
jobs run at most 6 hours. CI needs non-interactive Databricks auth; Free Edition has no
account console or account-level APIs, which service-principal OAuth depends on.

## Decision
- `.github/workflows/capture.yml` runs the producer from the `dev` environment, uploading
  to `/Volumes/crypto_dev/raw/landing` (FR-P7, brought forward from Phase 5).
- A window starts every 5 hours at minute 7 (00:07, 05:07, 10:07, 15:07, 20:07 UTC) and
  lasts 345 minutes. Consecutive windows overlap by at least 45 minutes.
- Auth: a Databricks personal access token in the GitHub `dev` environment secret
  `DATABRICKS_TOKEN`, with a 90-day lifetime (created 2026-11-10, rotate before
  2027-02-08). `DATABRICKS_HOST` is an environment variable, not a secret.

## Consequences
- Coverage is continuous; a hole only appears if a run fails or starts more than 45
  minutes late, and then it is a normal gap repaired from REST (ADR-002, ADR-003). This
  largely settles the ADR-002 follow-up about periods between capture windows.
- Overlaps land duplicate trades in bronze; silver.trades drops them (FR-S3).
- The token is tied to a user and expires; rotation goes in the runbook.
- GitHub disables scheduled workflows after 60 days without repository activity.
