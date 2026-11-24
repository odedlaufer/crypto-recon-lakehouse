# ADR-015: Continuous deployment to dev; prd kept deployable, not run

Status: Accepted (2026-11-24)

## Context
PRD §8.3 asks for `bundle deploy -t prod` on every merge to main. On Databricks Free Edition a
second running environment would duplicate capture, the stream pipeline and the daily jobs, and
the daily fair-use quota already forced ADR-011 (batching capture files). Until now, code changes
reached the running system only through a manual `make deploy`.

## Options
1. Deploy and run prd: real separation, but doubles quota use.
2. Deploy prd without running it: resources that look like production but prove nothing (its
   dashboard datasets would still read crypto_dev).
3. dev is the live environment and is deployed on every merge; prd is validated on every merge.

## Decision
Option 3. `deploy.yml` runs on every push to main: `bundle validate -t prd`, then
`bundle deploy -t dev`, one deploy at a time. Destructive changes are not auto-approved; such a
deploy fails and is applied manually with `make deploy` after review.

## Consequences
- Merged code reaches the running system within minutes, with no manual step.
- A change that breaks the prd target fails CI before it can matter.
- Not covered: a prd environment receiving data. With a paid workspace: a separate prd
  workspace with a service principal, `dataset_catalog` set per target for the dashboard, and
  this workflow deploying prd after dev.
