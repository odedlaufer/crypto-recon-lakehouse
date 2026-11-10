# ADR-007: dbt Core v1 instead of dbt v2

- Status: Accepted
- Date: 2026-11-10

## Context
dbt v2 (a single binary using ADBC drivers) is now the default for new installs, and its Databricks adapter is generally available. The PRD specifies dbt Core with dbt-databricks. The Elementary CLI (FR-O5) declares a hard dependency on dbt-core and conflicts with dbt v2 in the same environment (elementary-data/elementary#2354).

## Decision
Use dbt Core 1.12 with dbt-databricks 1.13, in the uv `dbt` dependency group. Revisit once the Elementary CLI runs against dbt v2 without dbt-core.

## Consequences
- dbt and Elementary run in one environment and one lockfile.
- Slower parse and compile than v2.
- dbt documents the v2 upgrade as project-compatible, so migrating later is a contained change.
