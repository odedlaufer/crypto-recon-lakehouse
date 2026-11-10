# ADR-006: One Unity Catalog catalog per environment

- Status: Accepted
- Date: 2026-11-10
- Supersedes: PRD ENG-1 wording ("separate schemas in the same catalog")

## Context
PRD §4 names schemas `crypto.bronze`, `crypto.silver`, and so on, while ENG-1 says dev and prod are separate schemas in the same catalog. Both cannot hold.

## Decision
- Catalogs `crypto_dev` and `crypto_prd`, each with identical schemas (`raw`, `bronze`, `silver`, `gold`, `marts`, `ops`, `quarantine`) and the Volume `raw.landing`.
- Code and dbt parameterize the catalog name only.
- The bundle's `dev` target does not use `mode: development`, which prefixes UC schemas with `dev_<user>_`. It uses presets instead: `pipelines_development: true`, `trigger_pause_status: PAUSED`.
- Catalogs are created once in the UI, because Free Edition only allows Default Storage catalogs there. Schemas and the Volume are defined in the bundle.

## Consequences
- Dev cannot write to prod by accident; identical object names in both environments.
- One manual bootstrap step, documented in the runbook.
- The deployment lock stays enabled in dev.
