# ADR-005: uv workspace with a shared libs/common package

- Status: Accepted
- Date: 2026-11-10

## Context
The producer, fetchers and Databricks code have different dependencies and deploy targets (Docker image, GitHub Actions, Databricks wheel), but share code such as config loading and uploading to the Volume.

## Decision
Use a uv workspace: a virtual root project plus members `libs/common`, `producer`, `fetchers` and `databricks`, with a single committed `uv.lock`. dbt and PySpark are opt-in dependency groups. `libs/common` is an addition to the PRD §8.1 structure.

## Consequences
- Each deployable installs only its own dependencies (`uv sync --package <name> --frozen`).
- One lockfile keeps versions consistent across all components.
- Shared code lives once instead of being copied.
- One more top-level directory than PRD §8.1.
