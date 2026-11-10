# ADR-001: Databricks Free Edition with an external producer

- Status: Accepted
- Date: 2026-11-10

## Context
The project must run on free infrastructure only (PRD §1 non-goals). Databricks Free Edition is serverless only, restricts outbound internet, supports only AvailableNow streaming triggers, and has a daily fair-use quota (PRD §3).

## Decision
Use Databricks Free Edition for compute and storage. The WebSocket producer and REST fetchers run outside Databricks (Docker locally, GitHub Actions on a schedule) and upload files into the Unity Catalog Volume `raw.landing`. Databricks only reads from the Volume.

## Consequences
- Works within the outbound-network restriction; the producer is portable to any platform.
- Two runtimes to operate (external producer and Databricks).
- Data is captured in windows, not 24/7, and latency is bounded by flush interval plus upload time.
