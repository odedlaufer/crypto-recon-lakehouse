# Architecture Decision Records

| ADR | Decision | Status |
| --- | --- | --- |
| [ADR-001](ADR-001-free-edition-external-producer.md) | Databricks Free Edition with an external producer landing files in a UC Volume | Accepted |
| [ADR-002](ADR-002-gap-detection.md) | Detect gaps by trade-id continuity per product | Accepted |
| [ADR-003](ADR-003-backfill-strategy.md) | Backfill gaps from REST trades by trade-id cursor | Accepted |
| [ADR-004](ADR-004-soda-data-contracts.md) | WAP audit with Soda Core v4 data contracts | Accepted |
| [ADR-005](ADR-005-uv-workspace.md) | uv workspace with a shared `libs/common` package | Accepted |
| [ADR-006](ADR-006-catalog-per-environment.md) | One Unity Catalog catalog per environment | Accepted |
| [ADR-007](ADR-007-dbt-core-v1.md) | dbt Core v1 instead of dbt v2 | Accepted |
| [ADR-008](ADR-008-ci-capture.md) | Capture in GitHub Actions with overlapping windows, PAT auth | Accepted |
| [ADR-009](ADR-009-gold-candles-materialized-view.md) | Build gold.candles_stream_1m as a materialized view | Accepted |
| [ADR-010](ADR-010-orchestration.md) | GitHub Actions orchestrates the daily flow; Databricks does the Spark work | Accepted |
| [ADR-011](ADR-011-few-large-landing-files.md) | Land few, large files; process the stream daily | Accepted |
| [ADR-012](ADR-012-official-mismatch.md) | Classify official-candle errors as OFFICIAL_MISMATCH | Accepted |
| [ADR-013](ADR-013-gap-detector-dbt.md) | Gap detector as an incremental dbt model | Accepted |
| [ADR-014](ADR-014-backfill-tables.md) | Backfilled trades in their own bronze and silver tables | Accepted |
