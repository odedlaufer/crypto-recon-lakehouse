# ADR-011: Land few, large files; process the stream daily

- Status: Accepted
- Date: 2026-11-19
- Changes: PRD FR-P3 (flush every 30 s or 1,000 messages) and the stream freshness SLO

## Context
From 2026-11-17 the daily job was refused before starting: "Triggering new runs for
organization ... is currently disabled temporarily" (Free Edition usage limits).
`system.billing.usage` showed our compute at about 2 DBUs per day, but
`DEFAULT_STORAGE` at 17 to 25 DSUs per day from the day continuous capture started,
almost entirely `API_OPERATION`; storage space was below 0.25 DSUs per day.

| Day | API operations (DSU) | Storage space (DSU) |
| --- | --- | --- |
| 2026-11-10 (no continuous capture) | 3.49 | 0.00 |
| 2026-11-13 | 20.92 | 0.05 |
| 2026-11-14 | 25.21 | 0.09 |
| 2026-11-15 | 22.18 | 0.21 |

Flushing every 30 seconds per product and channel produced about 800 files per hour;
each file costs upload operations and is listed again by Auto Loader.

## Decision
- The producer flushes each batch after 600 seconds or 100,000 messages (CLI defaults
  `--flush-seconds`, `--max-messages`): about 42 files per hour instead of about 800.
- The stream is captured continuously but processed daily (the pipeline is triggered by
  the daily job, ADR-010). The PRD's "< 2 min" stream freshness SLO does not apply on
  Free Edition; freshness is daily.

## Consequences
- About 19 times fewer landing files and API operations; to be confirmed in
  `system.billing.usage` after deployment.
- Latency is still measured per message (`received_at` on every line).
- A producer crash can lose up to 10 minutes of buffered messages instead of 30 seconds;
  those become trade-id gaps, repaired from REST in Phase 3.
- On a paid platform, a continuous pipeline and short flushes restore minute-level
  freshness (README trade-offs).
