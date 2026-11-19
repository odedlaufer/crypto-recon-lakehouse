"""Read open gaps from ops.gaps through the SQL warehouse (FR-B4, ADR-013)."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from databricks.sdk.service.sql import StatementState

# The catalog is passed as an API parameter, not interpolated into the SQL.
OPEN_GAPS_SQL = """
SELECT gap_id, product_id, from_trade_id, to_trade_id, missing_count,
       CAST(to_date(gap_start_time) AS STRING) AS gap_day
FROM ops.gaps
WHERE status = 'open'
ORDER BY gap_start_time
"""

_RUNNING = (StatementState.PENDING, StatementState.RUNNING)


@dataclass(frozen=True)
class OpenGap:
    gap_id: str
    product_id: str
    from_trade_id: int
    to_trade_id: int
    missing_count: int
    day: date


class StatementRunner(Protocol):
    """The part of the Databricks SDK statement execution API used here."""

    def execute_statement(self, statement: str, warehouse_id: str, **kwargs: Any) -> Any: ...

    def get_statement(self, statement_id: str) -> Any: ...


def read_open_gaps(
    statements: StatementRunner,
    warehouse_id: str,
    catalog: str,
    *,
    limit: int,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 300.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list[OpenGap]:
    """Open gaps, oldest first, at most `limit`."""
    response = statements.execute_statement(
        statement=OPEN_GAPS_SQL,
        warehouse_id=warehouse_id,
        catalog=catalog,
        row_limit=limit,
        wait_timeout="30s",
    )
    waited = 0.0
    while response.status.state in _RUNNING:
        if waited >= timeout_seconds:
            raise TimeoutError(f"reading open gaps took longer than {timeout_seconds}s")
        sleep(poll_seconds)
        waited += poll_seconds
        response = statements.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"reading open gaps failed: {response.status.error}")

    rows = (response.result.data_array if response.result else None) or []
    return [
        OpenGap(
            gap_id=row[0],
            product_id=row[1],
            from_trade_id=int(row[2]),
            to_trade_id=int(row[3]),
            missing_count=int(row[4]),
            day=date.fromisoformat(row[5]),
        )
        for row in rows
    ]
