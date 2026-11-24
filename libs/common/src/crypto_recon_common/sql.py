"""Run SQL on a Databricks SQL warehouse and get the rows back as dicts."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Protocol

from databricks.sdk.service.sql import StatementState

_RUNNING = (StatementState.PENDING, StatementState.RUNNING)


class StatementRunner(Protocol):
    """The part of the Databricks SDK statement execution API used here."""

    def execute_statement(self, statement: str, warehouse_id: str, **kwargs: Any) -> Any: ...

    def get_statement(self, statement_id: str) -> Any: ...


def query(
    statements: StatementRunner,
    warehouse_id: str,
    catalog: str,
    sql: str,
    *,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 300.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, str | None]]:
    """Rows of `sql` as {column: value} (values as strings, as the API returns them)."""
    response = statements.execute_statement(
        statement=sql, warehouse_id=warehouse_id, catalog=catalog, wait_timeout="50s"
    )
    waited = 0.0
    while response.status.state in _RUNNING:
        if waited >= timeout_seconds:
            raise TimeoutError(f"query took longer than {timeout_seconds}s")
        sleep(poll_seconds)
        waited += poll_seconds
        response = statements.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query failed: {response.status.error}")
    if response.result and response.result.next_chunk_index:
        raise RuntimeError("result has more than one chunk; narrow the query")
    columns = [column.name for column in response.manifest.schema.columns]
    rows = (response.result.data_array if response.result else None) or []
    return [dict(zip(columns, row, strict=True)) for row in rows]
