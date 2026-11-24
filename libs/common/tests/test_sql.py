from types import SimpleNamespace

import pytest
from databricks.sdk.service.sql import StatementState

from crypto_recon_common.sql import query


def _response(state, rows=None, next_chunk=None):
    return SimpleNamespace(
        statement_id="s1",
        status=SimpleNamespace(state=state, error="boom"),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(columns=[SimpleNamespace(name="day"), SimpleNamespace(name="n")])
        ),
        result=SimpleNamespace(data_array=rows, next_chunk_index=next_chunk),
    )


class Fake:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute_statement(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)

    def get_statement(self, statement_id):
        return self.responses.pop(0)


def test_rows_become_dicts_after_polling() -> None:
    fake = Fake(
        [
            _response(StatementState.PENDING),
            _response(StatementState.SUCCEEDED, [["2026-11-21", "3"], ["2026-11-22", None]]),
        ]
    )
    rows = query(fake, "wh", "crypto_dev", "SELECT 1", sleep=lambda s: None)
    assert rows == [{"day": "2026-11-21", "n": "3"}, {"day": "2026-11-22", "n": None}]
    assert fake.calls[0]["catalog"] == "crypto_dev"


def test_failure_and_multi_chunk_raise() -> None:
    with pytest.raises(RuntimeError, match="query failed"):
        query(Fake([_response(StatementState.FAILED)]), "wh", "c", "x", sleep=lambda s: None)
    with pytest.raises(RuntimeError, match="more than one chunk"):
        query(Fake([_response(StatementState.SUCCEEDED, [], 1)]), "wh", "c", "x")
