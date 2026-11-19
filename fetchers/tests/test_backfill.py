import itertools
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from databricks.sdk.service.sql import StatementState

from crypto_recon_fetchers.__main__ import _parse_args
from crypto_recon_fetchers.backfill import run_backfill, write_attempts_file
from crypto_recon_fetchers.gaps_source import OpenGap, read_open_gaps
from crypto_recon_fetchers.rest import BASE_URL
from crypto_recon_fetchers.trades import TradesClient

NOW = datetime(2026, 11, 19, 10, 0, 0, tzinfo=UTC)


def _gap(gap_id: str, frm: int, to: int, product: str = "BTC-USD") -> OpenGap:
    return OpenGap(gap_id, product, frm, to, to - frm + 1, date(2026, 11, 14))


class FakeStatements:
    def __init__(self, states: list[StatementState], rows: list[list[str]]) -> None:
        self.states = states
        self.rows = rows
        self.calls: list[dict] = []

    def _response(self) -> SimpleNamespace:
        state = self.states.pop(0)
        return SimpleNamespace(
            statement_id="s1",
            status=SimpleNamespace(state=state, error="boom"),
            result=SimpleNamespace(data_array=self.rows)
            if state == StatementState.SUCCEEDED
            else None,
        )

    def execute_statement(self, **kwargs) -> SimpleNamespace:
        self.calls.append(kwargs)
        return self._response()

    def get_statement(self, statement_id: str) -> SimpleNamespace:
        return self._response()


def test_read_open_gaps_polls_and_parses() -> None:
    fake = FakeStatements(
        [StatementState.PENDING, StatementState.RUNNING, StatementState.SUCCEEDED],
        [["g1", "BTC-USD", "100", "109", "10", "2026-11-14"]],
    )
    gaps = read_open_gaps(fake, "wh", "crypto_dev", limit=5, sleep=lambda s: None)

    assert gaps == [OpenGap("g1", "BTC-USD", 100, 109, 10, date(2026, 11, 14))]
    assert fake.calls[0]["catalog"] == "crypto_dev"
    assert fake.calls[0]["row_limit"] == 5


def test_read_open_gaps_raises_on_failure() -> None:
    fake = FakeStatements([StatementState.FAILED], [])
    with pytest.raises(RuntimeError, match="reading open gaps failed"):
        read_open_gaps(fake, "wh", "crypto_dev", limit=5, sleep=lambda s: None)


def _client(holes: set[int] = frozenset(), fail_after: int | None = None) -> TradesClient:
    def handler(request: httpx.Request) -> httpx.Response:
        after = int(request.url.params["after"])
        if fail_after is not None and after > fail_after:
            return httpx.Response(400, json={"message": "bad"})
        limit = int(request.url.params["limit"])
        ids = [i for i in range(after - 1, max(after - 1 - limit, 0), -1) if i not in holes]
        trades = [
            {"trade_id": i, "size": "1", "price": "1", "side": "buy", "time": "t"} for i in ids
        ]
        return httpx.Response(200, json=trades)

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url=BASE_URL)
    ticks = itertools.count(step=10.0)
    return TradesClient(http, sleep=lambda s: None, monotonic=lambda: next(ticks))


def test_run_backfill_budget_holes_and_failures(tmp_path: Path) -> None:
    gaps = [
        _gap("small", 100, 109),  # 10 ids, 2 of them not in REST
        _gap("huge", 1000, 1999),  # 1,000 ids: over the remaining budget
        _gap("fails", 5000, 5004),  # REST error
        _gap("tiny", 200, 201),  # 2 ids, fits the budget after "huge" is skipped
    ]
    report = run_backfill(
        _client(holes={103, 104}, fail_after=4000), gaps, tmp_path, max_trades=20, now=lambda: NOW
    )

    assert [(o.gap.gap_id, o.received, o.missing) for o in report.outcomes] == [
        ("small", 8, 2),
        ("tiny", 2, 0),
    ]
    assert [g.gap_id for g in report.skipped] == ["huge"]
    assert [g.gap_id for g in report.failed] == ["fails"]
    assert len(report.files) == 2


def test_write_attempts_file(tmp_path: Path) -> None:
    report = run_backfill(
        _client(holes={101}), [_gap("g1", 100, 102)], tmp_path, max_trades=10, now=lambda: NOW
    )
    path = write_attempts_file(tmp_path, report.outcomes, NOW)

    assert path.parent == tmp_path / "rest" / "trades_attempts" / "dt=2026-11-19"
    [record] = [json.loads(line) for line in path.read_text().splitlines()]
    assert record == {
        "fetched_at": "2026-11-19T10:00:00Z",
        "gap_id": "g1",
        "product_id": "BTC-USD",
        "from_trade_id": 100,
        "to_trade_id": 102,
        "requested": 3,
        "received": 2,
        "missing": 1,
    }
    assert write_attempts_file(tmp_path, [], NOW) is None


def test_backfill_args() -> None:
    today = date(2026, 11, 19)
    args = _parse_args(["backfill", "--warehouse-id", "wh"], today)
    assert (args.catalog, args.max_gaps, args.max_trades, args.dry_run) == (
        "crypto_dev",
        200,
        2_000_000,
        False,
    )
    with pytest.raises(SystemExit):
        _parse_args(["backfill", "--warehouse-id", "wh", "--max-trades", "0"], today)


def test_backfill_requires_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    with pytest.raises(SystemExit):
        _parse_args(["backfill"], date(2026, 11, 19))
