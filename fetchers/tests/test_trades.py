import itertools
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from crypto_recon_fetchers.rest import BASE_URL, RestError
from crypto_recon_fetchers.trades import (
    RangeResult,
    TradeRange,
    TradesClient,
    write_trades_files,
)


def _trade(trade_id: int) -> dict:
    return {
        "trade_id": trade_id,
        "side": "buy",
        "size": "0.10000000",
        "price": "90000.01",
        "time": "2026-11-14T07:31:57.500000Z",
    }


def _fake_coinbase(holes: set[int] = frozenset(), seen: list | None = None):
    """Serves `after`/`limit` pages like Coinbase: ids just below `after`, newest first."""

    def handler(request: httpx.Request) -> httpx.Response:
        after = int(request.url.params["after"])
        limit = int(request.url.params["limit"])
        if seen is not None:
            seen.append((after, limit))
        ids = [i for i in range(after - 1, max(after - 1 - limit, 0), -1) if i not in holes]
        return httpx.Response(200, json=[_trade(i) for i in ids])

    return handler


def _client(handler, page_size: int = 1000) -> TradesClient:
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url=BASE_URL)
    ticks = itertools.count(step=10.0)
    client = TradesClient(http, sleep=lambda s: None, monotonic=lambda: next(ticks))
    client.page_size = page_size
    return client


def test_small_range_is_one_request() -> None:
    seen: list = []
    client = _client(_fake_coinbase(seen=seen))
    result = client.fetch_range(TradeRange("BTC-USD", 101, 110))

    assert seen == [(111, 10)]
    assert [t["trade_id"] for t in result.trades] == list(range(101, 111))
    assert (result.received, result.missing) == (10, 0)


def test_large_range_pages_down_with_the_cursor() -> None:
    seen: list = []
    client = _client(_fake_coinbase(seen=seen), page_size=4)
    result = client.fetch_range(TradeRange("BTC-USD", 100, 109))

    assert seen == [(110, 4), (106, 4), (102, 2)]
    assert [t["trade_id"] for t in result.trades] == list(range(100, 110))


def test_ids_rest_does_not_have_are_reported_missing() -> None:
    client = _client(_fake_coinbase(holes={103, 104}), page_size=4)
    result = client.fetch_range(TradeRange("BTC-USD", 100, 109))

    assert 103 not in [t["trade_id"] for t in result.trades]
    assert (result.received, result.missing) == (8, 2)


def test_empty_page_stops() -> None:
    client = _client(lambda request: httpx.Response(200, json=[]))
    result = client.fetch_range(TradeRange("BTC-USD", 1, 5))
    assert (result.received, result.missing) == (0, 5)


def test_cursor_that_does_not_move_raises() -> None:
    client = _client(lambda request: httpx.Response(200, json=[_trade(500)]))
    with pytest.raises(RestError, match="cursor did not move"):
        client.fetch_range(TradeRange("BTC-USD", 100, 109))


def test_malformed_trades_raise() -> None:
    client = _client(lambda request: httpx.Response(200, json=[{"id": 1}]))
    with pytest.raises(RestError, match="malformed trade"):
        client.fetch_range(TradeRange("BTC-USD", 1, 5))


def test_write_trades_files_chunks_and_layout(tmp_path: Path) -> None:
    result = RangeResult(TradeRange("ETH-USD", 1, 5), [_trade(i) for i in range(1, 6)])
    fetched_at = datetime(2026, 11, 19, 10, 0, 0, tzinfo=UTC)

    paths = write_trades_files(
        tmp_path, result, "gap-1", date(2026, 11, 14), fetched_at, chunk_trades=2
    )

    assert [p.name.split("_")[-1] for p in paths] == ["000.jsonl", "001.jsonl", "002.jsonl"]
    assert all(p.parent == tmp_path / "rest" / "trades" / "dt=2026-11-14" for p in paths)
    assert paths[0].name.startswith("trades_ETH-USD_1-5_20261119T100000Z_")
    first = json.loads(paths[0].read_text(encoding="utf-8").splitlines()[0])
    assert first["gap_id"] == "gap-1"
    assert first["product_id"] == "ETH-USD"
    assert json.loads(first["raw"])["trade_id"] == 1
    assert sum(len(p.read_text().splitlines()) for p in paths) == 5
    assert list(tmp_path.rglob("*.tmp")) == []


def test_write_nothing_for_empty_result(tmp_path: Path) -> None:
    result = RangeResult(TradeRange("ETH-USD", 1, 5), [])
    assert write_trades_files(tmp_path, result, "g", date(2026, 11, 14), datetime.now(UTC)) == []
