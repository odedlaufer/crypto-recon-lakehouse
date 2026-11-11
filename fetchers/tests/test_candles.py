import itertools
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from crypto_recon_fetchers.candles import (
    BASE_URL,
    CandlesClient,
    CandlesError,
    Window,
    day_windows,
    write_candles_file,
)

DAY = date(2026, 11, 10)
DAY_START = int(datetime(2026, 11, 10, tzinfo=UTC).timestamp())


def _row(minute: int, close: str = "90015") -> str:
    t = DAY_START + 60 * minute
    return f"[{t},90001.5,90020,90010.250,{close},1.20000000]"


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 11, 10, hour, minute, tzinfo=UTC)


def _client(handler) -> tuple[CandlesClient, list[float]]:
    sleeps: list[float] = []
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url=BASE_URL)
    ticks = itertools.count(step=10.0)  # requests far apart: throttling never sleeps
    return CandlesClient(http, sleep=sleeps.append, monotonic=lambda: next(ticks)), sleeps


def test_day_windows_cover_the_day_in_five_requests() -> None:
    windows = day_windows(DAY)
    assert len(windows) == 5
    assert windows[0] == Window(_at(0, 0), _at(4, 59))
    assert windows[-1].end == _at(23, 59)
    for previous, current in itertools.pairwise(windows):
        assert current.start == previous.end + timedelta(minutes=1)
    assert all((w.end - w.start) // timedelta(minutes=1) + 1 <= 300 for w in windows)


def test_fetch_window_sends_params_filters_sorts_and_keeps_raw_digits() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = f"[{_row(2)},{_row(1)},{_row(-1)},{_row(1)}]"  # newest first, one before start
        return httpx.Response(200, text=body)

    client, _ = _client(handler)
    window = Window(_at(0, 0), _at(0, 2))
    rows = client.fetch_window("BTC-USD", window)

    params = seen[0].url.params
    assert seen[0].url.path == "/products/BTC-USD/candles"
    assert (params["granularity"], params["start"], params["end"]) == (
        "60",
        "2026-11-10T00:00:00Z",
        "2026-11-10T00:02:00Z",
    )
    assert [r.time - DAY_START for r in rows] == [60, 120]
    assert rows[0].raw == _row(1)  # 90010.250 and 1.20000000 kept exactly


def test_fetch_day_makes_five_requests() -> None:
    client, _ = _client(lambda request: httpx.Response(200, text="[]"))
    assert client.fetch_day("BTC-USD", DAY) == []
    assert client.requests == 5


def test_retries_429_then_succeeds_using_retry_after() -> None:
    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "2"}),
            httpx.Response(503),
            httpx.Response(200, text=f"[{_row(0)}]"),
        ]
    )
    client, sleeps = _client(lambda request: next(responses))
    window = Window(_at(0, 0), _at(0, 0))

    assert len(client.fetch_window("BTC-USD", window)) == 1
    assert sleeps == [2.0, 2.0]  # Retry-After, then exponential backoff for attempt 2


def test_gives_up_after_max_attempts() -> None:
    client, _ = _client(lambda request: httpx.Response(500))
    with pytest.raises(CandlesError, match="giving up after 5 attempts"):
        client.fetch_window("BTC-USD", day_windows(DAY)[0])


def test_client_errors_are_not_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(400, json={"message": "invalid granularity"})

    client, _ = _client(handler)
    with pytest.raises(CandlesError, match="HTTP 400"):
        client.fetch_window("BTC-USD", day_windows(DAY)[0])
    assert len(calls) == 1


@pytest.mark.parametrize(
    "body", ['{"message": "NotFound"}', "[[1, 2]]", "not json"], ids=["object", "short", "text"]
)
def test_malformed_responses_raise(body: str) -> None:
    client, _ = _client(lambda request: httpx.Response(200, text=body))
    with pytest.raises(CandlesError):
        client.fetch_window("BTC-USD", day_windows(DAY)[0])


def test_throttle_spaces_requests() -> None:
    clock = iter([0.0, 0.1, 0.35, 1.0, 1.0])
    sleeps: list[float] = []
    http = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text="[]")), base_url=BASE_URL
    )
    client = CandlesClient(http, sleep=sleeps.append, monotonic=lambda: next(clock))
    window = day_windows(DAY)[0]
    client.fetch_window("BTC-USD", window)
    client.fetch_window("BTC-USD", window)
    client.fetch_window("BTC-USD", window)
    assert sleeps == [pytest.approx(0.25)]


def test_write_candles_file(tmp_path: Path) -> None:
    client, _ = _client(lambda request: httpx.Response(200, text=f"[{_row(1)},{_row(0)}]"))
    rows = client.fetch_window("BTC-USD", day_windows(DAY)[0])
    fetched_at = datetime(2026, 11, 11, 0, 30, 5, tzinfo=UTC)

    path = write_candles_file(tmp_path, "BTC-USD", DAY, rows, fetched_at)

    relative = path.relative_to(tmp_path)
    assert relative.parts[:3] == ("rest", "candles", "dt=2026-11-10")
    assert relative.name.startswith("candles_BTC-USD_20261110_20261111T003005Z_")
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert records[0] == {
        "fetched_at": "2026-11-11T00:30:05Z",
        "product_id": "BTC-USD",
        "granularity": 60,
        "raw": _row(0),
    }
    assert len(records) == 2
    assert list(tmp_path.rglob("*.tmp")) == []
