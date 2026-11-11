import itertools
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from crypto_recon_fetchers.__main__ import _parse_args, fetch_candles
from crypto_recon_fetchers.candles import BASE_URL, CandlesClient

TODAY = date(2026, 11, 11)


def test_defaults_to_yesterday() -> None:
    args = _parse_args(["candles"], today=TODAY)
    assert (args.start, args.end, args.product) == (date(2026, 11, 10), date(2026, 11, 10), None)


def test_accepts_range_and_products() -> None:
    args = _parse_args(
        ["candles", "--start", "2026-11-01", "--end", "2026-11-07", "--product", "BTC-USD"],
        today=TODAY,
    )
    assert (args.start, args.end, args.product) == (
        date(2026, 11, 1),
        date(2026, 11, 7),
        ["BTC-USD"],
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["candles", "--start", "2026-11-05", "--end", "2026-11-04"],
        ["candles", "--start", "2026-11-11", "--end", "2026-11-11"],
    ],
    ids=["start-after-end", "today-not-final"],
)
def test_rejects_invalid_ranges(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(argv, today=TODAY)


def test_fetch_candles_lands_one_file_per_product_day(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        start = datetime.fromisoformat(request.url.params["start"].replace("Z", "+00:00"))
        if request.url.path.startswith("/products/ETH-USD") or start.day != 10:
            return httpx.Response(200, text="[]")
        t = int(start.timestamp())
        return httpx.Response(200, text=f"[[{t},1,2,1.5,1.75,10.5]]")

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url=BASE_URL)
    ticks = itertools.count(step=10.0)
    client = CandlesClient(http, sleep=lambda s: None, monotonic=lambda: next(ticks))

    written, empty = fetch_candles(
        client,
        ["BTC-USD", "ETH-USD"],
        date(2026, 11, 10),
        date(2026, 11, 11),
        tmp_path,
        now=lambda: datetime(2026, 11, 12, 0, 30, tzinfo=UTC),
    )

    assert client.requests == 2 * 2 * 5  # products x days x windows
    assert [p.parent.name for p in written] == ["dt=2026-11-10"]
    lines = written[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5  # one candle per window in the fake
    assert json.loads(lines[0])["product_id"] == "BTC-USD"
    assert empty == [
        ("ETH-USD", date(2026, 11, 10)),
        ("BTC-USD", date(2026, 11, 11)),
        ("ETH-USD", date(2026, 11, 11)),
    ]
