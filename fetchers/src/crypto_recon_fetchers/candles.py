"""Official 1-minute candles from Coinbase REST (FR-B1, FR-B2).

GET /products/{product_id}/candles returns at most 300 candles per request. Start and end
are inclusive, so one request covers 300 one-minute buckets (a 299-minute span) and a UTC
day takes 5 requests. Rows are [time, low, high, open, close, volume], newest first. A
response may include buckets before `start`, and minutes with no trades are omitted.

Numbers are parsed as Decimal and written back with their original digits, so the landed
`raw` row matches what Coinbase sent; no float rounding.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from crypto_recon_fetchers.rest import BASE_URL, RestClient, RestError

__all__ = [
    "BASE_URL",
    "CandleRow",
    "CandlesClient",
    "CandlesError",
    "Window",
    "day_windows",
    "write_candles_file",
]

GRANULARITY_SECONDS = 60
MAX_CANDLES_PER_REQUEST = 300

CandlesError = RestError


@dataclass(frozen=True)
class Window:
    """An inclusive range of bucket start times for one request."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class CandleRow:
    time: int  # bucket start, epoch seconds
    raw: str  # the row as Coinbase sent it, e.g. [1762732800,90001.5,90020,90010.25,90015,1.2]


def day_windows(
    day: date,
    *,
    granularity: int = GRANULARITY_SECONDS,
    max_candles: int = MAX_CANDLES_PER_REQUEST,
) -> list[Window]:
    """Inclusive request windows covering one UTC day, at most `max_candles` buckets each."""
    step = timedelta(seconds=granularity)
    first = datetime(day.year, day.month, day.day, tzinfo=UTC)
    last = first + timedelta(days=1) - step
    windows: list[Window] = []
    start = first
    while start <= last:
        end = min(start + step * (max_candles - 1), last)
        windows.append(Window(start, end))
        start = end + step
    return windows


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_text(row: list[int | Decimal]) -> str:
    return "[" + ",".join(str(value) for value in row) + "]"


def _parse_rows(body: str) -> list[list[int | Decimal]]:
    try:
        rows = json.loads(body, parse_float=Decimal)
    except ValueError as exc:
        raise CandlesError(f"response is not JSON: {body[:200]!r}") from exc
    if not isinstance(rows, list):
        raise CandlesError(f"expected a list of candles, got: {body[:200]!r}")
    for row in rows:
        if not (isinstance(row, list) and len(row) == 6 and isinstance(row[0], int)):
            raise CandlesError(f"malformed candle row: {row!r}")
    return rows


class CandlesClient(RestClient):
    """Fetches candles with throttling and retries on 429, 5xx and network errors (FR-B2)."""

    def fetch_window(self, product_id: str, window: Window) -> list[CandleRow]:
        """Candles for one window, oldest first, restricted to the requested range."""
        params = {
            "granularity": str(GRANULARITY_SECONDS),
            "start": _iso(window.start),
            "end": _iso(window.end),
        }
        rows = _parse_rows(self._get(f"/products/{product_id}/candles", params))
        low, high = int(window.start.timestamp()), int(window.end.timestamp())
        by_time = {
            row[0]: CandleRow(row[0], _row_text(row)) for row in rows if low <= row[0] <= high
        }
        return [by_time[t] for t in sorted(by_time)]

    def fetch_day(self, product_id: str, day: date) -> list[CandleRow]:
        """All candles for one product and UTC day, oldest first."""
        rows: list[CandleRow] = []
        for window in day_windows(day):
            rows.extend(self.fetch_window(product_id, window))
        return rows


def write_candles_file(
    output_dir: Path,
    product_id: str,
    day: date,
    rows: list[CandleRow],
    fetched_at: datetime,
) -> Path:
    """Write one product-day of candles as JSONL, atomically.

    Layout: {output_dir}/rest/candles/dt=YYYY-MM-DD/candles_{product}_{day}_{fetched}_{uuid}.jsonl
    Every fetch writes a new file, so re-fetches (Q5) never overwrite earlier ones.
    """
    fetched = fetched_at.astimezone(UTC)
    directory = output_dir / "rest" / "candles" / f"dt={day:%Y-%m-%d}"
    directory.mkdir(parents=True, exist_ok=True)
    name = f"candles_{product_id}_{day:%Y%m%d}_{fetched:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex}.jsonl"
    final_path = directory / name
    tmp_path = directory / f"{name}.tmp"

    fetched_iso = fetched.isoformat(timespec="seconds").replace("+00:00", "Z")
    lines = [
        json.dumps(
            {
                "fetched_at": fetched_iso,
                "product_id": product_id,
                "granularity": GRANULARITY_SECONDS,
                "raw": row.raw,
            }
        )
        for row in rows
    ]
    with tmp_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_path)
    return final_path
