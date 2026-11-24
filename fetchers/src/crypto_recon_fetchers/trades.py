"""Backfill trades from Coinbase REST by trade-id range (FR-B4, ADR-003).

GET /products/{product_id}/trades?after=X returns the trades with ids just below X,
newest first, up to `limit` (max 1,000). A gap [from_trade_id, to_trade_id] is fetched by
starting at after = to_trade_id + 1 and moving the cursor down to the lowest id received,
until from_trade_id is covered. Ids that REST does not return are reported as missing.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from crypto_recon_fetchers.rest import RestClient, RestError

PAGE_SIZE = 1000
CHUNK_TRADES = 100_000


@dataclass(frozen=True)
class TradeRange:
    """An inclusive range of missing trade ids for one product."""

    product_id: str
    from_trade_id: int
    to_trade_id: int

    @property
    def size(self) -> int:
        return self.to_trade_id - self.from_trade_id + 1


@dataclass(frozen=True)
class RangeResult:
    range: TradeRange
    trades: list[dict] = field(default_factory=list)  # ascending trade_id, within range

    @property
    def received(self) -> int:
        return len(self.trades)

    @property
    def missing(self) -> int:
        return self.range.size - self.received


def _parse_trades(body: str) -> list[dict]:
    try:
        trades = json.loads(body)
    except ValueError as exc:
        raise RestError(f"response is not JSON: {body[:200]!r}") from exc
    if not isinstance(trades, list):
        raise RestError(f"expected a list of trades, got: {body[:200]!r}")
    for trade in trades:
        if not (isinstance(trade, dict) and isinstance(trade.get("trade_id"), int)):
            raise RestError(f"malformed trade: {trade!r}")
    return trades


class TradesClient(RestClient):
    """Fetches trades by id range with throttling and retries (FR-B2, FR-B4)."""

    page_size = PAGE_SIZE

    def fetch_range(self, trade_range: TradeRange) -> RangeResult:
        found: dict[int, dict] = {}
        cursor = trade_range.to_trade_id + 1
        while cursor > trade_range.from_trade_id:
            limit = min(self.page_size, cursor - trade_range.from_trade_id)
            page = _parse_trades(
                self._get(
                    f"/products/{trade_range.product_id}/trades",
                    {"after": str(cursor), "limit": str(limit)},
                )
            )
            if not page:
                break
            for trade in page:
                trade_id = trade["trade_id"]
                if trade_range.from_trade_id <= trade_id <= trade_range.to_trade_id:
                    found[trade_id] = trade
            lowest = min(trade["trade_id"] for trade in page)
            if lowest >= cursor:
                raise RestError(f"cursor did not move below {cursor} for {trade_range}")
            cursor = lowest
        return RangeResult(trade_range, [found[k] for k in sorted(found)])


def write_trades_files(
    output_dir: Path,
    result: RangeResult,
    gap_id: str,
    day: date,
    fetched_at: datetime,
    chunk_trades: int = CHUNK_TRADES,
) -> list[Path]:
    """Write the fetched trades of one gap (see write_batch_files)."""
    return write_batch_files(output_dir, [(gap_id, result)], day, fetched_at, chunk_trades)


def write_batch_files(
    output_dir: Path,
    batch: list[tuple[str, RangeResult]],
    day: date,
    fetched_at: datetime,
    chunk_trades: int = CHUNK_TRADES,
) -> list[Path]:
    """Write the fetched trades of one or more gaps of one product as JSONL, atomically.

    Nearby gaps fetched together share files, so many small gaps do not become many small
    files (ADR-011). Each line keeps its own gap_id.

    Layout: {output_dir}/rest/trades/dt=YYYY-MM-DD/
            trades_{product}_{from}-{to}_{fetched}_{uuid}_{part}.jsonl
    One line per trade: {"fetched_at", "product_id", "gap_id", "raw"}.
    """
    rows = [(gap_id, trade) for gap_id, result in batch for trade in result.trades]
    if not rows:
        return []
    product = batch[0][1].range.product_id
    low = min(result.range.from_trade_id for _, result in batch)
    high = max(result.range.to_trade_id for _, result in batch)
    fetched = fetched_at.astimezone(UTC)
    fetched_iso = fetched.isoformat(timespec="seconds").replace("+00:00", "Z")
    directory = output_dir / "rest" / "trades" / f"dt={day:%Y-%m-%d}"
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"trades_{product}_{low}-{high}_{fetched:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex}"

    paths: list[Path] = []
    for part, start in enumerate(range(0, len(rows), chunk_trades)):
        lines = [
            json.dumps(
                {
                    "fetched_at": fetched_iso,
                    "product_id": product,
                    "gap_id": gap_id,
                    "raw": json.dumps(trade, separators=(",", ":")),
                }
            )
            for gap_id, trade in rows[start : start + chunk_trades]
        ]
        final_path = directory / f"{stem}_{part:03d}.jsonl"
        tmp_path = directory / f"{final_path.name}.tmp"
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, final_path)
        paths.append(final_path)
    return paths
