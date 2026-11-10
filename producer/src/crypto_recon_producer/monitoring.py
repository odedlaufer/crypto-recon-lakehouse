"""Producer health: heartbeat gap detection (FR-P5, DQ-02) and per-window metrics (FR-P6).

Gap detection compares each heartbeat's `last_trade_id` with the highest trade id seen on
the matches channel. If the heartbeat stays ahead for longer than a grace period (trades
can arrive slightly after the heartbeat that announces them), the missing range is
reported as a trade gap. These events are telemetry; the authoritative gap list is built
later from silver by the gap detector job (FR-J2).
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class TradeGap:
    product_id: str
    from_trade_id: int
    to_trade_id: int
    detected_at: datetime

    @property
    def missing_count(self) -> int:
        return self.to_trade_id - self.from_trade_id + 1


@dataclass(frozen=True)
class HeartbeatTimeout:
    product_id: str
    last_heartbeat_at: datetime
    detected_at: datetime


HealthEvent = TradeGap | HeartbeatTimeout


class GapTracker:
    """Tracks, per product, the last trade id seen against heartbeat `last_trade_id`."""

    def __init__(
        self,
        products: Iterable[str],
        *,
        grace: timedelta = timedelta(seconds=5),
        heartbeat_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        self._products = tuple(products)
        self._grace = grace
        self._heartbeat_timeout = heartbeat_timeout
        self._last_trade: dict[str, int] = {}
        self._reported_upto: dict[str, int] = {}
        self._pending: dict[str, tuple[int, datetime]] = {}
        self._last_heartbeat: dict[str, datetime] = {}
        self._timed_out: set[str] = set()

    def start(self, now: datetime) -> None:
        """Call on every (re)connect: heartbeat timers restart and open suspicions reset."""
        self._pending.clear()
        self._timed_out.clear()
        for product in self._products:
            self._last_heartbeat[product] = now

    def on_trade(self, product_id: str, trade_id: int) -> None:
        if trade_id > self._last_trade.get(product_id, -1):
            self._last_trade[product_id] = trade_id
        pending = self._pending.get(product_id)
        if pending is not None and self._last_trade[product_id] >= pending[0]:
            del self._pending[product_id]

    def on_heartbeat(self, product_id: str, last_trade_id: int, now: datetime) -> None:
        self._last_heartbeat[product_id] = now
        self._timed_out.discard(product_id)

        seen = self._last_trade.get(product_id)
        if seen is None:
            return  # no baseline until the first trade (last_match) arrives
        known = max(seen, self._reported_upto.get(product_id, seen))
        if last_trade_id <= known:
            self._pending.pop(product_id, None)
            return
        since = self._pending[product_id][1] if product_id in self._pending else now
        self._pending[product_id] = (last_trade_id, since)

    def check(self, now: datetime) -> list[HealthEvent]:
        """Return gaps past their grace period and products whose heartbeat went silent."""
        events: list[HealthEvent] = []

        for product_id, (target, since) in list(self._pending.items()):
            if now - since < self._grace:
                continue
            seen = self._last_trade[product_id]
            start = max(seen, self._reported_upto.get(product_id, seen)) + 1
            if target >= start:
                events.append(TradeGap(product_id, start, target, now))
            self._reported_upto[product_id] = target
            del self._pending[product_id]

        for product_id in self._products:
            last = self._last_heartbeat.get(product_id)
            if last is None or product_id in self._timed_out:
                continue
            if now - last > self._heartbeat_timeout:
                events.append(HeartbeatTimeout(product_id, last, now))
                self._timed_out.add(product_id)

        return events


def _percentile(sorted_values: list[float], q: float) -> float | None:
    """Nearest-rank percentile; None when there are no values."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(q * len(sorted_values)))
    return sorted_values[rank - 1]


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass
class MetricsWindow:
    """Counters for one metrics window; `snapshot()` returns a record and starts a new one."""

    run_id: str
    started_at: datetime
    messages: int = 0
    landed: int = 0
    rejected: int = 0
    reconnects: int = 0
    trade_gaps: int = 0
    heartbeat_gaps: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def snapshot(self, now: datetime) -> dict[str, object]:
        seconds = max((now - self.started_at).total_seconds(), 1e-9)
        latencies = sorted(self.latencies_ms)
        p50 = _percentile(latencies, 0.50)
        p95 = _percentile(latencies, 0.95)
        record: dict[str, object] = {
            "run_id": self.run_id,
            "window_start": _iso(self.started_at),
            "window_end": _iso(now),
            "window_seconds": round(seconds, 3),
            "messages": self.messages,
            "msgs_per_sec": round(self.messages / seconds, 3),
            "landed": self.landed,
            "rejected": self.rejected,
            "reconnects": self.reconnects,
            "trade_gaps": self.trade_gaps,
            "heartbeat_gaps": self.heartbeat_gaps,
            "latency_p50_ms": None if p50 is None else round(p50, 3),
            "latency_p95_ms": None if p95 is None else round(p95, 3),
        }
        self.started_at = now
        self.messages = self.landed = self.rejected = self.reconnects = 0
        self.trade_gaps = self.heartbeat_gaps = 0
        self.latencies_ms = []
        return record
