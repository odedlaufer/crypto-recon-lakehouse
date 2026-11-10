"""Coinbase WebSocket client (FR-P1, FR-P4, FR-P5, FR-P6, FR-P8).

Subscribes to `matches` and `heartbeat` for the configured products, routes every raw
message to the batcher, checks heartbeat gaps, writes a metrics record per window,
reconnects with exponential backoff (1 s to 60 s), and flushes everything on shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from crypto_recon_producer.batching import (
    METRICS_CHANNEL,
    REJECTED_CHANNEL,
    UNKNOWN_PRODUCT,
    Batcher,
    FlushedFile,
    channel_for,
    landed_record,
    rejected_record,
    utc_now,
)
from crypto_recon_producer.messages import (
    ErrorMessage,
    HeartbeatMessage,
    InvalidMessageError,
    MatchMessage,
    SubscriptionsMessage,
    parse_message,
)
from crypto_recon_producer.monitoring import GapTracker, HeartbeatTimeout, MetricsWindow, TradeGap

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
CHANNELS = ("matches", "heartbeat")
METRICS_PRODUCT = "producer"


def subscribe_message(products: Iterable[str]) -> str:
    """The subscribe request. Coinbase disconnects if none arrives within 5 seconds."""
    return json.dumps(
        {"type": "subscribe", "product_ids": list(products), "channels": list(CHANNELS)}
    )


def backoff_delays(initial: float = 1.0, cap: float = 60.0) -> Iterator[float]:
    """Exponential backoff: 1, 2, 4, ... seconds, capped at 60 (FR-P4)."""
    delay = initial
    while True:
        yield delay
        delay = min(delay * 2, cap)


@dataclass
class ProducerStats:
    """Totals for the whole run."""

    messages: int = 0
    landed: int = 0
    rejected: int = 0
    ignored: int = 0
    server_errors: int = 0
    reconnects: int = 0
    trade_gaps: int = 0
    heartbeat_gaps: int = 0
    files_flushed: int = 0


class Producer:
    """Connects, subscribes and lands messages until `stop` is set."""

    def __init__(
        self,
        products: Iterable[str],
        batcher: Batcher,
        *,
        url: str = COINBASE_WS_URL,
        clock: Callable[[], datetime] = utc_now,
        reconnect_delays: Callable[[], Iterator[float]] = backoff_delays,
        tick_seconds: float = 1.0,
        metrics_interval: timedelta = timedelta(seconds=30),
        on_flush: Callable[[list[FlushedFile]], None] | None = None,
    ) -> None:
        self._products = tuple(products)
        self._batcher = batcher
        self._url = url
        self._clock = clock
        self._reconnect_delays = reconnect_delays
        self._tick_seconds = tick_seconds
        self._metrics_interval = metrics_interval
        self._on_flush = on_flush
        self.run_id = uuid.uuid4().hex
        self.stats = ProducerStats()
        self._gaps = GapTracker(self._products)
        self._gaps.start(clock())
        self._window = MetricsWindow(run_id=self.run_id, started_at=clock())

    def handle_raw(self, raw: str | bytes) -> None:
        """Validate one raw message, update gap tracking and metrics, route it to a batch."""
        received_at = self._clock()
        self.stats.messages += 1
        self._window.messages += 1
        try:
            message = parse_message(raw)
        except InvalidMessageError as exc:
            self.stats.rejected += 1
            self._window.rejected += 1
            line = rejected_record(raw, received_at, str(exc))
            self._emit(self._batcher.add(REJECTED_CHANNEL, UNKNOWN_PRODUCT, line))
            return

        if isinstance(message, MatchMessage):
            self._gaps.on_trade(message.product_id, message.trade_id)
            latency_ms = (received_at - message.time).total_seconds() * 1000
            self._window.latencies_ms.append(latency_ms)
        elif isinstance(message, HeartbeatMessage):
            self._gaps.on_heartbeat(message.product_id, message.last_trade_id, received_at)

        if isinstance(message, MatchMessage | HeartbeatMessage):
            self.stats.landed += 1
            self._window.landed += 1
            line = landed_record(raw, received_at)
            self._emit(self._batcher.add(channel_for(message), message.product_id, line))
        elif isinstance(message, SubscriptionsMessage):
            channels = {c.name: list(c.product_ids) for c in message.channels}
            logger.info("subscribed: %s", channels)
        elif isinstance(message, ErrorMessage):
            self.stats.server_errors += 1
            logger.error("server error: %s (reason: %s)", message.message, message.reason)
        else:
            self.stats.ignored += 1

    def tick(self) -> None:
        """Periodic work while connected: health checks, metrics, age-based flushes."""
        now = self._clock()
        for event in self._gaps.check(now):
            if isinstance(event, TradeGap):
                self.stats.trade_gaps += 1
                self._window.trade_gaps += 1
                logger.warning(
                    "trade gap: %s trade_ids %d..%d (%d missing)",
                    event.product_id,
                    event.from_trade_id,
                    event.to_trade_id,
                    event.missing_count,
                )
            elif isinstance(event, HeartbeatTimeout):
                self.stats.heartbeat_gaps += 1
                self._window.heartbeat_gaps += 1
                logger.warning(
                    "no heartbeat for %s since %s", event.product_id, event.last_heartbeat_at
                )
        if now - self._window.started_at >= self._metrics_interval:
            self._write_metrics(now)
        self._emit(self._batcher.flush_due())

    async def run(self, stop: asyncio.Event) -> None:
        """Run until `stop` is set, reconnecting on any connection failure."""
        delays = self._reconnect_delays()
        try:
            while not stop.is_set():
                try:
                    async with connect(self._url) as ws:
                        await ws.send(subscribe_message(self._products))
                        logger.info("connected to %s, products=%s", self._url, self._products)
                        self._gaps.start(self._clock())
                        delays = self._reconnect_delays()
                        await self._consume(ws, stop)
                except (OSError, TimeoutError, WebSocketException) as exc:
                    if stop.is_set():
                        break
                    delay = next(delays)
                    self.stats.reconnects += 1
                    self._window.reconnects += 1
                    logger.warning(
                        "connection lost (%s: %s); reconnect #%d in %.1fs",
                        type(exc).__name__,
                        exc,
                        self.stats.reconnects,
                        delay,
                    )
                    await _sleep_unless_stopped(stop, delay)
        finally:
            self._write_metrics(self._clock())
            self._emit(self._batcher.flush_all())
            logger.info("stopped: run_id=%s %s", self.run_id, self.stats)

    async def _consume(self, ws: ClientConnection, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                # Cancelling recv() on timeout is safe in websockets: no data is lost.
                raw = await asyncio.wait_for(ws.recv(), timeout=self._tick_seconds)
            except TimeoutError:
                pass
            else:
                self.handle_raw(raw)
            self.tick()

    def _write_metrics(self, now: datetime) -> None:
        record = self._window.snapshot(now)
        logger.info("metrics: %s", record)
        self._emit(self._batcher.add(METRICS_CHANNEL, METRICS_PRODUCT, json.dumps(record)))

    def _emit(self, flushed: list[FlushedFile]) -> None:
        if not flushed:
            return
        self.stats.files_flushed += len(flushed)
        for f in flushed:
            logger.debug("flushed %d messages to %s", f.message_count, f.path)
        if self._on_flush is not None:
            self._on_flush(flushed)


async def _sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
