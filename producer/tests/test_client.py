import asyncio
import itertools
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from websockets.asyncio.server import ServerConnection, serve

from crypto_recon_producer.batching import Batcher
from crypto_recon_producer.client import Producer, backoff_delays, subscribe_message

MATCH = {
    "type": "match",
    "trade_id": 10,
    "sequence": 50,
    "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
    "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
    "time": "2026-11-10T12:00:00.000001Z",
    "product_id": "BTC-USD",
    "size": "0.5",
    "price": "90000.01",
    "side": "sell",
}
HEARTBEAT = {
    "type": "heartbeat",
    "sequence": 51,
    "last_trade_id": 10,
    "product_id": "BTC-USD",
    "time": "2026-11-10T12:00:01Z",
}


def _files(root: Path, channel: str) -> list[Path]:
    return sorted((root / "ws" / channel).rglob("*.jsonl"))


def _raws(path: Path) -> list[str]:
    return [json.loads(line)["raw"] for line in path.read_text(encoding="utf-8").splitlines()]


def _fast_retries() -> Iterator[float]:
    return itertools.repeat(0.01)


def test_subscribe_message() -> None:
    assert json.loads(subscribe_message(["BTC-USD", "ETH-USD"])) == {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "heartbeat"],
    }


def test_backoff_doubles_and_caps_at_60() -> None:
    assert list(itertools.islice(backoff_delays(), 8)) == [1, 2, 4, 8, 16, 32, 60, 60]


def test_handle_raw_routes_messages(tmp_path: Path) -> None:
    batcher = Batcher(tmp_path)
    producer = Producer(["BTC-USD"], batcher)
    producer.handle_raw(json.dumps(MATCH))
    producer.handle_raw(json.dumps(HEARTBEAT))
    producer.handle_raw("garbage")
    producer.handle_raw(json.dumps({"type": "ticker"}))
    producer.handle_raw(json.dumps({"type": "subscriptions", "channels": []}))
    producer.handle_raw(json.dumps({"type": "error", "message": "bad"}))
    batcher.flush_all()

    stats = producer.stats
    assert (stats.messages, stats.landed, stats.rejected) == (6, 2, 1)
    assert (stats.ignored, stats.server_errors) == (1, 1)
    assert _raws(_files(tmp_path, "matches")[0]) == [json.dumps(MATCH)]
    assert _raws(_files(tmp_path, "heartbeat")[0]) == [json.dumps(HEARTBEAT)]
    assert _raws(_files(tmp_path, "_rejected")[0]) == ["garbage"]


def test_run_lands_messages_and_reconnects(tmp_path: Path) -> None:
    asyncio.run(_run_against_local_server(tmp_path))


async def _run_against_local_server(tmp_path: Path) -> None:
    stop = asyncio.Event()
    subscribes: list[dict] = []

    async def handler(ws: ServerConnection) -> None:
        subscribes.append(json.loads(await ws.recv()))
        if len(subscribes) == 1:
            await ws.send(json.dumps({**MATCH, "type": "last_match"}))
            await ws.send(json.dumps(HEARTBEAT))
            await ws.send("garbage")
            return  # server closes the connection: the producer must reconnect
        await ws.send(json.dumps({**MATCH, "trade_id": 11}))
        await asyncio.sleep(0.2)
        stop.set()
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        producer = Producer(
            ["BTC-USD"],
            Batcher(tmp_path),
            url=f"ws://127.0.0.1:{port}",
            reconnect_delays=_fast_retries,
            tick_seconds=0.05,
        )
        await asyncio.wait_for(producer.run(stop), timeout=5)

    assert len(subscribes) == 2
    assert all(s["channels"] == ["matches", "heartbeat"] for s in subscribes)
    assert producer.stats.reconnects == 1

    [matches_file] = _files(tmp_path, "matches")
    trade_ids = [json.loads(raw)["trade_id"] for raw in _raws(matches_file)]
    assert trade_ids == [10, 11]
    assert len(_files(tmp_path, "heartbeat")) == 1
    assert len(_files(tmp_path, "_rejected")) == 1


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def test_tick_reports_heartbeat_gap_and_writes_metrics(tmp_path: Path) -> None:
    clock = FakeClock(datetime(2026, 11, 10, 12, 0, 0, 500000, tzinfo=UTC))
    batcher = Batcher(tmp_path)
    producer = Producer(["BTC-USD"], batcher, clock=clock, metrics_interval=timedelta(seconds=30))

    producer.handle_raw(json.dumps(MATCH))  # trade 10, exchange time 12:00:00.000001
    producer.handle_raw(json.dumps({**HEARTBEAT, "last_trade_id": 13}))
    clock.advance(5)
    producer.handle_raw(json.dumps({**HEARTBEAT, "last_trade_id": 13}))
    producer.tick()
    assert producer.stats.trade_gaps == 1

    clock.advance(25)
    producer.tick()
    batcher.flush_all()

    [metrics_file] = _files(tmp_path, "_metrics")
    [record] = [json.loads(line) for line in metrics_file.read_text().splitlines()]
    assert record["run_id"] == producer.run_id
    assert (record["messages"], record["landed"], record["trade_gaps"]) == (3, 3, 1)
    assert record["msgs_per_sec"] == 0.1
    assert record["latency_p50_ms"] == 499.999
