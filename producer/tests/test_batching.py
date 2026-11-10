import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crypto_recon_producer.batching import (
    HEARTBEAT_CHANNEL,
    MATCHES_CHANNEL,
    Batcher,
    channel_for,
    landed_record,
    rejected_record,
)
from crypto_recon_producer.messages import HeartbeatMessage, MatchMessage, parse_message

T0 = datetime(2026, 11, 10, 12, 59, 50, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _batcher(tmp_path: Path, clock: FakeClock, max_messages: int = 3) -> Batcher:
    return Batcher(tmp_path, max_messages=max_messages, max_age=timedelta(seconds=30), clock=clock)


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_flushes_when_batch_is_full(tmp_path: Path) -> None:
    batcher = _batcher(tmp_path, FakeClock(T0))
    assert batcher.add(MATCHES_CHANNEL, "BTC-USD", "a") == []
    assert batcher.add(MATCHES_CHANNEL, "BTC-USD", "b") == []
    [flushed] = batcher.add(MATCHES_CHANNEL, "BTC-USD", "c")

    assert flushed.message_count == 3
    assert _lines(flushed.path) == ["a", "b", "c"]
    assert batcher.pending_messages == 0


def test_file_layout_and_name(tmp_path: Path) -> None:
    batcher = _batcher(tmp_path, FakeClock(T0), max_messages=1)
    [flushed] = batcher.add(MATCHES_CHANNEL, "BTC-USD", "a")

    relative = flushed.path.relative_to(tmp_path)
    assert relative.parts[:4] == ("ws", "matches", "dt=2026-11-10", "hr=12")
    assert relative.name.startswith("matches_BTC-USD_20261110T125950Z_")
    assert relative.suffix == ".jsonl"


def test_partition_uses_batch_start_time(tmp_path: Path) -> None:
    clock = FakeClock(T0)
    batcher = _batcher(tmp_path, clock, max_messages=2)
    batcher.add(MATCHES_CHANNEL, "BTC-USD", "a")
    clock.advance(20)  # crosses into hour 13
    [flushed] = batcher.add(MATCHES_CHANNEL, "BTC-USD", "b")
    assert "hr=12" in flushed.path.parts


def test_flushes_when_batch_is_old(tmp_path: Path) -> None:
    clock = FakeClock(T0)
    batcher = _batcher(tmp_path, clock)
    batcher.add(MATCHES_CHANNEL, "BTC-USD", "a")

    clock.advance(29)
    assert batcher.flush_due() == []
    clock.advance(1)
    [flushed] = batcher.flush_due()
    assert _lines(flushed.path) == ["a"]


def test_add_flushes_other_old_batches(tmp_path: Path) -> None:
    clock = FakeClock(T0)
    batcher = _batcher(tmp_path, clock)
    batcher.add(MATCHES_CHANNEL, "BTC-USD", "a")
    clock.advance(30)
    [flushed] = batcher.add(MATCHES_CHANNEL, "ETH-USD", "b")
    assert flushed.product_id == "BTC-USD"
    assert batcher.pending_messages == 1


def test_batches_are_separate_per_channel_and_product(tmp_path: Path) -> None:
    batcher = _batcher(tmp_path, FakeClock(T0))
    batcher.add(MATCHES_CHANNEL, "BTC-USD", "m-btc")
    batcher.add(MATCHES_CHANNEL, "ETH-USD", "m-eth")
    batcher.add(HEARTBEAT_CHANNEL, "BTC-USD", "h-btc")

    flushed = batcher.flush_all()
    contents = {(f.channel, f.product_id): _lines(f.path) for f in flushed}
    assert contents == {
        (MATCHES_CHANNEL, "BTC-USD"): ["m-btc"],
        (MATCHES_CHANNEL, "ETH-USD"): ["m-eth"],
        (HEARTBEAT_CHANNEL, "BTC-USD"): ["h-btc"],
    }


def test_flush_all_leaves_no_temp_files(tmp_path: Path) -> None:
    batcher = _batcher(tmp_path, FakeClock(T0))
    batcher.add(MATCHES_CHANNEL, "BTC-USD", "a")
    batcher.flush_all()
    assert list(tmp_path.rglob("*.tmp")) == []
    assert batcher.flush_all() == []


def test_unsafe_product_id_is_sanitized_in_path(tmp_path: Path) -> None:
    batcher = _batcher(tmp_path, FakeClock(T0), max_messages=1)
    [flushed] = batcher.add(MATCHES_CHANNEL, "../../etc", "a")
    assert flushed.path.is_relative_to(tmp_path / "ws" / "matches")
    assert "/" not in flushed.path.name.removeprefix("matches_")


@pytest.mark.parametrize(
    "kwargs", [{"max_messages": 0}, {"max_age": timedelta(0)}], ids=["size", "age"]
)
def test_rejects_invalid_limits(tmp_path: Path, kwargs: dict) -> None:
    with pytest.raises(ValueError):
        Batcher(tmp_path, **kwargs)


def test_landed_record_keeps_raw_message_unchanged() -> None:
    raw = '{"type": "match",  "price": "400.23"}'
    record = json.loads(landed_record(raw, T0))
    assert record == {"received_at": "2026-11-10T12:59:50.000000Z", "raw": raw}
    assert json.loads(landed_record(raw.encode(), T0))["raw"] == raw


def test_rejected_record_includes_error() -> None:
    record = json.loads(rejected_record(b"\xffnot json", T0, "not valid JSON"))
    assert record["error"] == "not valid JSON"
    assert record["raw"].endswith("not json")


def test_channel_for() -> None:
    match = parse_message(
        '{"type": "match", "trade_id": 1, "sequence": 1, "maker_order_id": "a",'
        ' "taker_order_id": "b", "time": "2026-11-10T12:00:00Z", "product_id": "BTC-USD",'
        ' "size": "1", "price": "1", "side": "buy"}'
    )
    heartbeat = parse_message(
        '{"type": "heartbeat", "sequence": 1, "last_trade_id": 1, "product_id": "BTC-USD",'
        ' "time": "2026-11-10T12:00:00Z"}'
    )
    assert isinstance(match, MatchMessage)
    assert isinstance(heartbeat, HeartbeatMessage)
    assert channel_for(match) == MATCHES_CHANNEL
    assert channel_for(heartbeat) == HEARTBEAT_CHANNEL
