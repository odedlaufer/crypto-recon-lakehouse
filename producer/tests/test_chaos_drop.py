import json
import random
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crypto_recon_producer.batching import Batcher
from crypto_recon_producer.client import Producer

NOW = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _match(trade_id: int) -> str:
    return json.dumps(
        {
            "type": "match",
            "trade_id": trade_id,
            "sequence": trade_id,
            "maker_order_id": "m",
            "taker_order_id": "t",
            "time": "2026-11-23T12:00:00Z",
            "product_id": "BTC-USD",
            "size": "0.01",
            "price": "50000",
            "side": "buy",
        }
    )


def _heartbeat() -> str:
    return json.dumps(
        {
            "type": "heartbeat",
            "sequence": 1,
            "last_trade_id": 1,
            "product_id": "BTC-USD",
            "time": "2026-11-23T12:00:00Z",
        }
    )


def _producer(tmp_path: Path, pct: float) -> Producer:
    batcher = Batcher(tmp_path, clock=lambda: NOW)
    return Producer(
        ["BTC-USD"],
        batcher,
        clock=lambda: NOW,
        chaos_drop_pct=pct,
        rng=random.Random(42),  # noqa: S311
    )


def test_no_drops_by_default(tmp_path: Path) -> None:
    producer = _producer(tmp_path, 0.0)
    for trade_id in range(1, 101):
        producer.handle_raw(_match(trade_id))
    assert producer.stats.chaos_dropped == 0
    assert producer.stats.landed == 100


def test_drops_about_the_requested_share_of_trades_only(tmp_path: Path) -> None:
    producer = _producer(tmp_path, 10.0)
    for trade_id in range(1, 2001):
        producer.handle_raw(_match(trade_id))
    for _ in range(50):
        producer.handle_raw(_heartbeat())

    dropped = producer.stats.chaos_dropped
    assert 150 <= dropped <= 250  # about 10% of 2,000 trades
    assert producer.stats.landed == 2000 - dropped + 50  # heartbeats are never dropped


def test_rejects_invalid_percentage(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _producer(tmp_path, 101.0)
