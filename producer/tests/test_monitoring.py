from datetime import UTC, datetime, timedelta

from crypto_recon_producer.monitoring import GapTracker, HeartbeatTimeout, MetricsWindow, TradeGap

T0 = datetime(2026, 11, 10, 12, 0, 0, tzinfo=UTC)


def _at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _tracker(products: tuple[str, ...] = ("BTC-USD",)) -> GapTracker:
    tracker = GapTracker(products)
    tracker.start(T0)
    return tracker


def _gaps(events: list) -> list[TradeGap]:
    return [e for e in events if isinstance(e, TradeGap)]


def test_no_gap_when_heartbeat_matches_trades() -> None:
    tracker = _tracker()
    tracker.on_trade("BTC-USD", 10)
    tracker.on_heartbeat("BTC-USD", 10, _at(1))
    assert _gaps(tracker.check(_at(10))) == []


def test_gap_reported_after_grace_period() -> None:
    tracker = _tracker()
    tracker.on_trade("BTC-USD", 10)
    tracker.on_heartbeat("BTC-USD", 12, _at(1))

    assert _gaps(tracker.check(_at(5.9))) == []
    tracker.on_heartbeat("BTC-USD", 12, _at(5.95))
    [gap] = _gaps(tracker.check(_at(6)))
    assert (gap.from_trade_id, gap.to_trade_id, gap.missing_count) == (11, 12, 2)


def test_late_trades_within_grace_clear_the_suspicion() -> None:
    tracker = _tracker()
    tracker.on_trade("BTC-USD", 10)
    tracker.on_heartbeat("BTC-USD", 12, _at(1))
    tracker.on_trade("BTC-USD", 11)
    tracker.on_trade("BTC-USD", 12)
    assert _gaps(tracker.check(_at(10))) == []


def test_gap_is_reported_once_and_only_new_ranges_after() -> None:
    tracker = _tracker()
    tracker.on_trade("BTC-USD", 10)
    tracker.on_heartbeat("BTC-USD", 12, _at(1))
    assert len(_gaps(tracker.check(_at(6)))) == 1

    tracker.on_heartbeat("BTC-USD", 12, _at(7))
    assert _gaps(tracker.check(_at(20))) == []

    tracker.on_heartbeat("BTC-USD", 15, _at(21))
    [gap] = _gaps(tracker.check(_at(26)))
    assert (gap.from_trade_id, gap.to_trade_id) == (13, 15)


def test_no_baseline_before_first_trade() -> None:
    tracker = _tracker()
    tracker.on_heartbeat("BTC-USD", 99, _at(1))
    assert _gaps(tracker.check(_at(10))) == []


def test_heartbeat_timeout_reported_once_per_silence() -> None:
    tracker = _tracker(("BTC-USD", "ETH-USD"))
    tracker.on_heartbeat("BTC-USD", 1, _at(1))

    [timeout] = tracker.check(_at(5.5))
    assert isinstance(timeout, HeartbeatTimeout)
    assert timeout.product_id == "ETH-USD"
    assert tracker.check(_at(5.6)) == []

    tracker.on_heartbeat("ETH-USD", 1, _at(6))
    assert [e.product_id for e in tracker.check(_at(6.5))] == ["BTC-USD"]


def test_start_resets_heartbeat_timers() -> None:
    tracker = _tracker()
    tracker.start(_at(100))
    assert tracker.check(_at(104)) == []


def test_metrics_snapshot_and_reset() -> None:
    window = MetricsWindow(run_id="r1", started_at=T0)
    window.messages = 60
    window.landed = 59
    window.rejected = 1
    window.latencies_ms = [float(v) for v in range(1, 101)]

    record = window.snapshot(_at(30))
    assert record["msgs_per_sec"] == 2.0
    assert (record["latency_p50_ms"], record["latency_p95_ms"]) == (50.0, 95.0)
    assert record["window_start"] == "2026-11-10T12:00:00.000000Z"

    empty = window.snapshot(_at(60))
    assert (empty["messages"], empty["latency_p50_ms"]) == (0, None)
    assert empty["window_start"] == "2026-11-10T12:00:30.000000Z"
