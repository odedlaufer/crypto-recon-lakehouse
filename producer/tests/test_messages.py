import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_recon_producer.messages import (
    ErrorMessage,
    HeartbeatMessage,
    InvalidMessageError,
    MatchMessage,
    SubscriptionsMessage,
    parse_message,
)

# Examples taken from the Coinbase Exchange WebSocket docs.
MATCH = {
    "type": "match",
    "trade_id": 10,
    "sequence": 50,
    "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
    "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
    "time": "2014-11-07T08:19:27.028459Z",
    "product_id": "BTC-USD",
    "size": "5.23512",
    "price": "400.23",
    "side": "sell",
}
HEARTBEAT = {
    "type": "heartbeat",
    "sequence": 90,
    "last_trade_id": 20,
    "product_id": "BTC-USD",
    "time": "2014-11-07T08:19:28.464459Z",
}
SUBSCRIPTIONS = {
    "type": "subscriptions",
    "channels": [
        {"name": "matches", "product_ids": ["BTC-USD", "ETH-USD"]},
        {"name": "heartbeat", "product_ids": ["BTC-USD", "ETH-USD"]},
    ],
}


def _raw(message: dict) -> str:
    return json.dumps(message)


def test_parses_match() -> None:
    msg = parse_message(_raw(MATCH))
    assert isinstance(msg, MatchMessage)
    assert msg.trade_id == 10
    assert msg.price == Decimal("400.23")
    assert msg.size == Decimal("5.23512")
    assert msg.time == datetime(2014, 11, 7, 8, 19, 27, 28459, tzinfo=UTC)


def test_parses_last_match_as_match() -> None:
    msg = parse_message(_raw({**MATCH, "type": "last_match"}))
    assert isinstance(msg, MatchMessage)
    assert msg.type == "last_match"


def test_parses_heartbeat() -> None:
    msg = parse_message(_raw(HEARTBEAT))
    assert isinstance(msg, HeartbeatMessage)
    assert msg.last_trade_id == 20


def test_parses_subscriptions() -> None:
    msg = parse_message(_raw(SUBSCRIPTIONS))
    assert isinstance(msg, SubscriptionsMessage)
    assert msg.channels[0].product_ids == ("BTC-USD", "ETH-USD")


def test_parses_error() -> None:
    msg = parse_message(_raw({"type": "error", "message": "Failed to subscribe"}))
    assert isinstance(msg, ErrorMessage)
    assert msg.reason is None


def test_accepts_bytes() -> None:
    assert isinstance(parse_message(_raw(MATCH).encode()), MatchMessage)


def test_ignores_unknown_type() -> None:
    assert parse_message(_raw({"type": "ticker", "price": "1"})) is None


def test_ignores_unknown_fields() -> None:
    msg = parse_message(_raw({**MATCH, "new_field": "x"}))
    assert isinstance(msg, MatchMessage)


def test_business_rules_are_not_enforced_here() -> None:
    # Negative price and a bad side are silver rules (DQ-05, DQ-06), not schema errors.
    msg = parse_message(_raw({**MATCH, "price": "-1", "side": "hold"}))
    assert isinstance(msg, MatchMessage)


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[1, 2]",
        '{"no_type": 1}',
        '{"type": 5}',
        _raw({k: v for k, v in MATCH.items() if k != "trade_id"}),
        _raw({**MATCH, "trade_id": "abc"}),
        _raw({**MATCH, "price": "abc"}),
        _raw({**MATCH, "price": "NaN"}),
        _raw({**MATCH, "time": "2014-11-07T08:19:27"}),
        _raw({**HEARTBEAT, "last_trade_id": None}),
    ],
    ids=[
        "not-json",
        "not-object",
        "no-type",
        "type-not-string",
        "missing-trade-id",
        "trade-id-not-int",
        "price-not-number",
        "price-nan",
        "time-without-timezone",
        "heartbeat-null-trade-id",
    ],
)
def test_rejects_malformed(raw: str) -> None:
    with pytest.raises(InvalidMessageError):
        parse_message(raw)
