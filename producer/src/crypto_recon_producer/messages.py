"""Pydantic models for the Coinbase Exchange WebSocket messages the producer consumes.

The producer checks structure only (DQ-01): the message is JSON, has the fields we need,
and each field has the right type. Business rules such as price > 0, side in (buy, sell)
or the product allow-list are silver checks (DQ-05 to DQ-08), so a structurally valid but
wrong trade still reaches bronze and is quarantined there with a recorded reason.

Message formats: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter, ValidationError


class _Message(BaseModel):
    # Coinbase may add fields at any time; unknown fields are ignored, not rejected.
    model_config = ConfigDict(extra="ignore", frozen=True)


class MatchMessage(_Message):
    """A trade. The first message after subscribing to `matches` is a `last_match`."""

    type: Literal["match", "last_match"]
    trade_id: int
    sequence: int
    maker_order_id: str
    taker_order_id: str
    time: AwareDatetime
    product_id: str
    size: Decimal
    price: Decimal
    side: str  # maker side; validity is a silver rule (DQ-06)


class HeartbeatMessage(_Message):
    """Sent once per second per product; used for gap detection (FR-P5)."""

    type: Literal["heartbeat"]
    sequence: int
    last_trade_id: int
    product_id: str
    time: AwareDatetime


class SubscribedChannel(_Message):
    name: str
    product_ids: tuple[str, ...] = ()


class SubscriptionsMessage(_Message):
    """Server confirmation of the current subscriptions."""

    type: Literal["subscriptions"]
    channels: tuple[SubscribedChannel, ...]


class ErrorMessage(_Message):
    """Server-side error, for example an invalid subscribe request."""

    type: Literal["error"]
    message: str
    reason: str | None = None


FeedMessage = Annotated[
    MatchMessage | HeartbeatMessage | SubscriptionsMessage | ErrorMessage,
    Field(discriminator="type"),
]

KNOWN_TYPES = frozenset({"match", "last_match", "heartbeat", "subscriptions", "error"})

_adapter: TypeAdapter[FeedMessage] = TypeAdapter(FeedMessage)


class InvalidMessageError(ValueError):
    """The raw message is not valid JSON or does not match its schema (DQ-01)."""


def parse_message(raw: str | bytes) -> FeedMessage | None:
    """Parse one raw WebSocket message.

    Returns the typed message, or None for message types we do not consume
    (Coinbase may add new types at any time and clients are expected to ignore them).
    Raises InvalidMessageError if the message is malformed.
    """
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise InvalidMessageError(f"not valid JSON: {exc}") from exc

    if not isinstance(data, dict) or not isinstance(data.get("type"), str):
        raise InvalidMessageError("message is not a JSON object with a string 'type'")
    if data["type"] not in KNOWN_TYPES:
        return None

    try:
        return _adapter.validate_python(data)
    except ValidationError as exc:
        raise InvalidMessageError(str(exc)) from exc
