"""Micro-batch landed messages into JSONL files (FR-P3).

Each line keeps the raw WebSocket message exactly as received, plus the local receive
time. Bronze stores the raw text as its payload, so nothing is lost or re-serialized here.

Batches are kept per (channel, product) and flushed when they reach max_messages or
max_age, whichever comes first. Files are written to a temporary name and renamed, so a
reader (the uploader) never sees a partial file.

Layout: {output_dir}/ws/{channel}/dt=YYYY-MM-DD/hr=HH/{channel}_{product}_{start}_{uuid}.jsonl
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_recon_producer.messages import HeartbeatMessage, MatchMessage

MATCHES_CHANNEL = "matches"
HEARTBEAT_CHANNEL = "heartbeat"
REJECTED_CHANNEL = "_rejected"
UNKNOWN_PRODUCT = "unknown"

_UNSAFE_PATH_CHARS = re.compile(r"[^A-Za-z0-9-]")


def utc_now() -> datetime:
    return datetime.now(UTC)


def channel_for(message: MatchMessage | HeartbeatMessage) -> str:
    """The landing channel a consumed message belongs to."""
    return MATCHES_CHANNEL if isinstance(message, MatchMessage) else HEARTBEAT_CHANNEL


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _as_text(raw: str | bytes) -> str:
    return raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw


def landed_record(raw: str | bytes, received_at: datetime) -> str:
    """One JSONL line for a valid message: the raw text, unchanged, plus receive time."""
    return json.dumps({"received_at": _iso(received_at), "raw": _as_text(raw)})


def rejected_record(raw: str | bytes, received_at: datetime, error: str) -> str:
    """One JSONL line for a message that failed schema validation (DQ-01)."""
    return json.dumps({"received_at": _iso(received_at), "raw": _as_text(raw), "error": error})


@dataclass(frozen=True)
class FlushedFile:
    path: Path
    channel: str
    product_id: str
    message_count: int
    started_at: datetime


@dataclass
class _Batch:
    started_at: datetime
    lines: list[str] = field(default_factory=list)


class Batcher:
    """Collects JSONL lines per (channel, product) and flushes them to files."""

    def __init__(
        self,
        output_dir: Path,
        *,
        max_messages: int = 1000,
        max_age: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if max_messages < 1:
            raise ValueError("max_messages must be at least 1")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        self._output_dir = output_dir
        self._max_messages = max_messages
        self._max_age = max_age
        self._clock = clock
        self._batches: dict[tuple[str, str], _Batch] = {}

    @property
    def pending_messages(self) -> int:
        return sum(len(batch.lines) for batch in self._batches.values())

    def add(self, channel: str, product_id: str, line: str) -> list[FlushedFile]:
        """Add one line. Returns any files flushed as a result (by size or age)."""
        flushed = self.flush_due()
        key = (channel, product_id)
        batch = self._batches.get(key)
        if batch is None:
            batch = self._batches[key] = _Batch(started_at=self._clock())
        batch.lines.append(line)
        if len(batch.lines) >= self._max_messages:
            flushed.append(self._flush(key))
        return flushed

    def flush_due(self) -> list[FlushedFile]:
        """Flush every batch that has reached max_age. Call this on a timer."""
        now = self._clock()
        due = [
            key for key, batch in self._batches.items() if now - batch.started_at >= self._max_age
        ]
        return [self._flush(key) for key in due]

    def flush_all(self) -> list[FlushedFile]:
        """Flush everything, regardless of size or age (graceful shutdown, FR-P8)."""
        return [self._flush(key) for key in list(self._batches)]

    def _flush(self, key: tuple[str, str]) -> FlushedFile:
        batch = self._batches.pop(key)
        channel, product_id = key
        started = batch.started_at.astimezone(UTC)
        safe_product = _UNSAFE_PATH_CHARS.sub("_", product_id) or UNKNOWN_PRODUCT

        directory = (
            self._output_dir / "ws" / channel / f"dt={started:%Y-%m-%d}" / f"hr={started:%H}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        name = f"{channel}_{safe_product}_{started:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex}.jsonl"
        final_path = directory / name
        tmp_path = directory / f"{name}.tmp"

        with tmp_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(batch.lines) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, final_path)

        return FlushedFile(
            path=final_path,
            channel=channel,
            product_id=product_id,
            message_count=len(batch.lines),
            started_at=started,
        )
