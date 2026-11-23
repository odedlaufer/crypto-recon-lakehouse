"""CH-3 (PRD §8.5): inject malformed trade messages and check each is caught by its rule.

The messages go through the producer's own code path (parse_message, Batcher,
landed_record / rejected_record) and are uploaded to the landing Volume like real
captures, so the next pipeline run treats them exactly like live data:

- structurally invalid messages fail the producer's schema check (DQ-01) and land in the
  rejected channel -> quarantine.ws_rejected
- messages that parse but break a trade rule land as matches and the silver expectations
  divert them -> quarantine.trades with the failed rule (DQ-05 to DQ-08)

Every injected trade fails at least one rule, so none reaches silver.trades. Trade ids
are far above Coinbase's and unique per run, so the run can be found in quarantine.

    uv run --package crypto-recon-producer python chaos/ch3_malformed_messages.py \
        --volume-path /Volumes/crypto_dev/raw/landing --databricks-profile personal
"""

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_recon_common.volume import VolumeUploader, connect_workspace
from crypto_recon_producer.batching import (
    REJECTED_CHANNEL,
    UNKNOWN_PRODUCT,
    Batcher,
    channel_for,
    landed_record,
    rejected_record,
)
from crypto_recon_producer.messages import InvalidMessageError, parse_message

TRADE_ID_BASE = 9_000_000_000_000


@dataclass(frozen=True)
class Case:
    name: str
    raw: str
    expected_table: str
    expected_rule: str


def build_cases(run_id: int, now: datetime) -> list[Case]:
    """One case per rule. trade ids: TRADE_ID_BASE + run_id * 100 + n."""
    marker = f"chaos-ch3-{run_id}"

    def match(n: int, **overrides: object) -> str:
        message = {
            "type": "match",
            "trade_id": TRADE_ID_BASE + run_id * 100 + n,
            "sequence": 1,
            "maker_order_id": marker,
            "taker_order_id": marker,
            "time": now.isoformat().replace("+00:00", "Z"),
            "product_id": "BTC-USD",
            "size": "0.01",
            "price": "50000.00",
            "side": "buy",
        }
        message.update(overrides)
        return json.dumps(message)

    future = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    missing_trade_id = json.loads(match(0))
    del missing_trade_id["trade_id"]
    return [
        Case(
            "negative price",
            match(1, price="-1.00"),
            "quarantine.trades",
            "dq05_price_size_positive",
        ),
        Case("zero size", match(2, size="0"), "quarantine.trades", "dq05_price_size_positive"),
        Case("unknown side", match(3, side="hold"), "quarantine.trades", "dq06_side_valid"),
        Case("future timestamp", match(4, time=future), "quarantine.trades", "dq07_not_in_future"),
        Case(
            "product not allowed",
            match(5, product_id="DOGE-USD"),
            "quarantine.trades",
            "dq08_product_allowed",
        ),
        Case("not JSON", f"{{{marker} this is not json", "quarantine.ws_rejected", "dq01_schema"),
        Case(
            "missing trade_id",
            json.dumps(missing_trade_id),
            "quarantine.ws_rejected",
            "dq01_schema",
        ),
    ]


def write_files(cases: list[Case], output_dir: Path, now: datetime) -> list[Path]:
    """Route each case exactly like the producer does and write the landing files."""
    batcher = Batcher(output_dir, max_messages=10_000, clock=lambda: now)
    for case in cases:
        try:
            message = parse_message(case.raw)
        except InvalidMessageError as exc:
            batcher.add(REJECTED_CHANNEL, UNKNOWN_PRODUCT, rejected_record(case.raw, now, str(exc)))
            continue
        if message is None:
            raise ValueError(f"case {case.name!r} parsed as an ignored message type")
        batcher.add(channel_for(message), message.product_id, landed_record(case.raw, now))
    return [flushed.path for flushed in batcher.flush_all()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--volume-path", required=True)
    parser.add_argument("--databricks-profile", default=None)
    args = parser.parse_args()

    now = datetime.now(UTC)
    run_id = int(now.timestamp())
    cases = build_cases(run_id, now)
    with tempfile.TemporaryDirectory() as tmp:
        output_dir = Path(tmp)
        files = write_files(cases, output_dir, now)
        uploader = VolumeUploader(
            connect_workspace(args.databricks_profile).files, output_dir, args.volume_path
        )
        result = uploader.upload_files(files)
    if result.failed:
        print(f"upload failed for {len(result.failed)} file(s)")
        return 1

    low, high = TRADE_ID_BASE + run_id * 100, TRADE_ID_BASE + run_id * 100 + 99
    print(f"CH-3 run {run_id}: injected {len(cases)} messages in {len(files)} files")
    for case in cases:
        print(f"  {case.name:<20} -> {case.expected_table} ({case.expected_rule})")
    print("\nAfter the next stream pipeline run, check:")
    print(
        f"  SELECT trade_id, rule_failed FROM crypto_dev.quarantine.trades\n"  # noqa: S608 (printed only)
        f"  WHERE trade_id BETWEEN {low} AND {high} ORDER BY trade_id;\n"
        f"  SELECT rule_failed FROM crypto_dev.quarantine.ws_rejected\n"
        f"  WHERE payload LIKE '%chaos-ch3-{run_id}%';\n"
        f"  SELECT count(*) FROM crypto_dev.silver.trades WHERE trade_id BETWEEN {low} AND {high};"
        f"  -- must be 0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
