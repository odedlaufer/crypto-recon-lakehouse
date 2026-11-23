import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from crypto_recon_producer.batching import MATCHES_CHANNEL, REJECTED_CHANNEL

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ch3", ROOT / "chaos" / "ch3_malformed_messages.py")
ch3 = importlib.util.module_from_spec(spec)
sys.modules["ch3"] = ch3  # dataclasses need the module registered
spec.loader.exec_module(ch3)

NOW = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def test_every_trade_rule_and_dq01_has_a_case() -> None:
    rules = {case.expected_rule for case in ch3.build_cases(1, NOW)}
    assert rules == {
        "dq01_schema",
        "dq05_price_size_positive",
        "dq06_side_valid",
        "dq07_not_in_future",
        "dq08_product_allowed",
    }


def test_cases_are_routed_like_the_producer(tmp_path: Path) -> None:
    cases = ch3.build_cases(1, NOW)
    files = ch3.write_files(cases, tmp_path, NOW)

    by_channel: dict[str, int] = {}
    for path in files:
        channel = path.relative_to(tmp_path).parts[1]
        by_channel[channel] = by_channel.get(channel, 0) + len(path.read_text().splitlines())

    expected_rejected = sum(c.expected_table == "quarantine.ws_rejected" for c in cases)
    assert by_channel[REJECTED_CHANNEL] == expected_rejected
    assert by_channel[MATCHES_CHANNEL] == len(cases) - expected_rejected


def test_trade_ids_are_unique_and_far_above_coinbase() -> None:
    ids = []
    for case in ch3.build_cases(7, NOW):
        try:
            ids.append(json.loads(case.raw)["trade_id"])
        except (ValueError, KeyError):
            continue
    assert len(ids) == len(set(ids)) == 5
    assert min(ids) > 10**12
