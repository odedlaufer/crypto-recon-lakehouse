"""Build the integration-test fixture from one recorded hour of real capture (PRD §8.4).

Reads a local copy of the dev landing Volume (copied first with the Databricks CLI, see
below): every trade and heartbeat file of one UTC hour and every official-candle fetch of
that day. Removes a few known trade-id ranges (one per product) to create gaps, and writes
the files gzipped under tests/integration/fixture/ with the same layout as the Volume.
expected.json records what the test must find.

    SRC=data/it-source; DAY=2026-11-21; HR=10; V=dbfs:/Volumes/crypto_dev/raw/landing
    databricks fs cp -r $V/ws/matches/dt=$DAY/hr=$HR $SRC/ws/matches/dt=$DAY/hr=$HR
    databricks fs cp -r $V/ws/heartbeat/dt=$DAY/hr=$HR $SRC/ws/heartbeat/dt=$DAY/hr=$HR
    databricks fs cp -r $V/rest/candles/dt=$DAY $SRC/rest/candles/dt=$DAY
    uv run python tests/integration/build_fixture.py --source-dir $SRC --day $DAY --hour $HR
"""

from __future__ import annotations

import argparse
import gzip
import itertools
import json
import shutil
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURE_DIR = HERE / "fixture"
EXPECTED = HERE / "expected.json"

# Trade-id ranges to remove: (product, minutes into the hour, number of consecutive ids).
REMOVALS = [("BTC-USD", 20, 5), ("ETH-USD", 35, 3), ("SOL-USD", 50, 2)]


def _read_dir(directory: Path) -> dict[Path, list[str]]:
    files = sorted(directory.glob("*.jsonl"))
    return {f: f.read_text(encoding="utf-8").splitlines() for f in files}


def _trade(line: str) -> tuple[str, int, str]:
    raw = json.loads(json.loads(line)["raw"])
    return raw["product_id"], raw["trade_id"], raw["time"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--day", required=True, help="YYYY-MM-DD")
    parser.add_argument("--hour", type=int, required=True)
    args = parser.parse_args()
    src = args.source_dir
    hour_dir = f"dt={args.day}/hr={args.hour:02d}"

    matches = _read_dir(src / "ws" / "matches" / hour_dir)
    heartbeats = _read_dir(src / "ws" / "heartbeat" / hour_dir)
    candles = _read_dir(src / "rest" / "candles" / f"dt={args.day}")
    if not matches or not candles:
        raise SystemExit(f"no trade or candle files under {src} for that hour/day")
    print(f"read {len(matches)} trade, {len(heartbeats)} heartbeat, {len(candles)} candle files")

    # Pick the trade ids to remove: the first ids at or after the chosen minute.
    trades = defaultdict(dict)  # product -> trade_id -> time
    for lines in matches.values():
        for line in lines:
            product, trade_id, time = _trade(line)
            trades[product][trade_id] = time
    removed: dict[str, set[int]] = {}
    for product, minute, count in REMOVALS:
        start_time = f"{args.day}T{args.hour:02d}:{minute:02d}:00"
        first = min(t for t, time in trades[product].items() if time >= start_time)
        ids = set(range(first, first + count))
        assert ids <= trades[product].keys(), f"{product}: ids {ids} not all captured"
        removed[product] = ids

    if FIXTURE_DIR.exists():
        shutil.rmtree(FIXTURE_DIR)
    kept = defaultdict(set)
    for source, lines in {**matches, **heartbeats, **candles}.items():
        if source in matches:
            lines = [ln for ln in lines if _trade(ln)[1] not in removed[_trade(ln)[0]]]
            for ln in lines:
                product, trade_id, _ = _trade(ln)
                kept[product].add(trade_id)
        target = FIXTURE_DIR / (str(source.relative_to(src)) + ".gz")
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    # Every gap between the hour's first and last kept trade, ours and any real ones.
    gaps = []
    for product, ids in sorted(kept.items()):
        ordered = sorted(ids)
        for prev, nxt in itertools.pairwise(ordered):
            if nxt - prev > 1:
                gaps.append(
                    {
                        "product_id": product,
                        "from_trade_id": prev + 1,
                        "to_trade_id": nxt - 1,
                        "missing_count": nxt - prev - 1,
                    }
                )
    expected = {
        "day": args.day,
        "hour": args.hour,
        "removed": {p: sorted(ids) for p, ids in sorted(removed.items())},
        "trades": {p: len(ids) for p, ids in sorted(kept.items())},
        "gaps": gaps,
    }
    EXPECTED.write_text(json.dumps(expected, indent=2) + "\n")

    size = sum(f.stat().st_size for f in FIXTURE_DIR.rglob("*.gz"))
    print(f"fixture: {len(list(FIXTURE_DIR.rglob('*.gz')))} files, {size / 1e6:.1f} MB gzipped")
    print(f"trades kept: {expected['trades']}")
    print(f"gaps: {len(gaps)} ({sum(g['missing_count'] for g in gaps)} ids)")
    for gap in gaps:
        print(f"  {gap}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
