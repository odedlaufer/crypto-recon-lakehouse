"""CH-4 (PRD §8.5): a corrupted official-candle file must be blocked by the WAP gate.

`inject` fetches one real product-day of official candles, swaps low and high in one
candle (so high < low, breaking DQ-11) and lands it as a new fetch. Staging takes the
latest fetch per minute, so the next `candles_official` run for that day stages the
corrupted candle, the Soda audit fails DQ-11 and `publish` never runs.

`restore` lands a fresh, correct fetch of the same day; it is newer, so the next run
stages correct candles again and passes.

    uv run --package crypto-recon-fetchers python chaos/ch4_corrupt_candles.py inject \
        --volume-path /Volumes/crypto_dev/raw/landing --databricks-profile personal
"""

from __future__ import annotations

import argparse
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from crypto_recon_common.volume import VolumeUploader, connect_workspace
from crypto_recon_fetchers.candles import CandleRow, CandlesClient, write_candles_file

DEFAULT_DAY = date(2026, 11, 15)  # an old day: daily-batch only re-runs the last 2 days


def corrupt(rows: list[CandleRow]) -> tuple[list[CandleRow], CandleRow]:
    """Swap low and high in the first candle where high > low. Returns rows and the original."""
    for i, row in enumerate(rows):
        t, low, high, open_, close, volume = row.raw.strip("[]").split(",")
        if float(high) > float(low):
            bad = CandleRow(row.time, f"[{t},{high},{low},{open_},{close},{volume}]")
            return [*rows[:i], bad, *rows[i + 1 :]], row
    raise ValueError("no candle with high > low to corrupt")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["inject", "restore"])
    parser.add_argument("--day", type=date.fromisoformat, default=DEFAULT_DAY)
    parser.add_argument("--product", default="BTC-USD")
    parser.add_argument("--volume-path", required=True)
    parser.add_argument("--databricks-profile", default=None)
    args = parser.parse_args()

    rows = CandlesClient().fetch_day(args.product, args.day)
    if args.action == "inject":
        rows, original = corrupt(rows)
        minute = datetime.fromtimestamp(original.time, UTC)
        print(f"corrupted {args.product} {minute:%Y-%m-%d %H:%M} UTC: {original.raw}")
        print("  low and high swapped, so high < low (breaks DQ-11)")

    with tempfile.TemporaryDirectory() as tmp:
        output_dir = Path(tmp)
        path = write_candles_file(output_dir, args.product, args.day, rows, datetime.now(UTC))
        workspace = connect_workspace(args.databricks_profile)
        result = VolumeUploader(workspace.files, output_dir, args.volume_path).upload_files([path])
    if result.failed:
        print("upload failed")
        return 1
    print(f"{args.action}: landed {len(rows)} candles for {args.product} {args.day}")
    print(
        "next: databricks bundle run candles_official -t dev "
        f"--params start_date={args.day},end_date={args.day}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
