"""Fetch official candles (FR-B1, FR-B3).

python -m crypto_recon_fetchers candles                      # yesterday (UTC), all products
python -m crypto_recon_fetchers candles --start 2026-11-01 --end 2026-11-07 --product BTC-USD
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from crypto_recon_common.config import ProductsConfig, load_products_config
from crypto_recon_common.logs import configure_logging
from crypto_recon_common.volume import VolumeUploader, connect_files_client
from crypto_recon_fetchers.candles import CandlesClient, write_candles_file

logger = logging.getLogger("crypto_recon_fetchers")

MINUTES_PER_DAY = 1440


def _today_utc() -> date:
    return datetime.now(UTC).date()


def _parse_args(argv: Sequence[str] | None, today: date) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="crypto_recon_fetchers", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    candles = sub.add_parser("candles", help="Fetch official 1-minute candles per UTC day.")
    yesterday = today - timedelta(days=1)
    candles.add_argument("--start", type=date.fromisoformat, default=yesterday)
    candles.add_argument("--end", type=date.fromisoformat, default=yesterday)
    candles.add_argument(
        "--product",
        action="append",
        default=None,
        help="Product id; repeat for several. Default: all products in --config.",
    )
    candles.add_argument("--config", type=Path, default=Path("config/products.yml"))
    candles.add_argument("--output-dir", type=Path, default=Path("data/landing"))
    candles.add_argument("--volume-path", default=None, help="Upload the files written here.")
    candles.add_argument("--databricks-profile", default=None)
    candles.add_argument("--log-level", default="INFO")
    candles.add_argument("--log-file", type=Path, default=None)

    args = parser.parse_args(argv)
    if args.start > args.end:
        parser.error(f"--start {args.start} is after --end {args.end}")
    if args.end >= today:
        parser.error(f"--end must be before today ({today} UTC): today's candles are not final")
    return args


def fetch_candles(
    client: CandlesClient,
    products: Sequence[str],
    start: date,
    end: date,
    output_dir: Path,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[list[Path], list[tuple[str, date]]]:
    """Fetch and land each product-day. Returns (files written, product-days with no candles)."""
    written: list[Path] = []
    empty: list[tuple[str, date]] = []
    day = start
    while day <= end:
        for product in products:
            rows = client.fetch_day(product, day)
            if not rows:
                logger.error("no candles for %s on %s", product, day)
                empty.append((product, day))
                continue
            path = write_candles_file(output_dir, product, day, rows, now())
            missing = MINUTES_PER_DAY - len(rows)
            logger.info(
                "%s %s: %d candles (%d minutes without trades) -> %s",
                product,
                day,
                len(rows),
                missing,
                path,
            )
            written.append(path)
        day += timedelta(days=1)
    return written, empty


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv, today=_today_utc())
    configure_logging(args.log_level, args.log_file)

    if args.product:
        products = ProductsConfig(products=tuple(args.product)).products  # validates ids
    else:
        products = load_products_config(args.config).products

    client = CandlesClient()
    written, empty = fetch_candles(client, products, args.start, args.end, args.output_dir)
    logger.info("fetched %d files with %d requests", len(written), client.requests)

    if args.volume_path is not None and written:
        uploader = VolumeUploader(
            connect_files_client(args.databricks_profile), args.output_dir, args.volume_path
        )
        if uploader.upload_files(written).failed:
            logger.error("some uploads failed; files are kept in %s", args.output_dir)
            return 1

    return 1 if empty else 0


if __name__ == "__main__":
    raise SystemExit(main())
