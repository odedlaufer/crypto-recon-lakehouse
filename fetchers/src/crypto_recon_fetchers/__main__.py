"""Coinbase REST fetchers.

python -m crypto_recon_fetchers candles                      # yesterday (UTC), all products
python -m crypto_recon_fetchers candles --start 2026-11-01 --end 2026-11-07 --product BTC-USD
python -m crypto_recon_fetchers backfill --warehouse-id ID   # open gaps from ops.gaps
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from crypto_recon_common.config import ProductsConfig, load_products_config
from crypto_recon_common.logs import configure_logging
from crypto_recon_common.volume import VolumeUploader, connect_workspace
from crypto_recon_fetchers.backfill import run_backfill, write_attempts_file
from crypto_recon_fetchers.candles import CandlesClient, write_candles_file
from crypto_recon_fetchers.gaps_source import read_open_gaps
from crypto_recon_fetchers.trades import TradesClient

logger = logging.getLogger("crypto_recon_fetchers")

MINUTES_PER_DAY = 1440


def _today_utc() -> date:
    return datetime.now(UTC).date()


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", type=Path, default=Path("data/landing"))
    parser.add_argument("--volume-path", default=None, help="Upload the files written here.")
    parser.add_argument("--databricks-profile", default=None)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", type=Path, default=None)


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
    _add_common(candles)

    backfill = sub.add_parser("backfill", help="Fetch the trades of open gaps (FR-B4).")
    backfill.add_argument("--catalog", default="crypto_dev")
    backfill.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse to read ops.gaps. Default: DATABRICKS_WAREHOUSE_ID.",
    )
    backfill.add_argument("--max-gaps", type=int, default=200)
    backfill.add_argument(
        "--max-trades",
        type=int,
        default=2_000_000,
        help="Budget of trade ids per run; larger gaps wait for a later run.",
    )
    backfill.add_argument("--dry-run", action="store_true", help="List open gaps only.")
    _add_common(backfill)

    args = parser.parse_args(argv)
    if args.command == "candles":
        if args.start > args.end:
            parser.error(f"--start {args.start} is after --end {args.end}")
        if args.end >= today:
            parser.error(f"--end must be before today ({today} UTC): today's candles are not final")
    else:
        if not args.warehouse_id:
            parser.error("--warehouse-id or DATABRICKS_WAREHOUSE_ID is required")
        if args.max_gaps < 1 or args.max_trades < 1:
            parser.error("--max-gaps and --max-trades must be at least 1")
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


def _upload(args: argparse.Namespace, files: list[Path], workspace=None) -> bool:
    if args.volume_path is None or not files:
        return True
    workspace = workspace or connect_workspace(args.databricks_profile)
    uploader = VolumeUploader(workspace.files, args.output_dir, args.volume_path)
    if uploader.upload_files(files).failed:
        logger.error("some uploads failed; files are kept in %s", args.output_dir)
        return False
    return True


def _run_candles(args: argparse.Namespace) -> int:
    if args.product:
        products = ProductsConfig(products=tuple(args.product)).products  # validates ids
    else:
        products = load_products_config(args.config).products

    client = CandlesClient()
    written, empty = fetch_candles(client, products, args.start, args.end, args.output_dir)
    logger.info("fetched %d files with %d requests", len(written), client.requests)
    if not _upload(args, written):
        return 1
    return 1 if empty else 0


def _run_backfill(args: argparse.Namespace) -> int:
    workspace = connect_workspace(args.databricks_profile)
    gaps = read_open_gaps(
        workspace.statement_execution, args.warehouse_id, args.catalog, limit=args.max_gaps
    )
    total = sum(g.missing_count for g in gaps)
    logger.info("%d open gaps, %d missing trade ids", len(gaps), total)
    if args.dry_run:
        for g in gaps:
            logger.info(
                "  %s %s %s %d..%d (%d)",
                g.day,
                g.product_id,
                g.gap_id[:8],
                g.from_trade_id,
                g.to_trade_id,
                g.missing_count,
            )
        return 0

    client = TradesClient()
    report = run_backfill(client, gaps, args.output_dir, max_trades=args.max_trades)
    attempts = write_attempts_file(args.output_dir, report.outcomes, datetime.now(UTC))
    received = sum(o.received for o in report.outcomes)
    not_in_rest = sum(o.missing for o in report.outcomes)
    logger.info(
        "backfilled %d gaps with %d requests: %d trades received, %d not in REST; "
        "%d gaps over budget, %d failed",
        len(report.outcomes),
        client.requests,
        received,
        not_in_rest,
        len(report.skipped),
        len(report.failed),
    )
    files = report.files + ([attempts] if attempts else [])
    if not _upload(args, files, workspace):
        return 1
    return 1 if report.failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv, today=_today_utc())
    configure_logging(args.log_level, args.log_file)
    return _run_candles(args) if args.command == "candles" else _run_backfill(args)


if __name__ == "__main__":
    raise SystemExit(main())
