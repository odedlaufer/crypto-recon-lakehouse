"""Run the producer: python -m crypto_recon_producer [--duration-seconds N]."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import threading
from datetime import timedelta
from pathlib import Path

from crypto_recon_common.config import load_products_config
from crypto_recon_common.logs import configure_logging
from crypto_recon_common.volume import VolumeUploader, connect_files_client, upload_continuously
from crypto_recon_producer.batching import Batcher
from crypto_recon_producer.client import COINBASE_WS_URL, Producer

logger = logging.getLogger("crypto_recon_producer")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="crypto_recon_producer", description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/products.yml"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/landing"))
    parser.add_argument("--url", default=COINBASE_WS_URL)
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Stop after this many seconds (a capture window). Default: run until stopped.",
    )
    parser.add_argument(
        "--volume-path",
        default=None,
        help="Upload flushed files to this Volume path. Default: keep files local.",
    )
    parser.add_argument(
        "--databricks-profile",
        default=None,
        help="Databricks CLI profile. Default: DATABRICKS_HOST/DATABRICKS_TOKEN or DEFAULT.",
    )
    parser.add_argument("--upload-interval-seconds", type=float, default=5.0)
    parser.add_argument(
        "--flush-seconds",
        type=float,
        default=600.0,
        help="Flush each batch after this many seconds (ADR-011: few, larger files).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=100_000,
        help="Flush a batch early once it holds this many messages.",
    )
    parser.add_argument(
        "--chaos-drop-pct",
        type=float,
        default=0.0,
        help="CH-1 fault injection: drop this percentage of trades (0 to 100). Default 0.",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Also append logs to this file (survives Ctrl+C, unlike piping to tee).",
    )
    args = parser.parse_args(argv)
    if args.flush_seconds <= 0:
        parser.error("--flush-seconds must be positive")
    if args.max_messages < 1:
        parser.error("--max-messages must be at least 1")
    return args


async def _run(args: argparse.Namespace) -> None:
    products = load_products_config(args.config).products
    batcher = Batcher(
        args.output_dir,
        max_messages=args.max_messages,
        max_age=timedelta(seconds=args.flush_seconds),
    )
    if args.chaos_drop_pct:
        logger.warning("CH-1: dropping %.1f%% of trades on purpose", args.chaos_drop_pct)
    producer = Producer(products, batcher, url=args.url, chaos_drop_pct=args.chaos_drop_pct)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)  # graceful shutdown (FR-P8)
    if args.duration_seconds is not None:
        loop.call_later(args.duration_seconds, stop.set)

    upload_stop = threading.Event()
    uploader_thread: threading.Thread | None = None
    if args.volume_path is not None:
        uploader = VolumeUploader(
            connect_files_client(args.databricks_profile), args.output_dir, args.volume_path
        )
        uploader_thread = threading.Thread(
            target=upload_continuously,
            args=(uploader, upload_stop, args.upload_interval_seconds),
            name="volume-uploader",
            daemon=True,
        )
        uploader_thread.start()

    try:
        await producer.run(stop)
    finally:
        if uploader_thread is not None:
            upload_stop.set()  # the uploader does one final sweep after the last flush
            await asyncio.to_thread(uploader_thread.join, 120)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(args.log_level, args.log_file)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
