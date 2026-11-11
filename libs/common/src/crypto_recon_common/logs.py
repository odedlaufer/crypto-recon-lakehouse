"""Logging setup shared by the producer and fetchers: UTC timestamps, optional log file."""

from __future__ import annotations

import logging
import time
from pathlib import Path


def configure_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    """Log to stderr and, if given, append to `log_file` (survives Ctrl+C, unlike `| tee`)."""
    logging.Formatter.converter = time.gmtime
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=handlers,
        force=True,
    )
