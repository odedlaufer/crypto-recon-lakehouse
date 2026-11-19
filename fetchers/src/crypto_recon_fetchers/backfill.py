"""Backfill open gaps from REST and record every attempt (FR-B4, FR-J3)."""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from crypto_recon_fetchers.gaps_source import OpenGap
from crypto_recon_fetchers.rest import RestError
from crypto_recon_fetchers.trades import TradeRange, TradesClient, write_trades_files

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GapOutcome:
    gap: OpenGap
    received: int
    missing: int
    files: list[Path] = field(default_factory=list)


@dataclass
class BackfillReport:
    outcomes: list[GapOutcome] = field(default_factory=list)
    skipped: list[OpenGap] = field(default_factory=list)  # over the trade budget
    failed: list[OpenGap] = field(default_factory=list)  # REST errors after retries

    @property
    def files(self) -> list[Path]:
        return [path for outcome in self.outcomes for path in outcome.files]


def run_backfill(
    client: TradesClient,
    gaps: list[OpenGap],
    output_dir: Path,
    *,
    max_trades: int,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BackfillReport:
    """Fetch each gap's trade ids, oldest gap first, within a budget of `max_trades` ids."""
    report = BackfillReport()
    budget = max_trades
    for gap in gaps:
        if gap.missing_count > budget:
            report.skipped.append(gap)
            continue
        trade_range = TradeRange(gap.product_id, gap.from_trade_id, gap.to_trade_id)
        try:
            result = client.fetch_range(trade_range)
        except RestError:
            logger.exception("backfill failed for gap %s %s", gap.gap_id, trade_range)
            report.failed.append(gap)
            continue
        files = write_trades_files(output_dir, result, gap.gap_id, gap.day, now())
        report.outcomes.append(GapOutcome(gap, result.received, result.missing, files))
        budget -= gap.missing_count
        logger.info(
            "gap %s %s %d..%d: received %d of %d (%d not in REST) -> %d files",
            gap.gap_id[:8],
            gap.product_id,
            gap.from_trade_id,
            gap.to_trade_id,
            result.received,
            trade_range.size,
            result.missing,
            len(files),
        )
    return report


def write_attempts_file(
    output_dir: Path, outcomes: list[GapOutcome], fetched_at: datetime
) -> Path | None:
    """One line per backfill attempt; the basis for marking gaps unrepairable (FR-J3).

    Layout: {output_dir}/rest/trades_attempts/dt=YYYY-MM-DD/attempts_{fetched}_{uuid}.jsonl
    """
    if not outcomes:
        return None
    fetched = fetched_at.astimezone(UTC)
    fetched_iso = fetched.isoformat(timespec="seconds").replace("+00:00", "Z")
    directory = output_dir / "rest" / "trades_attempts" / f"dt={fetched:%Y-%m-%d}"
    directory.mkdir(parents=True, exist_ok=True)
    name = f"attempts_{fetched:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex}.jsonl"
    lines = [
        json.dumps(
            {
                "fetched_at": fetched_iso,
                "gap_id": o.gap.gap_id,
                "product_id": o.gap.product_id,
                "from_trade_id": o.gap.from_trade_id,
                "to_trade_id": o.gap.to_trade_id,
                "requested": o.gap.missing_count,
                "received": o.received,
                "missing": o.missing,
            }
        )
        for o in outcomes
    ]
    final_path = directory / name
    tmp_path = directory / f"{name}.tmp"
    with tmp_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_path)
    return final_path
