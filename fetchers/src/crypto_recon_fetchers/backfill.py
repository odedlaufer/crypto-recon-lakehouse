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
from crypto_recon_fetchers.trades import (
    PAGE_SIZE,
    RangeResult,
    TradeRange,
    TradesClient,
    write_batch_files,
)

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
        """Each file once (gaps fetched together share files)."""
        return list(dict.fromkeys(path for outcome in self.outcomes for path in outcome.files))


def group_nearby(gaps: list[OpenGap], max_span: int = PAGE_SIZE) -> list[list[OpenGap]]:
    """Group gaps of the same product whose combined id span fits in `max_span` ids.

    Each group is fetched as one id range, so gaps a few trades apart cost one REST request
    instead of one each. A gap larger than `max_span` forms its own group.
    """
    groups: list[list[OpenGap]] = []
    for gap in sorted(gaps, key=lambda g: (g.product_id, g.from_trade_id)):
        current = groups[-1] if groups else None
        if (
            current
            and current[0].product_id == gap.product_id
            and gap.to_trade_id - current[0].from_trade_id + 1 <= max_span
        ):
            current.append(gap)
        else:
            groups.append([gap])
    return groups


def run_backfill(
    client: TradesClient,
    gaps: list[OpenGap],
    output_dir: Path,
    *,
    max_trades: int,
    max_span: int = PAGE_SIZE,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BackfillReport:
    """Fetch the trade ids of open gaps within a budget of `max_trades` ids.

    The budget is applied oldest gap first; the gaps that fit are then fetched in groups of
    nearby gaps (group_nearby), one id range per group.
    """
    report = BackfillReport()
    budget = max_trades
    selected: list[OpenGap] = []
    for gap in gaps:
        if gap.missing_count > budget:
            report.skipped.append(gap)
            continue
        selected.append(gap)
        budget -= gap.missing_count

    for group in group_nearby(selected, max_span):
        first, last = group[0], group[-1]
        span = TradeRange(first.product_id, first.from_trade_id, last.to_trade_id)
        try:
            fetched = client.fetch_range(span)
        except RestError:
            logger.exception("backfill failed for %d gap(s) in %s", len(group), span)
            report.failed.extend(group)
            continue
        per_gap = [
            (
                gap,
                RangeResult(
                    TradeRange(gap.product_id, gap.from_trade_id, gap.to_trade_id),
                    [
                        t
                        for t in fetched.trades
                        if gap.from_trade_id <= t["trade_id"] <= gap.to_trade_id
                    ],
                ),
            )
            for gap in group
        ]
        files = write_batch_files(
            output_dir, [(gap.gap_id, result) for gap, result in per_gap], first.day, now()
        )
        for gap, result in per_gap:
            report.outcomes.append(GapOutcome(gap, result.received, result.missing, files))
        logger.info(
            "%s %d..%d: %d gap(s), received %d of %d missing ids -> %d files",
            span.product_id,
            span.from_trade_id,
            span.to_trade_id,
            len(group),
            sum(r.received for _, r in per_gap),
            sum(g.missing_count for g in group),
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
