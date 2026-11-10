"""Q2 probe: how far back can GET /products/{id}/trades reach? (PRD Q2, ADR-003)

Coinbase paginates trades with an `after` cursor that returns older trades. This probe
takes the latest trade_id, jumps back by increasing distances, and records whether trades
are returned, how old they are, and whether each returned page has contiguous trade ids.

Usage: uv run python scripts/probes/q2_rest_trades_depth.py [PRODUCT ...]
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

BASE_URL = "https://api.exchange.coinbase.com"
HEADERS = {"User-Agent": "crypto-recon-lakehouse/0.1 (q2-probe)", "Accept": "application/json"}
DISTANCES = (1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000)


def _get(path: str) -> tuple[list[dict], dict[str, str]]:
    # S310: the URL is always BASE_URL (https) plus a path built in this file.
    request = urllib.request.Request(BASE_URL + path, headers=HEADERS)  # noqa: S310
    with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
        return json.load(response), dict(response.headers)


def _age_hours(iso_time: str) -> float:
    ts = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
    return (datetime.now(UTC) - ts).total_seconds() / 3600


def probe(product: str) -> None:
    latest, headers = _get(f"/products/{product}/trades?limit=1")
    latest_id = latest[0]["trade_id"]
    print(f"{product}: latest trade_id={latest_id} at {latest[0]['time']}")
    print(f"  cursor header CB-AFTER on first page: {headers.get('Cb-After')}")

    for distance in DISTANCES:
        cursor = latest_id - distance
        if cursor <= 0:
            print(f"  back {distance:>11,}: before the first trade, stopping")
            break
        try:
            trades, _ = _get(f"/products/{product}/trades?after={cursor}&limit=100")
        except urllib.error.HTTPError as exc:
            print(f"  back {distance:>11,}: HTTP {exc.code} {exc.read()[:120]!r}")
            continue
        if not trades:
            print(f"  back {distance:>11,}: no trades returned")
            continue
        ids = [t["trade_id"] for t in trades]
        contiguous = ids == list(range(ids[0], ids[0] - len(ids), -1))
        print(
            f"  back {distance:>11,}: ids {ids[-1]}..{ids[0]} "
            f"age {_age_hours(trades[0]['time']):>9,.1f} h  contiguous={contiguous}"
        )
        time.sleep(0.5)  # stay well under the public rate limit


def main() -> None:
    for product in sys.argv[1:] or ["BTC-USD", "ETH-USD", "SOL-USD"]:
        probe(product)


if __name__ == "__main__":
    main()
