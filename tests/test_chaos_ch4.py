import importlib.util
import sys
from pathlib import Path

import pytest

from crypto_recon_fetchers.candles import CandleRow

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ch4", ROOT / "chaos" / "ch4_corrupt_candles.py")
ch4 = importlib.util.module_from_spec(spec)
sys.modules["ch4"] = ch4
spec.loader.exec_module(ch4)


def test_corrupt_swaps_low_and_high_of_first_valid_candle() -> None:
    rows = [
        CandleRow(60, "[60,10,10,10,10,1]"),  # flat candle: high == low, skipped
        CandleRow(120, "[120,9.5,11.25,10,10.5,2.0]"),
        CandleRow(180, "[180,9,12,10,11,3]"),
    ]
    corrupted, original = ch4.corrupt(rows)

    assert original == rows[1]
    assert corrupted[1].raw == "[120,11.25,9.5,10,10.5,2.0]"  # low > high now
    assert corrupted[0] == rows[0] and corrupted[2] == rows[2]


def test_corrupt_needs_a_candle_with_range() -> None:
    with pytest.raises(ValueError):
        ch4.corrupt([CandleRow(60, "[60,10,10,10,10,1]")])
