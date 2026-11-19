from pathlib import Path

import pytest

from crypto_recon_producer.__main__ import _parse_args


def test_log_file_is_optional() -> None:
    assert _parse_args([]).log_file is None
    assert _parse_args(["--log-file", "data/x.log"]).log_file == Path("data/x.log")


def test_batching_defaults_follow_adr_011() -> None:
    args = _parse_args([])
    assert (args.flush_seconds, args.max_messages) == (600.0, 100_000)


def test_batching_can_be_overridden() -> None:
    args = _parse_args(["--flush-seconds", "30", "--max-messages", "1000"])
    assert (args.flush_seconds, args.max_messages) == (30.0, 1000)


@pytest.mark.parametrize(
    "argv", [["--flush-seconds", "0"], ["--max-messages", "0"]], ids=["flush", "max"]
)
def test_batching_rejects_invalid_values(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(argv)
