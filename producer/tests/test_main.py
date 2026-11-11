from pathlib import Path

from crypto_recon_producer.__main__ import _parse_args


def test_log_file_is_optional() -> None:
    assert _parse_args([]).log_file is None
    assert _parse_args(["--log-file", "data/x.log"]).log_file == Path("data/x.log")
