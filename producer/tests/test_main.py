import logging
from pathlib import Path

from crypto_recon_producer.__main__ import _configure_logging, _parse_args


def test_log_file_receives_utc_timestamped_logs(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "producer.log"
    root = logging.getLogger()
    try:
        _configure_logging("INFO", log_file)
        logging.getLogger("crypto_recon_producer.test").info("hello file")
        for handler in root.handlers:
            handler.flush()

        text = log_file.read_text(encoding="utf-8")
        assert "INFO crypto_recon_producer.test: hello file" in text
        assert text.split()[0].endswith("Z")
    finally:
        for handler in root.handlers[:]:
            handler.close()
            root.removeHandler(handler)


def test_log_file_is_optional() -> None:
    assert _parse_args([]).log_file is None
    assert _parse_args(["--log-file", "data/x.log"]).log_file == Path("data/x.log")
