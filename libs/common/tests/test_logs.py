import logging
from pathlib import Path

from crypto_recon_common.logs import configure_logging


def test_log_file_receives_utc_timestamped_logs(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "app.log"
    root = logging.getLogger()
    try:
        configure_logging("INFO", log_file)
        logging.getLogger("crypto_recon.test").info("hello file")
        for handler in root.handlers:
            handler.flush()

        text = log_file.read_text(encoding="utf-8")
        assert "INFO crypto_recon.test: hello file" in text
        assert text.split()[0].endswith("Z")
    finally:
        for handler in root.handlers[:]:
            handler.close()
            root.removeHandler(handler)
