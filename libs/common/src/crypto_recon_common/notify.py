"""Send alerts to Slack (PRD §7), never failing the caller.

The webhook comes from SLACK_WEBHOOK_URL. When it is not set (a fresh clone, a local run,
a fork), the alert is only logged, so code that alerts never depends on Slack.

CLI for workflows:
    python -m crypto_recon_common.notify --level error "daily-batch failed"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

SLACK_PREFIX = "https://hooks.slack.com/"
LEVEL_ICONS = {"info": ":information_source:", "warning": ":warning:", "error": ":rotating_light:"}


def github_run_url(env: dict[str, str] | None = None) -> str | None:
    """Link to the current GitHub Actions run, when running in one."""
    env = os.environ if env is None else env
    server, repo, run_id = (
        env.get("GITHUB_SERVER_URL"),
        env.get("GITHUB_REPOSITORY"),
        env.get("GITHUB_RUN_ID"),
    )
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return None


def format_message(text: str, level: str, link: str | None = None) -> str:
    icon = LEVEL_ICONS.get(level, LEVEL_ICONS["info"])
    message = f"{icon} *crypto-recon-lakehouse* {text}"
    return f"{message}\n<{link}|View run>" if link else message


def _post(url: str, payload: bytes, timeout: float) -> int:
    request = urllib.request.Request(  # noqa: S310 (URL checked to be https://hooks.slack.com/)
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return response.status


def notify(
    text: str,
    *,
    level: str = "info",
    webhook_url: str | None = None,
    link: str | None = None,
    post: Callable[[str, bytes, float], int] = _post,
    timeout: float = 10.0,
) -> bool:
    """Post `text` to Slack. Returns True if Slack accepted it; never raises."""
    url = webhook_url if webhook_url is not None else os.environ.get("SLACK_WEBHOOK_URL", "")
    link = link if link is not None else github_run_url()
    message = format_message(text, level, link)
    if not url:
        logger.warning("SLACK_WEBHOOK_URL not set; alert not sent: %s", message)
        return False
    if not url.startswith(SLACK_PREFIX):
        logger.error("SLACK_WEBHOOK_URL is not a Slack webhook; alert not sent: %s", message)
        return False
    try:
        status = post(url, json.dumps({"text": message}).encode("utf-8"), timeout)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.error("Slack alert failed (%s): %s", exc, message)
        return False
    if status != 200:
        logger.error("Slack alert rejected with HTTP %s: %s", status, message)
        return False
    logger.info("Slack alert sent: %s", message)
    return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="crypto_recon_common.notify", description=__doc__)
    parser.add_argument("text")
    parser.add_argument("--level", choices=sorted(LEVEL_ICONS), default="info")
    args = parser.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(levelname)s %(name)s: %(message)s")
    notify(args.text, level=args.level)
    return 0  # an alert must never fail the step that sends it


if __name__ == "__main__":
    raise SystemExit(main())
