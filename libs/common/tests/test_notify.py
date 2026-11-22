import json
import urllib.error

from crypto_recon_common.notify import format_message, github_run_url, main, notify

HOOK = "https://hooks.slack.com/services/T000/B000/XXXX"


class FakePost:
    def __init__(self, status: int = 200, error: Exception | None = None) -> None:
        self.status = status
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, url: str, payload: bytes, timeout: float) -> int:
        self.calls.append((url, json.loads(payload)))
        if self.error:
            raise self.error
        return self.status


def test_sends_formatted_message_with_link() -> None:
    post = FakePost()
    assert notify(
        "WAP gate failed", level="error", webhook_url=HOOK, link="https://x/run/1", post=post
    )
    url, payload = post.calls[0]
    assert url == HOOK
    assert payload["text"] == (
        ":rotating_light: *crypto-recon-lakehouse* WAP gate failed\n<https://x/run/1|View run>"
    )


def test_missing_webhook_only_logs(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    post = FakePost()
    assert notify("hello", post=post) is False
    assert post.calls == []


def test_non_slack_url_is_refused() -> None:
    post = FakePost()
    assert notify("hello", webhook_url="https://evil.example/hook", post=post) is False
    assert post.calls == []


def test_network_error_and_rejection_never_raise() -> None:
    assert (
        notify("x", webhook_url=HOOK, post=FakePost(error=urllib.error.URLError("down"))) is False
    )
    assert notify("x", webhook_url=HOOK, post=FakePost(status=500)) is False


def test_github_run_url() -> None:
    env = {
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_REPOSITORY": "odedlaufer/crypto-recon-lakehouse",
        "GITHUB_RUN_ID": "42",
    }
    assert (
        github_run_url(env)
        == "https://github.com/odedlaufer/crypto-recon-lakehouse/actions/runs/42"
    )
    assert github_run_url({}) is None


def test_unknown_level_falls_back_to_info() -> None:
    assert format_message("x", "debug").startswith(":information_source:")


def test_cli_never_fails_without_webhook(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert main(["--level", "warning", "something happened"]) == 0
