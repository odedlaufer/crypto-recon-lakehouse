"""Shared Coinbase Exchange REST client: throttling and retries (FR-B2)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.exchange.coinbase.com"
USER_AGENT = "crypto-recon-lakehouse/0.1 (+https://github.com/odedlaufer/crypto-recon-lakehouse)"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class RestError(RuntimeError):
    """The endpoint returned an error or a malformed response."""


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


class RestClient:
    """GET with a minimum interval between requests and retries on 429, 5xx and network errors."""

    def __init__(
        self,
        client: httpx.Client | None = None,
        *,
        min_interval_seconds: float = 0.35,
        max_attempts: int = 5,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client or httpx.Client(
            base_url=BASE_URL,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=15.0,
        )
        self._min_interval = min_interval_seconds
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request_at: float | None = None
        self.requests = 0

    def _get(self, path: str, params: dict[str, str]) -> str:
        for attempt in range(1, self._max_attempts + 1):
            self._throttle()
            retry_after: float | None = None
            try:
                response = self._client.get(path, params=params)
            except httpx.TransportError as exc:
                error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 200:
                    return response.text
                if response.status_code not in RETRY_STATUSES:
                    raise RestError(f"HTTP {response.status_code}: {response.text[:200]}")
                error = f"HTTP {response.status_code}"
                retry_after = _retry_after_seconds(response)

            if attempt == self._max_attempts:
                raise RestError(f"giving up after {attempt} attempts: {error}")
            delay = retry_after if retry_after is not None else min(2.0 ** (attempt - 1), 30.0)
            logger.warning(
                "%s %s failed (%s); retry %d in %.1fs", path, params, error, attempt, delay
            )
            self._sleep(delay)
        raise AssertionError("unreachable")

    def _throttle(self) -> None:
        now = self._monotonic()
        if self._last_request_at is not None:
            wait = self._min_interval - (now - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
                now = self._monotonic()
        self._last_request_at = now
        self.requests += 1
