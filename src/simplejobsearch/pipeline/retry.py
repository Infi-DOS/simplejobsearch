from __future__ import annotations

import errno
import random
import re
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
RETRYABLE_ERRNOS = frozenset(
    {
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETRESET,
        errno.ENETUNREACH,
        errno.EPIPE,
        errno.ETIMEDOUT,
    }
)
RETRYABLE_CLASS_NAMES = frozenset(
    {
        "APIConnectionError",
        "APITimeoutError",
        "ConnectError",
        "ConnectTimeout",
        "ConnectionResetError",
        "PoolTimeout",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "ServerDisconnectedError",
        "ServiceUnavailable",
        "TimeoutError",
        "WriteError",
        "WriteTimeout",
    }
)
RETRYABLE_MESSAGE_PARTS = (
    "connection reset",
    "connection aborted",
    "server disconnected",
    "temporarily unavailable",
    "temporary failure",
    "timed out",
    "timeout",
    "too many requests",
    "rate limit",
)


def error_status_code(exc: BaseException) -> int | None:
    for value in (
        getattr(exc, "status_code", None),
        getattr(exc, "code", None),
        getattr(getattr(exc, "response", None), "status_code", None),
    ):
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _duration_seconds(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    if not isinstance(value, str):
        return None

    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)s\s*", value)
    if match is None:
        return None
    return float(match.group(1))


def _retry_info_seconds(value: Any) -> list[float]:
    delays: list[float] = []
    if isinstance(value, dict):
        retry_delay = value.get("retryDelay")
        type_name = str(value.get("@type", ""))
        if retry_delay is not None and (
            not type_name or type_name.endswith("google.rpc.RetryInfo")
        ):
            parsed = _duration_seconds(retry_delay)
            if parsed is not None:
                delays.append(parsed)
        for nested in value.values():
            delays.extend(_retry_info_seconds(nested))
    elif isinstance(value, (list, tuple)):
        for nested in value:
            delays.extend(_retry_info_seconds(nested))
    return delays


def _retry_after_header_seconds(exc: BaseException) -> float | None:
    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers is None:
        return None
    try:
        value = headers.get("Retry-After")
    except AttributeError:
        return None
    if value is None:
        return None

    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass

    try:
        retry_at = parsedate_to_datetime(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


def provider_retry_after_seconds(exc: BaseException) -> float | None:
    """Return a provider-requested retry delay from structured error data."""
    delays = _retry_info_seconds(getattr(exc, "details", None))
    header_delay = _retry_after_header_seconds(exc)
    if header_delay is not None:
        delays.append(header_delay)

    # Some providers expose retry guidance only in the message. Keep this as a
    # fallback after inspecting structured RetryInfo and Retry-After values.
    if not delays:
        match = re.search(
            r"(?:please\s+)?retry\s+in\s+(\d+(?:\.\d+)?)\s*s(?:econds?)?\b",
            str(exc),
            flags=re.IGNORECASE,
        )
        if match is not None:
            delays.append(float(match.group(1)))

    return max(delays) if delays else None


def is_transient_ai_error(exc: BaseException) -> bool:
    """Classify transport/provider failures that are safe to retry."""
    status = error_status_code(exc)
    if status is not None:
        return status in RETRYABLE_HTTP_STATUSES

    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in RETRYABLE_ERRNOS:
        return True
    if type(exc).__name__ in RETRYABLE_CLASS_NAMES:
        return True

    message = str(exc).casefold()
    return any(part in message for part in RETRYABLE_MESSAGE_PARTS)


def retry_delay_seconds(
    retry_number: int,
    *,
    base_seconds: float,
    max_seconds: float,
    retry_after: float | None = None,
    random_value: Callable[[], float] = random.random,
) -> float:
    """Return jittered backoff that never undercuts provider retry guidance."""
    if retry_number < 1:
        raise ValueError("retry_number must be at least 1")
    cap = min(max_seconds, base_seconds * (2 ** (retry_number - 1)))
    jitter = min(1.0, max(0.0, random_value()))
    delay = cap * (0.5 + 0.5 * jitter)
    if retry_after is not None:
        # Waiting slightly beyond the provider boundary prevents a synchronized
        # retry wave from landing at the exact instant the quota window resets.
        delay = max(delay, max(0.0, retry_after) + 1.0 + (2.0 * jitter))
    return delay
