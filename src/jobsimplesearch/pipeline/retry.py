from __future__ import annotations

import errno
import random
from collections.abc import Callable

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


def _status_code(exc: BaseException) -> int | None:
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


def is_transient_ai_error(exc: BaseException) -> bool:
    """Classify transport/provider failures that are safe to retry immediately."""
    status = _status_code(exc)
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
    random_value: Callable[[], float] = random.random,
) -> float:
    """Return bounded exponential delay with equal jitter for retry 1, 2, ..."""
    if retry_number < 1:
        raise ValueError("retry_number must be at least 1")
    cap = min(max_seconds, base_seconds * (2 ** (retry_number - 1)))
    return cap * (0.5 + 0.5 * random_value())
