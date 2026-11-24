"""Retry helper for transient S3 / Odin failures (network drops, timeouts, throttling, HTTP 5xx).

Permanent errors (missing key, access denied, bad credentials, HTTP 4xx) are raised at once:
retrying them only delays the failure.
"""
from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")

MAX_BACKOFF_SECONDS = 120.0

# HTTP statuses worth another try: request timeout, rate limit and server-side errors.
_RETRYABLE_STATUSES = {408, 429}


def _status_is_transient(status) -> bool:
    try:
        status = int(status)
    except (TypeError, ValueError):
        return True  # unknown status: give it another go
    return status in _RETRYABLE_STATUSES or status >= 500


def is_transient_http(exc: BaseException) -> bool:
    """requests / Odin errors that may succeed on a new attempt."""
    import requests

    from .ai_client import AIError

    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return resp is None or _status_is_transient(resp.status_code)
    if isinstance(exc, (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError)):
        return True
    return isinstance(exc, AIError)  # e.g. chat created without an id: usually a hiccup on Odin's side


def is_transient_s3(exc: BaseException) -> bool:
    """boto3 / s3transfer errors that may succeed on a new attempt."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict) and "Error" in response:  # botocore ClientError
        code = str(response.get("Error", {}).get("Code", ""))
        if code in ("SlowDown", "RequestTimeout", "RequestTimeTooSkewed", "InternalError", "ServiceUnavailable",
                    "Throttling", "ThrottlingException"):
            return True
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status is None and code.isdigit():  # head_object 404/403 come back as Code "404"/"403"
            status = code
        return status is not None and _status_is_transient(status)
    try:
        from botocore.exceptions import ConnectionError as BotoConnectionError
        from botocore.exceptions import HTTPClientError, IncompleteReadError
        from s3transfer.exceptions import RetriesExceededError

        if isinstance(exc, (BotoConnectionError, HTTPClientError, IncompleteReadError, RetriesExceededError)):
            return True
    except ImportError:  # pragma: no cover - boto3 is a dependency
        pass
    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return False  # local path problems do not fix themselves
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))  # socket errors, empty download


def retry_call(fn: Callable[[], T], *, attempts: int, backoff_seconds: float, is_retryable: Callable[[BaseException], bool],
               what: str, log, sleep: Callable[[float], None] = time.sleep) -> T:
    """Call ``fn`` up to ``attempts`` times. Waits ``backoff_seconds`` x 1, 2, 4 ... (capped at
    MAX_BACKOFF_SECONDS) between attempts. Re-raises the last error, or a permanent one at once."""
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt >= attempts or not is_retryable(exc):
                if attempt > 1:
                    log.error("%s failed after %d attempt(s): %s", what, attempt, exc)
                raise
            wait = min(MAX_BACKOFF_SECONDS, max(0.0, float(backoff_seconds)) * 2 ** (attempt - 1))
            log.warning("%s failed (attempt %d/%d), retrying in %.0fs: %s", what, attempt, attempts, wait, exc)
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover
