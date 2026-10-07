"""Minimal JSON-over-HTTPS client: timeouts, bounded retries with backoff,
429 handling, per-provider pacing, structured errors. Responses are parsed as
JSON data only; nothing a provider returns is ever executed.

Tests swap the transport with `set_transport`; no test touches the network.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Optional

from ..core.enums import ProviderStatus
from ..core.errors import ProviderError

USER_AGENT = "gem-radar/0.1 (+risk-analysis; read-only)"
MAX_BYTES = 8 * 1024 * 1024

# transport(method, url, body_bytes|None, headers, timeout) -> (status_code, body_bytes, headers)
Transport = Callable[[str, str, Optional[bytes], dict, float], tuple[int, bytes, dict]]


def _urllib_transport(method: str, url: str, body: Optional[bytes], headers: dict,
                      timeout: float) -> tuple[int, bytes, dict]:
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (https only)
            return resp.status, resp.read(MAX_BYTES + 1), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(MAX_BYTES + 1) if e.fp else b"", dict(e.headers or {})


_transport: Transport = _urllib_transport
_network_allowed = True
_last_call: dict[str, float] = {}
_lock = threading.Lock()
call_log: list[tuple[str, str]] = []  # (provider, method) — used by tests to count requests


def set_transport(t: Optional[Transport]) -> None:
    global _transport
    _transport = t or _urllib_transport


def set_network_allowed(allowed: bool) -> None:
    """The panel/data-only commands turn the network off: they must never fetch."""
    global _network_allowed
    _network_allowed = allowed


def network_allowed() -> bool:
    return _network_allowed and os.environ.get("GEM_RADAR_OFFLINE") != "1"


def _pace(provider: str, min_interval: float) -> None:
    with _lock:
        last = _last_call.get(provider, 0.0)
        wait = last + min_interval - time.monotonic()
        _last_call[provider] = max(time.monotonic(), last + min_interval)
    if wait > 0:
        time.sleep(wait)


def request_json(provider: str, url: str, *, method: str = "GET", payload: Any = None,
                 headers: Optional[dict] = None, timeout: float = 10.0, retries: int = 2,
                 backoff: float = 1.0, min_interval: float = 0.0) -> Any:
    if not url.startswith("https://"):
        raise ProviderError(provider, ProviderStatus.ERROR, "refusing non-HTTPS URL")
    if not network_allowed():
        raise ProviderError(provider, ProviderStatus.SKIPPED, "network disabled for this command")
    hdrs = {"Accept": "application/json", "User-Agent": USER_AGENT, **(headers or {})}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        hdrs["Content-Type"] = "application/json"
    attempt = 0
    while True:
        _pace(provider, min_interval)
        call_log.append((provider, method))
        try:
            status, data, rh = _transport(method, url, body, hdrs, timeout)
        except (socket.timeout, TimeoutError):
            err = ProviderError(provider, ProviderStatus.TIMEOUT, f"no answer within {timeout}s")
        except urllib.error.URLError as e:
            reason = str(getattr(e, "reason", e))
            if "403" in reason or "Tunnel connection failed" in reason:
                raise ProviderError(provider, ProviderStatus.BLOCKED,
                                    f"network policy refused the host ({reason})") from None
            if "timed out" in reason:
                err = ProviderError(provider, ProviderStatus.TIMEOUT, reason)
            else:
                err = ProviderError(provider, ProviderStatus.ERROR, f"network error: {reason}")
        except OSError as e:
            if "403" in str(e) or "Tunnel connection failed" in str(e):
                raise ProviderError(provider, ProviderStatus.BLOCKED,
                                    f"network policy refused the host ({e})") from None
            err = ProviderError(provider, ProviderStatus.ERROR, f"network error: {e}")
        else:
            if len(data) > MAX_BYTES:
                raise ProviderError(provider, ProviderStatus.PARSE_ERROR, "response too large")
            if status == 429:
                err = ProviderError(provider, ProviderStatus.RATE_LIMITED, "HTTP 429")
                retry_after = rh.get("Retry-After") or rh.get("retry-after")
                if retry_after and str(retry_after).isdigit():
                    backoff = max(backoff, min(float(retry_after), 10.0))
            elif status == 404:
                raise ProviderError(provider, ProviderStatus.NOT_FOUND, "HTTP 404")
            elif status >= 500:
                err = ProviderError(provider, ProviderStatus.HTTP_ERROR, f"HTTP {status}")
            elif status >= 400:
                raise ProviderError(provider, ProviderStatus.HTTP_ERROR, f"HTTP {status}")
            else:
                try:
                    return json.loads(data.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    raise ProviderError(provider, ProviderStatus.PARSE_ERROR,
                                        f"invalid JSON: {e}") from None
        attempt += 1
        if attempt > retries:
            raise err
        time.sleep(backoff * (2 ** (attempt - 1)))
