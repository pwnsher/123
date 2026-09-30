"""GET-only JSON client for public market-data REST endpoints (no POST/PUT/DELETE exists here).

Step 6.4: every failure is raised as a typed HttpError (a ConnectionError, as before) carrying the status code, a
SAFE endpoint identity (host + path only: no query string, no credentials) and an explicit classification:

    RATE_LIMITED   429                         retryable (normal backoff)
    SERVER_ERROR   5xx                         retryable
    NETWORK        timeout / connection error  retryable
    ACCESS_DENIED  403, 451                    TERMINAL for this source / stream for the current session: the
                                               provider refuses this public endpoint from this location. Never
                                               retried in a loop, never bypassed (no proxies, no alternate
                                               jurisdiction endpoints): the data is recorded as UNAVAILABLE.
    CLIENT_ERROR   any other 4xx               not retryable by itself; the caller's own schedule decides, and the
                                               request fails closed (nothing is parsed from it)
"""
from urllib.parse import urlsplit

import requests

TERMINAL_STATUSES = (403, 451)


def safe_endpoint(url):
    """host + path of a URL: no scheme credentials, no query string (symbols / cursors are not identity)."""
    try:
        u = urlsplit(str(url))
    except ValueError:
        return "?"
    host = u.hostname or ""
    if u.port:
        host = f"{host}:{u.port}"
    return f"{host}{u.path}"


def classify(status):
    """-> (kind, retryable, terminal) for an HTTP status code (None = no response: network)."""
    if status is None:
        return "NETWORK", True, False
    if status == 429:
        return "RATE_LIMITED", True, False
    if status >= 500:
        return "SERVER_ERROR", True, False
    if status in TERMINAL_STATUSES:
        return "ACCESS_DENIED", False, True
    if 400 <= status < 500:
        return "CLIENT_ERROR", False, False
    return "UNEXPECTED_STATUS", False, False


class HttpError(ConnectionError):
    def __init__(self, status, url, detail=""):
        self.status = status
        self.endpoint = safe_endpoint(url)
        self.kind, self.retryable, self.terminal = classify(status)
        self.detail = str(detail)[:120]
        super().__init__(f"HTTP {status if status is not None else '-'} {self.kind} {self.endpoint}"
                         + (f": {self.detail}" if self.detail else ""))

    def to_dict(self):
        return {"status": self.status, "endpoint": self.endpoint, "kind": self.kind, "retryable": self.retryable,
                "terminal": self.terminal}


def is_terminal(err):
    return isinstance(err, HttpError) and err.terminal


class HttpGetter:
    def __init__(self, timeout=5.0, session=None):
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = "kalshi-local-research-collector"

    def get_json(self, url, params=None):
        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as e:                     # timeout / connection reset / DNS
            raise HttpError(None, url, type(e).__name__) from e
        if r.status_code >= 400:
            raise HttpError(r.status_code, url, getattr(r, "reason", "") or "")
        try:
            return r.json()
        except ValueError as e:
            raise HttpError(r.status_code, url, f"invalid JSON body: {e}") from e
