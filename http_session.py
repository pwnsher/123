#!/usr/bin/env python3
"""
http_session.py — thread-local persistent HTTP sessions (connection pooling only).

Each thread gets its own requests.Session, so TCP/TLS connections are reused between polls
instead of being re-established on every requests.get(). Sessions are never shared across
threads. Request semantics are unchanged: callers pass the same URL, params and timeout as
before, and a cookie policy that refuses every cookie keeps each request as stateless as a
fresh requests.get() (no cookie carries over between requests). GET only; no network calls
happen at import time.
"""
import http.cookiejar
import threading

import requests

_local = threading.local()


def session():
    """This thread's persistent Session (created on first use)."""
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        s.cookies.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))   # stateless like requests.get
        _local.session = s
    return s


def get(url, **kwargs):
    """Drop-in for requests.get(url, **kwargs) over this thread's pooled connection."""
    return session().get(url, **kwargs)
