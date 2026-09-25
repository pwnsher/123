"""GET-only JSON client for public market-data REST endpoints (no POST/PUT/DELETE exists here)."""
import requests


class HttpGetter:
    def __init__(self, timeout=5.0, session=None):
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = "kalshi-local-research-collector"

    def get_json(self, url, params=None):
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code == 429 or r.status_code >= 500:
            raise ConnectionError(f"HTTP {r.status_code}")
        r.raise_for_status()
        return r.json()
