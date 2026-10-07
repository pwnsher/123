"""Provider response cache (SQLite). Stores normalized readings, never secrets."""
from __future__ import annotations

import json
import sqlite3
from typing import Optional

from ..core.enums import ProviderStatus, Status
from ..core.types import ProviderReport, Reading
from .freshness import classify


class ProviderCache:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def put(self, chain: str, contract: str, report: ProviderReport) -> None:
        if report.status != ProviderStatus.AVAILABLE or report.fetched_at is None:
            return
        payload = {"message": report.message, "extra": _jsonable(report.extra),
                   "readings": [r.to_dict() for r in report.readings]}
        self.conn.execute(
            "INSERT OR REPLACE INTO provider_cache(provider, chain, contract, fetched_at, payload_json)"
            " VALUES (?,?,?,?,?)",
            (report.name, chain, contract, report.fetched_at, json.dumps(payload)))
        self.conn.commit()

    def get(self, provider: str, chain: str, contract: str, *, now: float, cfg: dict,
            max_age: Optional[float] = None) -> Optional[ProviderReport]:
        row = self.conn.execute(
            "SELECT fetched_at, payload_json FROM provider_cache WHERE provider=? AND chain=? "
            "AND contract=?", (provider, chain, contract)).fetchone()
        if row is None:
            return None
        fetched_at = float(row["fetched_at"])
        if max_age is not None and now - fetched_at > max_age:
            return None
        payload = json.loads(row["payload_json"])
        readings = []
        for d in payload.get("readings", []):
            r = Reading(d["metric"], d["value"], d["source"], d["fetched_at"], Status.CACHED,
                        d.get("raw"), d.get("note", ""), d.get("is_mock", False))
            r.status = classify(r.metric, r.fetched_at, now, cfg, live=False)
            readings.append(r)
        extra = payload.get("extra") or {}
        return ProviderReport(provider, ProviderStatus.CACHED, fetched_at,
                              payload.get("message", ""), readings, extra)


def _jsonable(x):
    if isinstance(x, set):
        return sorted(x)
    if isinstance(x, dict):
        return {k: _jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_jsonable(v) for v in x]
    return x
