"""Data for the Claude Code side panel: read from local storage only.
Rendering lives in the mod (hooks/register.tsx); this only shapes stored data."""
from __future__ import annotations

import sqlite3

from ..storage import history, watchlist


def _summary(s: dict) -> dict:
    flags = [f for f in (s.get("red_flags") or []) if f["severity"] == "CRITICAL"]
    ev = s.get("evidence") or {}
    statuses: dict[str, int] = {}
    for m in ev.values():
        if m.get("readings"):
            statuses[m["status"]] = statuses.get(m["status"], 0) + 1
    return {"id": s["id"], "contract": s["contract"], "chain": s["chain"], "ts": s["ts"],
            "verdict": s["verdict"], "score": s["final_score"], "confidence": s["confidence"],
            "mode": s["analysis_mode"], "model": s["model"], "is_mock": s["is_mock"],
            "critical": [f["message"] for f in flags], "freshness": statuses}


def panel_data(conn: sqlite3.Connection, limit: int = 25) -> dict:
    scans = history.recent(conn, limit)
    hist = []
    by_token: dict[tuple, list[dict]] = {}
    for s in scans:
        by_token.setdefault((s["contract"], s["chain"]), []).append(s)
    for s in scans:
        older = [o for o in by_token[(s["contract"], s["chain"])] if o["ts"] < s["ts"]]
        item = _summary(s)
        if older:
            d = history.diff(older[0], s)
            item["change"] = d["score"]["delta"]
            item["changes"] = history.explain(d)[:6]
            item["flags_added"] = d["flags_added"]
            item["flags_removed"] = d["flags_removed"]
        hist.append(item)
    return {
        "latest": _summary(scans[0]) if scans else None,
        "watchlist": watchlist.entries(conn),
        "history": hist,
    }
