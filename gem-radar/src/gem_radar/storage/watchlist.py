"""Persistent watchlist. Adding a contract does NOT start background monitoring:
re-scoring happens only when the user runs /gem-recheck (or /gem-watch recheck)."""
from __future__ import annotations

import sqlite3
import time

from . import history


def add(conn: sqlite3.Connection, contract: str, chain: str, note: str = "") -> bool:
    cur = conn.execute("INSERT OR IGNORE INTO watchlist(contract, chain, added_at, note) "
                       "VALUES (?,?,?,?)", (contract, chain, time.time(), note))
    conn.commit()
    return cur.rowcount > 0


def remove(conn: sqlite3.Connection, contract: str, chain: str | None = None) -> int:
    if chain:
        cur = conn.execute("DELETE FROM watchlist WHERE lower(contract)=lower(?) AND chain=?",
                           (contract, chain))
    else:
        cur = conn.execute("DELETE FROM watchlist WHERE lower(contract)=lower(?)", (contract,))
    conn.commit()
    return cur.rowcount


def entries(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM watchlist ORDER BY added_at").fetchall()
    out = []
    for r in rows:
        scans = history.for_contract(conn, r["contract"], r["chain"], limit=2)
        e = {"contract": r["contract"], "chain": r["chain"], "added_at": r["added_at"],
             "current_score": None, "previous_score": None, "change": None, "verdict": None,
             "confidence": None, "scanned_at": None, "changes": []}
        if scans:
            cur = scans[0]
            e.update(current_score=cur["final_score"], verdict=cur["verdict"],
                     confidence=cur["confidence"], scanned_at=cur["ts"])
            if len(scans) > 1:
                d = history.diff(scans[1], cur)
                e.update(previous_score=scans[1]["final_score"], change=d["score"]["delta"],
                         movement=history.movement_line(d), changes=history.explain(d))
        out.append(e)
    return out
