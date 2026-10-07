"""/gem-recheck <contract>: bypass the response cache (providers still paced and
rate-limited), then show movement against the previous stored scan."""
from __future__ import annotations

import sqlite3
from typing import Optional

from ..storage import db, history
from . import gem


def known_chain(conn: sqlite3.Connection, contract: str) -> Optional[str]:
    row = conn.execute("SELECT chain FROM watchlist WHERE lower(contract)=lower(?) "
                       "UNION ALL SELECT chain FROM scans WHERE lower(contract)=lower(?) "
                       "LIMIT 1", (contract, contract)).fetchone()
    return row["chain"] if row else None


def recheck(contract: str, *, chain: Optional[str] = None, conn: Optional[sqlite3.Connection] = None,
            **kw) -> dict:
    conn = conn or db.connect()
    chain = chain or known_chain(conn, contract.strip())
    result = gem.scan(contract, chain=chain, refresh=True, conn=conn, **kw)
    if result.get("kind") == "verdict" and result.get("scan_id"):
        scans = history.for_contract(conn, result["contract"], result["chain"], limit=2)
        if len(scans) == 2:
            d = history.diff(scans[1], scans[0])
            result["movement"] = {"line": history.movement_line(d), "changes": history.explain(d)}
    return result
