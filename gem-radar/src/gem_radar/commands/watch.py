"""/gem-watch, /gem-unwatch, /gem-history. No network: chain comes from
history, the address format, or --chain."""
from __future__ import annotations

import sqlite3
from typing import Optional

from ..chains.detect import detect
from ..core.errors import GemRadarError
from ..storage import history, watchlist
from ..ui import formatters
from .recheck import known_chain


def _chain_for(conn: sqlite3.Connection, contract: str, chain: Optional[str]) -> tuple[str, str]:
    det = detect(contract)
    if not det.supported:
        raise GemRadarError(f"{det.family}: {det.reason}")
    if chain:
        if chain not in det.candidates:
            raise GemRadarError(f"a {det.family} address cannot be on {chain}")
        return det.address, chain
    c = known_chain(conn, det.address)
    if c:
        return det.address, c
    if det.family == "solana":
        return det.address, "solana"
    raise GemRadarError("EVM chain unknown: run /gem <contract> first or pass --chain <name>")


def add(conn: sqlite3.Connection, contract: str, chain: Optional[str] = None) -> str:
    try:
        addr, c = _chain_for(conn, contract, chain)
    except GemRadarError as e:
        return f"Not added: {e}"
    new = watchlist.add(conn, addr, c)
    return (f"{'Added' if new else 'Already watching'} {c} {addr}. "
            "No background monitoring runs: use /gem-recheck to refresh.")


def remove(conn: sqlite3.Connection, contract: str, chain: Optional[str] = None) -> str:
    try:
        addr = detect(contract).address
    except GemRadarError as e:
        return f"Not removed: {e}"
    n = watchlist.remove(conn, addr, chain)
    return f"Removed {n} watchlist entr{'y' if n == 1 else 'ies'} for {addr}."


def show_list(conn: sqlite3.Connection) -> str:
    return formatters.render_watchlist(watchlist.entries(conn))


def show_history(conn: sqlite3.Connection, contract: str, chain: Optional[str] = None,
                 limit: int = 10) -> tuple[str, dict]:
    try:
        addr = detect(contract).address
    except GemRadarError as e:
        return f"Invalid contract: {e}", {}
    scans = history.for_contract(conn, addr, chain, limit)
    diffs = []
    for newer, older in zip(scans, scans[1:], strict=False):
        if newer["chain"] == older["chain"]:
            diffs.append((older, newer, history.explain(history.diff(older, newer))))
    return formatters.render_history(addr, scans, diffs), {"scans": scans}
