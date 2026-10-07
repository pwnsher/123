"""/radar: status and the panel's data. Local reads only — the network is
switched off for these calls, and no model is ever involved."""
from __future__ import annotations

import os
import sqlite3
from typing import Optional

from .. import __version__
from ..core.config import ENV_CONFIG_PATH, data_dir, home_dir, load_config
from ..data import http
from ..data.providers import default_providers
from ..storage import db, history, watchlist
from ..ui.panel import panel_data

CREDENTIAL_ENV = ["SOLANA_RPC_URL", "HONEYPOT_IS_API_KEY", "GEM_RADAR_RPC_ETHEREUM",
                  "GEM_RADAR_RPC_BSC", "GEM_RADAR_RPC_BASE", "GEM_RADAR_RPC_ARBITRUM",
                  "GEM_RADAR_RPC_POLYGON", "GEM_RADAR_RPC_OPTIMISM", "GEM_RADAR_RPC_AVALANCHE"]


def status(conn: Optional[sqlite3.Connection] = None) -> dict:
    http.set_network_allowed(False)
    cfg = load_config()
    conn = conn or db.connect()
    return {
        "version": __version__,
        "home": str(home_dir()),
        "config_file": os.environ.get(ENV_CONFIG_PATH) or str(home_dir() / "config.json"),
        "local_data_dir": str(data_dir()),
        "local_data_present": data_dir().is_dir(),
        "providers": [{"name": p.name, "tier": p.tier,
                       "chains": sorted(p.chains) if p.chains else "all"}
                      for p in default_providers(cfg)],
        # names only, never values
        "credentials_set": [k for k in CREDENTIAL_ENV if os.environ.get(k)],
        "scans_stored": conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0],
        "watchlist_size": len(watchlist.entries(conn)),
        "recent": [{"contract": s["contract"], "chain": s["chain"], "verdict": s["verdict"],
                    "score": s["final_score"]} for s in history.recent(conn, 5)],
    }


def panel(conn: Optional[sqlite3.Connection] = None) -> dict:
    http.set_network_allowed(False)
    return panel_data(conn or db.connect())
