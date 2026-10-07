"""SQLite storage (stdlib). WAL mode keeps appends safe across processes."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from ..core.config import home_dir

SCHEMA = """
CREATE TABLE IF NOT EXISTS scans (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contract TEXT NOT NULL,
  chain TEXT NOT NULL,
  ts REAL NOT NULL,
  calculated_score REAL,
  final_score REAL,
  confidence REAL,
  verdict TEXT,
  components_json TEXT,
  red_flags_json TEXT,
  evidence_json TEXT,
  providers_json TEXT,
  missing_json TEXT,
  conflicts_json TEXT,
  model TEXT,
  analysis_mode TEXT,
  escalation_json TEXT,
  interpretation TEXT,
  is_mock INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS scans_by_contract ON scans(contract, chain, ts);
CREATE TABLE IF NOT EXISTS watchlist (
  contract TEXT NOT NULL,
  chain TEXT NOT NULL,
  added_at REAL NOT NULL,
  note TEXT DEFAULT '',
  PRIMARY KEY (contract, chain)
);
CREATE TABLE IF NOT EXISTS provider_cache (
  provider TEXT NOT NULL,
  chain TEXT NOT NULL,
  contract TEXT NOT NULL,
  fetched_at REAL NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (provider, chain, contract)
);
"""


def connect(path: Optional[Path] = None) -> sqlite3.Connection:
    p = Path(path) if path else home_dir() / "radar.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn
