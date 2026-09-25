"""
Session manifest (manifest.json in the session directory), written atomically at start, periodically
and at the end. No secrets: credentials are recorded only as "configured: true/false".
"""
import datetime as dt
import json
import os
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional

from market_data import APP_VERSION, FEATURE_SET_VERSION, MARKET_DATA_SCHEMA_VERSION
from market_data.storage import write_json_atomic

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SECRET_WORDS = ("secret", "token", "password", "private_key", "api_key", "authorization", "signature", "passphrase")


def utc_iso(ms=None):
    d = dt.datetime.now(dt.timezone.utc) if ms is None else dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
    return d.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_session_id(wall_ms):
    d = dt.datetime.fromtimestamp(wall_ms / 1000, dt.timezone.utc)
    return f"{d:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _read(path, key):
    try:
        with open(path, encoding="utf-8") as f:
            v = json.load(f)
        for k in key:
            v = v[k]
        return v
    except (OSError, ValueError, KeyError, TypeError):
        return None


def fingerprints(repo=REPO):
    """Recorded fingerprints (from the committed baselines; the collector also re-verifies the code)."""
    return {"legacy_strategy_fingerprint": _read(os.path.join(repo, "config", "strategy_baseline.json"),
                                                 ("fingerprints", "legacy_strategy_fingerprint")),
            "extended_strategy_fingerprint": _read(os.path.join(repo, "config", "strategy_baseline.json"),
                                                   ("fingerprints", "extended_strategy_fingerprint")),
            "settlement_fingerprint": _read(os.path.join(repo, "config", "settlement_baseline.json"),
                                            ("settlement_fingerprint",)),
            "market_data_fingerprint": _read(os.path.join(repo, "config", "market_data_baseline.json"),
                                             ("market_data_fingerprint",))}


def assert_no_secrets(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if any(w in str(k).lower() for w in _SECRET_WORDS):
                raise ValueError(f"secret-looking manifest key {path}{k}")
            assert_no_secrets(v, f"{path}{k}.")
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            assert_no_secrets(v, path)


@dataclass
class SessionManifest:
    session_id: str
    started_utc: str
    assets: list
    sources: dict                                  # name -> {enabled, critical, transport, config, version}
    ended_utc: Optional[str] = None
    status: str = "RUNNING"                        # RUNNING | COMPLETED | ABORTED | DRY_RUN
    app_version: str = APP_VERSION
    market_data_schema_version: int = MARKET_DATA_SCHEMA_VERSION
    feature_set_version: str = FEATURE_SET_VERSION
    fingerprints: dict = field(default_factory=fingerprints)
    code_verification: dict = field(default_factory=dict)
    disconnects: dict = field(default_factory=dict)
    reconnects: dict = field(default_factory=dict)
    dropped_messages: dict = field(default_factory=dict)
    parse_failures: dict = field(default_factory=dict)
    duplicates: dict = field(default_factory=dict)
    gaps: list = field(default_factory=list)
    clock_anomalies: list = field(default_factory=list)
    store: dict = field(default_factory=dict)
    settlement_store: Optional[str] = None
    notes: list = field(default_factory=list)
    research_only: bool = True

    def to_dict(self):
        d = asdict(self)
        assert_no_secrets(d)
        return d

    def write(self, session_dir):
        write_json_atomic(os.path.join(session_dir, "manifest.json"), self.to_dict())


def load_manifest(session_dir):
    with open(os.path.join(session_dir, "manifest.json"), encoding="utf-8") as f:
        return json.load(f)
