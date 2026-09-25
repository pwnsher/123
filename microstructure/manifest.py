"""
Microstructure session manifest: <session>/micro/manifest.json (atomic writes, no secrets - Step 3's
assert_no_secrets). Lists the book venues with their sequence policy and classification, depth / storage settings,
per-book reconstruction counters (snapshots, resnapshots, gaps, invalid / crossed books, checksum results),
drops / overloads (always recorded, never silent), latency and clock notes, and all fingerprints.
"""
import json
import os
from dataclasses import asdict, dataclass, field
from typing import Optional

from market_data.manifest import assert_no_secrets
from market_data.storage import write_json_atomic
from microstructure import APP_VERSION, MICRO_FEATURE_SET_VERSION, MICRO_SCHEMA_VERSION
from perp_data.manifest import fingerprints as step4_fingerprints

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fingerprints(repo=REPO):
    fp = step4_fingerprints(repo)
    try:
        with open(os.path.join(repo, "config", "microstructure_baseline.json"), encoding="utf-8") as f:
            fp["microstructure_fingerprint"] = json.load(f).get("microstructure_fingerprint")
    except (OSError, ValueError):
        fp["microstructure_fingerprint"] = None
    return fp


@dataclass
class MicroManifest:
    session_id: str
    started_utc: str
    assets: list
    book_sources: dict
    step3_session_dir: Optional[str] = None
    ended_utc: Optional[str] = None
    status: str = "RUNNING"
    app_version: str = APP_VERSION
    micro_schema_version: int = MICRO_SCHEMA_VERSION
    micro_feature_set_version: str = MICRO_FEATURE_SET_VERSION
    fingerprints: dict = field(default_factory=fingerprints)
    venue_semantics: dict = field(default_factory=dict)
    storage_settings: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    books: dict = field(default_factory=dict)
    disconnects: dict = field(default_factory=dict)
    reconnects: dict = field(default_factory=dict)
    resnapshot_requests: dict = field(default_factory=dict)
    parse_failures: dict = field(default_factory=dict)
    drops: dict = field(default_factory=dict)
    gaps: list = field(default_factory=list)
    clock_anomalies: list = field(default_factory=list)
    latency: dict = field(default_factory=dict)
    clock_quality_note: str = ("receive_ts is the local wall clock (NTP-quality at best); exchange event times use each "
                               "venue's clock. Latency = receive_ts - event_ts mixes transport delay and clock offset; "
                               "sub-10 ms differences are not interpretable.")
    store: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)
    synthetic: bool = False
    research_only: bool = True

    def to_dict(self):
        d = asdict(self)
        assert_no_secrets(d)
        return d

    def write(self, micro_dir):
        write_json_atomic(os.path.join(micro_dir, "manifest.json"), self.to_dict())
