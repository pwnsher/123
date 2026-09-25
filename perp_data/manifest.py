"""
Derivatives session manifest: <session>/perp/manifest.json, written atomically every few seconds and at the
end. It lists the DERIVATIVES sources separately from the Step-3 manifest (<session>/manifest.json, which the
combined collector links to), with venue semantics, contract-value / funding-interval state, counters, gaps
and fingerprints. No secrets (Step 3's assert_no_secrets is applied).
"""
import os
from dataclasses import asdict, dataclass, field
from typing import Optional

from market_data.manifest import assert_no_secrets, fingerprints as step3_fingerprints
from market_data.storage import write_json_atomic
from perp_data import APP_VERSION, PERP_FEATURE_SET_VERSION, PERP_SCHEMA_VERSION

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fingerprints(repo=REPO):
    fp = step3_fingerprints(repo)
    try:
        import json
        with open(os.path.join(repo, "config", "perp_data_baseline.json"), encoding="utf-8") as f:
            fp["perp_data_fingerprint"] = json.load(f).get("perp_data_fingerprint")
    except (OSError, ValueError):
        fp["perp_data_fingerprint"] = None
    return fp


@dataclass
class PerpManifest:
    session_id: str
    started_utc: str
    assets: list
    derivatives_sources: dict
    step3_session_dir: Optional[str] = None
    ended_utc: Optional[str] = None
    status: str = "RUNNING"
    app_version: str = APP_VERSION
    perp_schema_version: int = PERP_SCHEMA_VERSION
    perp_feature_set_version: str = PERP_FEATURE_SET_VERSION
    fingerprints: dict = field(default_factory=fingerprints)
    venue_semantics: dict = field(default_factory=dict)
    contract_values: dict = field(default_factory=dict)
    funding_intervals: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    disconnects: dict = field(default_factory=dict)
    reconnects: dict = field(default_factory=dict)
    parse_failures: dict = field(default_factory=dict)
    duplicates: dict = field(default_factory=dict)
    gaps: list = field(default_factory=list)
    clock_anomalies: list = field(default_factory=list)
    store: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)
    synthetic: bool = False
    research_only: bool = True

    def to_dict(self):
        d = asdict(self)
        assert_no_secrets(d)
        return d

    def write(self, perp_dir):
        write_json_atomic(os.path.join(perp_dir, "manifest.json"), self.to_dict())
