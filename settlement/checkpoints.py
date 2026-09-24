"""
Causal time-to-close checkpoints: one record per (market, checkpoint) with FEATURES and LABELS apart.

    FEATURES  computed by features_at(), which receives ONLY the observations visible at the
              checkpoint (filtered here, then filtered again inside reconstruct) and never receives
              an OfficialResolution. The allowed names are FEATURE_FIELDS; nothing else can appear.
    LABELS    computed by labels_for() from the final (label-mode) reconstruction and the official
              Kalshi resolution. They exist for training/evaluation targets only.

Leakage is tested by perturbation (test_stage18): changing anything that becomes available after a
checkpoint, or the official result, must leave that checkpoint's features byte-identical; a
deliberately leaky builder is shown to be caught.
"""
import csv
import json
import os
import tempfile
from dataclasses import dataclass

from settlement import ENGINE_VERSION
from settlement.policy import available_ts, reconstruction_policy, window_policy
from settlement.reconstruction import issue_visible, reconstruct

DEFAULT_CHECKPOINTS_S = (600, 480, 360, 300, 240, 180, 120, 90, 60, 30, 0)

KEY_FIELDS = ("market_ticker", "asset", "index_id", "strike", "close_ts_ms", "checkpoint_seconds_remaining",
              "checkpoint_ts_ms", "window_policy_id", "reconstruction_policy_id", "engine_version")
FEATURE_FIELDS = ("phase", "seconds_remaining", "current_index", "current_index_event_ts_ms", "last_observation_age_s",
                  "observations_seen", "samples_expected", "samples_elapsed", "samples_filled",
                  "samples_interpolated", "samples_missing", "samples_remaining", "coverage_elapsed",
                  "accumulated_sum", "accumulated_mean", "first_included_ts_ms", "last_included_ts_ms",
                  "max_gap_s", "quality", "flags", "sources")
LABEL_FIELDS = ("final_settlement_value", "reconstructed_outcome", "final_quality", "final_coverage",
                "official_result", "official_expiration_value", "reconstruction_matches_official")


@dataclass(frozen=True)
class CheckpointRecord:
    key: dict
    features: dict
    labels: dict

    def to_row(self):
        row = dict(self.key)
        row.update({f"f_{k}": v for k, v in self.features.items()})
        row.update({f"y_{k}": v for k, v in self.labels.items()})
        return row


def _plain(v):
    if isinstance(v, tuple):
        return ";".join(str(x) for x in v)
    return getattr(v, "value", v)


def features_at(market, observations, checkpoint_ts_ms, wpol=None, rpol=None, issues=()):
    """FEATURES at one checkpoint. Takes no resolution argument by design."""
    wpol = wpol or window_policy()
    rpol = rpol or reconstruction_policy()
    seen = [o for o in observations
            if available_ts(o, rpol) <= checkpoint_ts_ms and o.event_ts_ms <= checkpoint_ts_ms]
    seen_issues = [i for i in (issues or ()) if issue_visible(i, checkpoint_ts_ms)]
    st = reconstruct(market, seen, wpol, rpol, as_of_ms=checkpoint_ts_ms, issues=seen_issues).state
    d = st.to_dict()
    return {k: _plain(d[k]) if not isinstance(d[k], list) else ";".join(d[k]) for k in FEATURE_FIELDS}


def labels_for(market, observations, resolution=None, wpol=None, rpol=None, issues=()):
    """LABELS: final reconstruction (all data) + the official Kalshi outcome."""
    res = reconstruct(market, observations, wpol, rpol, as_of_ms=None, issues=issues)
    official = resolution.result if resolution is not None else None
    match = None
    if official is not None and res.reconstructed_outcome is not None:
        match = res.reconstructed_outcome == official
    st = res.state
    return {"final_settlement_value": res.final_value, "reconstructed_outcome": res.reconstructed_outcome,
            "final_quality": st.quality.value,
            "final_coverage": (st.samples_filled / st.samples_expected) if st.samples_expected else None,
            "official_result": official,
            "official_expiration_value": resolution.expiration_value if resolution is not None else None,
            "reconstruction_matches_official": match}


def build_checkpoints(market, observations, resolution=None, wpol=None, rpol=None,
                      checkpoints_s=DEFAULT_CHECKPOINTS_S, issues=()):
    wpol = wpol or window_policy()
    rpol = rpol or reconstruction_policy()
    labels = labels_for(market, observations, resolution, wpol, rpol, issues)
    out = []
    for s in checkpoints_s:
        if s < 0:
            raise ValueError("checkpoints are seconds BEFORE close (>= 0)")
        t = market.close_ts_ms - int(round(s * 1000))
        key = {"market_ticker": market.ticker, "asset": market.asset, "index_id": market.index_id,
               "strike": market.strike, "close_ts_ms": market.close_ts_ms, "checkpoint_seconds_remaining": s,
               "checkpoint_ts_ms": t, "window_policy_id": wpol.policy_id,
               "reconstruction_policy_id": rpol.policy_id, "engine_version": ENGINE_VERSION}
        feats = features_at(market, observations, t, wpol, rpol, issues)
        out.append(CheckpointRecord(key, feats, dict(labels)))
    return out


def assert_separated(records):
    """Structural guard: feature names are exactly FEATURE_FIELDS and disjoint from labels."""
    for r in records:
        if tuple(r.features) != FEATURE_FIELDS:
            raise AssertionError(f"unexpected feature fields: {sorted(set(r.features) ^ set(FEATURE_FIELDS))}")
        if set(r.features) & set(r.labels) or set(r.key) & set(r.labels):
            raise AssertionError("a label name appears among keys/features")
    return True


def write_dataset(records, path):
    """Atomic write (temp file + rename). .jsonl -> one JSON object per record; .csv -> flat columns."""
    assert_separated(records)
    rows = [r.to_row() for r in records]
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp_ckpt_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            if path.endswith(".csv"):
                cols = list(KEY_FIELDS) + [f"f_{k}" for k in FEATURE_FIELDS] + [f"y_{k}" for k in LABEL_FIELDS]
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                w.writerows(rows)
            else:
                for r in records:
                    f.write(json.dumps({"key": r.key, "features": r.features, "labels": r.labels},
                                       sort_keys=True, allow_nan=False) + "\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return len(rows)
