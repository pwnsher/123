"""
The FROZEN Step-6 feature universe: every candidate input from Steps 2-5, one immutable, fingerprinted manifest.

    build_universe()          -> {"version", "records": [...], "fingerprint", ...}  (derived from the frozen registries)
    load_frozen()             -> the stored manifest (config/step6_feature_universe.json)
    verify_frozen()           -> (ok, problems): the registries still produce EXACTLY the frozen records; a change in
                                 records without a new FEATURE_UNIVERSE_VERSION is refused (never silently accepted)

Record fields
    name, layer (STEP2_SETTLEMENT | STEP3_SPOT | STEP4_PERP | STEP5_MICRO), family (Step-6 evaluation family),
    source_family (the family / group already present in the repository - preserved), source (venue / feed),
    assets (applicability), window, units, value_type (numeric | enum), status_rules, min_warmup_ms,
    causal_rule, feature_version (the layer's feature-set version), source_fingerprint (the layer's frozen code
    fingerprint), role, alias_of, tags.

Roles (Step-6 addendum)
    MODEL_CANDIDATE     may enter a predictive model (after structural pruning)
    EXECUTION_STATE     executable market state (YES/NO bid/ask, executable asks, top-of-book sizes): kept in every
                        research row for economics, NOT an automatic model input (complements are deterministic)
    ALIAS               an algebraic duplicate of another feature (e.g. NO mid = 100 - YES mid): kept for provenance
    DATA_QUALITY        ages, readiness flags, latency, update counts, coverage counters, enum states: used for
                        eligibility / quality reporting, never as predictive inputs
Tags
    SESSION_NORMALIZED            normalization depends on the current session only (funding percentile, VPIN-style)
    SETTLEMENT_CONVENTION_DEPENDENT   derived from the Step-2 settlement accumulator (window convention unverified)
    COINBASE_L2_SEQUENCE          derived from the Coinbase Advanced Trade book (sequence semantics must be validated)
No candidate indicator may be added after freezing: a new input requires a new FEATURE_UNIVERSE_VERSION and a
deliberate rewrite of the frozen manifest (recorded in docs/STEP6_FEATURE_EVALUATION.md).
"""
import hashlib
import json
import os
import re

from feature_eval import FEATURE_UNIVERSE_VERSION

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FROZEN_PATH = os.path.join(REPO, "config", "step6_feature_universe.json")
ASSETS = ("BTC", "ETH", "SOL", "XRP")
LAYERS = ("STEP2_SETTLEMENT", "STEP3_SPOT", "STEP4_PERP", "STEP5_MICRO")

STEP3_FAMILY = {"price": "SPOT_PRICE_MOMENTUM", "volatility": "SPOT_VOLATILITY", "flow": "SPOT_FLOW",
                "volume": "SPOT_VOLUME", "cross": "CROSS_EXCHANGE_SPOT", "cf_vs_spot": "CF_VS_SPOT",
                "strike": "STRIKE_DISTANCE", "kalshi": "KALSHI_MARKET_STATE", "settlement": "SETTLEMENT"}
STEP4_FAMILY = {"PRICE": "PERP_PRICE", "BASIS": "BASIS", "FUNDING": "FUNDING", "OPEN_INTEREST": "OPEN_INTEREST",
                "LIQUIDATION": "LIQUIDATION", "FLOW": "PERP_FLOW", "CVD": "CVD", "ORDERBOOK": "PERP_ORDERBOOK",
                "CROSS_EXCHANGE": "PERP_CROSS_EXCHANGE", "PERP_SPOT_DIVERGENCE": "PERP_SPOT_DIVERGENCE"}

EXECUTION_STATE = {"kalshi.yes_bid", "kalshi.yes_ask", "kalshi.no_bid", "kalshi.no_ask", "kalshi.exec_yes_ask",
                   "kalshi.exec_no_ask", "kalshi.depth.yes_bid_qty", "kalshi.depth.yes_ask_qty",
                   "micro.kalshi.yes_bid", "micro.kalshi.yes_ask", "micro.kalshi.no_bid", "micro.kalshi.no_ask",
                   "micro.kalshi.yes_bid_size", "micro.kalshi.yes_ask_size", "micro.kalshi.no_bid_size",
                   "micro.kalshi.no_ask_size"}
# algebraic duplicates: name -> the representation kept as the candidate
ALIASES = {"kalshi.no_mid": "kalshi.yes_mid", "kalshi.no_spread": "kalshi.yes_spread",
           "kalshi.yes_mid": "kalshi.implied_prob", "settle.seconds_remaining": "strike.seconds_remaining"}
DATA_QUALITY_PATTERNS = (r"\.book_ready$", r"\.books_ready$", r"latency_median_ms", r"\.book_updates\.", r"\.book_age_ms$",
                         r"_age_s$", r"\.state_age_s$", r"^settle\.(quality|phase|observations_seen|samples_expected|"
                         r"samples_filled|coverage_elapsed)$", r"^xex\.n_sources$")
SESSION_NORMALIZED_PATTERNS = (r"funding_percentile$", r"vpin_style")


def _window_ms(w):
    """'5s' -> 5000, '3m' -> 180000, '15s/5m' -> the longest part, '-' -> 0; None for session / open windows."""
    if w in (None, "", "-"):
        return 0
    parts = str(w).split("/")
    out = 0
    for p in parts:
        m = re.fullmatch(r"(\d+)(ms|s|m|h)", p.strip())
        if not m:
            return None
        n, u = int(m.group(1)), m.group(2)
        out = max(out, n * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}[u])
    return out


def _warmup(name, window, layer):
    w = _window_ms(window)
    if w is None:                                   # 'session', 'open', 'buckets': warm-up = the session / market
        if "vpin" in name:
            return 600_000                          # VPIN-style calibration minutes (MicroFeatureConfig)
        if window == "open":
            return 900_000
        return 300_000                              # funding percentile: >= 30 obs spanning >= 5 min (PerpFeatureConfig)
    if "large_trade" in name:
        return max(w, 900_000)
    if name.endswith("vs_5m_median") or "liquidity_vacuum" in name:
        return max(w, 300_000)
    return w


def _layer_fingerprints():
    fp = {}
    for key, rel, field in (("settlement", "settlement_baseline.json", "settlement_fingerprint"),
                            ("market_data", "market_data_baseline.json", "market_data_fingerprint"),
                            ("perp_data", "perp_data_baseline.json", "perp_data_fingerprint"),
                            ("microstructure", "microstructure_baseline.json", "microstructure_fingerprint")):
        try:
            with open(os.path.join(REPO, "config", rel), encoding="utf-8") as f:
                fp[key] = json.load(f)[field]
        except (OSError, ValueError, KeyError):
            fp[key] = None
    return fp


def _role(name):
    if name in EXECUTION_STATE:
        return "EXECUTION_STATE", None
    if name in ALIASES:
        return "ALIAS", ALIASES[name]
    if any(re.search(p, name) for p in DATA_QUALITY_PATTERNS):
        return "DATA_QUALITY", None
    return "MODEL_CANDIDATE", None


def _tags(name, layer, family):
    t = []
    if any(re.search(p, name) for p in SESSION_NORMALIZED_PATTERNS):
        t.append("SESSION_NORMALIZED")
    if family == "SETTLEMENT":
        t.append("SETTLEMENT_CONVENTION_DEPENDENT")
    if name.startswith("micro.coinbase.") or name in ("micro.x.spot_mid_dispersion_bps", "micro.x.imbalance_10_median_spot"):
        t.append("COINBASE_L2_SEQUENCE")
    return t


STATUS_RULES = {
    "STEP2_SETTLEMENT": "Step-3 statuses (READY / NOT_READY / MISSING / UNAVAILABLE / UNDEFINED); value None unless READY; "
                        "settlement accumulator state as of T from CF observations available at T",
    "STEP3_SPOT": "Step-3 statuses; value None unless READY; windows require the source observed throughout",
    "STEP4_PERP": "Step-4 statuses; value None unless READY; USDT converted before USD comparisons; OKX coin sizes only "
                  "with verified contract values",
    "STEP5_MICRO": "Step-5 statuses; level features READY only when the local book is READY; windowed features need the "
                   "book valid over the whole window; missing book -> None + MISSING (never 0)",
}
CAUSAL_RULE = "usable at checkpoint T iff every input event has receive_ts_ms <= T (one pass, availability order)"


def build_universe():
    from market_data import FEATURE_SET_VERSION as S3V, MARKET_DATA_SCHEMA_VERSION
    from market_data.features.definitions import FEATURES as S3
    from microstructure import MICRO_FEATURE_SET_VERSION
    from microstructure.features.definitions import FEATURES as M5
    from perp_data import PERP_FEATURE_SET_VERSION
    from perp_data.features.definitions import FEATURES as P4
    fps = _layer_fingerprints()
    recs = []
    for f in S3:
        layer = "STEP2_SETTLEMENT" if f.group == "settlement" else "STEP3_SPOT"
        fam = STEP3_FAMILY[f.group]
        role, alias = _role(f.name)
        recs.append({"name": f.name, "layer": layer, "family": fam, "source_family": f.group, "source": f.source,
                     "window": f.window, "units": f.unit, "description": f.description,
                     "value_type": "enum" if f.unit == "enum" else "numeric",
                     "feature_version": f"{S3V} (market_data schema {MARKET_DATA_SCHEMA_VERSION})",
                     "source_fingerprint": fps["settlement"] if layer == "STEP2_SETTLEMENT" else fps["market_data"],
                     "role": role, "alias_of": alias})
    for f in P4:
        role, alias = _role(f.name)
        recs.append({"name": f.name, "layer": "STEP4_PERP", "family": STEP4_FAMILY[f.family], "source_family": f.family,
                     "source": f.venue, "window": f.window, "units": f.unit, "description": f.description,
                     "value_type": "numeric", "feature_version": PERP_FEATURE_SET_VERSION,
                     "source_fingerprint": fps["perp_data"], "role": role, "alias_of": alias})
    for f in M5:
        role, alias = _role(f.name)
        recs.append({"name": f.name, "layer": "STEP5_MICRO", "family": f.family, "source_family": f.family,
                     "source": f.venue, "window": f.window, "units": f.unit, "description": f.description,
                     "value_type": "numeric", "feature_version": MICRO_FEATURE_SET_VERSION,
                     "source_fingerprint": fps["microstructure"], "role": role, "alias_of": alias})
    for r in recs:
        r["assets"] = list(ASSETS)
        r["status_rules"] = STATUS_RULES[r["layer"]]
        r["min_warmup_ms"] = _warmup(r["name"], r["window"], r["layer"])
        r["causal_rule"] = CAUSAL_RULE
        r["tags"] = _tags(r["name"], r["layer"], r["family"])
    names = [r["name"] for r in recs]
    if len(set(names)) != len(names):
        raise AssertionError("duplicate feature names across layers")
    for r in recs:
        if r["alias_of"] is not None and r["alias_of"] not in names:
            raise AssertionError(f"alias target missing: {r['name']} -> {r['alias_of']}")
    recs.sort(key=lambda r: (LAYERS.index(r["layer"]), r["name"]))
    body = {"version": FEATURE_UNIVERSE_VERSION, "records": recs}
    body["records_sha256"] = _sha(recs)
    body["fingerprint"] = _sha({"version": FEATURE_UNIVERSE_VERSION, "records_sha256": body["records_sha256"]})
    body["layer_fingerprints"] = fps
    body["counts"] = counts(recs)
    body["max_causal_lookback_ms"] = max_lookback_ms()
    return body


def _sha(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def counts(recs):
    out = {"total": len(recs), "by_layer": {}, "by_family": {}, "by_role": {}, "by_tag": {}}
    for r in recs:
        for k, v in (("by_layer", r["layer"]), ("by_family", r["family"]), ("by_role", r["role"])):
            out[k][v] = out[k].get(v, 0) + 1
        for t in r["tags"]:
            out["by_tag"][t] = out["by_tag"].get(t, 0) + 1
    for k in ("by_layer", "by_family", "by_role", "by_tag"):
        out[k] = dict(sorted(out[k].items()))
    return out


def max_lookback_ms():
    """The longest causal history ANY frozen engine may read (its retention), used for purge / embargo."""
    from market_data.features.definitions import FeatureConfig
    from microstructure.features.definitions import MicroFeatureConfig
    from perp_data.features.definitions import PerpFeatureConfig
    return max(FeatureConfig().retention_ms, PerpFeatureConfig().retention_ms, MicroFeatureConfig().retention_ms)


def load_frozen(path=FROZEN_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def verify_frozen(path=FROZEN_PATH):
    """The frozen manifest must equal what the frozen registries produce. Any record change without a version change
    is refused, and so is a stored fingerprint that does not match its own records."""
    try:
        stored = load_frozen(path)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build_universe()
    problems = []
    if stored.get("records_sha256") != _sha(stored.get("records", [])):
        problems.append("stored records do not match their stored records_sha256 (edited by hand?)")
    if stored.get("fingerprint") != _sha({"version": stored.get("version"), "records_sha256": stored.get("records_sha256")}):
        problems.append("stored fingerprint does not match its version + records")
    if stored.get("records_sha256") != cur["records_sha256"]:
        if stored.get("version") == cur["version"]:
            problems.append("FEATURE UNIVERSE CHANGED WITHOUT A VERSION CHANGE (new FEATURE_UNIVERSE_VERSION required)")
        else:
            problems.append(f"feature universe version {stored.get('version')} != current {cur['version']}")
    if stored.get("version") != cur["version"]:
        problems.append(f"version mismatch: frozen {stored.get('version')} vs code {cur['version']}")
    return not problems, problems


def names(records, roles=("MODEL_CANDIDATE",), families=None, layers=None):
    return [r["name"] for r in records if r["role"] in roles and (families is None or r["family"] in families)
            and (layers is None or r["layer"] in layers)]


def families_by_layer(records):
    out = {}
    for r in records:
        if r["role"] == "MODEL_CANDIDATE":
            out.setdefault(r["layer"], set()).add(r["family"])
    return {k: sorted(v) for k, v in out.items()}


def write_frozen(path=FROZEN_PATH):
    body = build_universe()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(body, f, indent=1, sort_keys=True)
        f.write("\n")
    return body
