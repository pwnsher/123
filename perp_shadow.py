#!/usr/bin/env python3
"""
perp_shadow.py — STEP 4 SHADOW-ONLY evaluation of frozen perp filter policies.

It receives COPIES of the causal telemetry rows that the Step 2 telemetry worker already
built (after the real bot has made and logged its call) and records what a frozen policy
WOULD have said for the FIRST binary signal of each ticker:

    ALLOW / WOULD_BLOCK / UNAVAILABLE / NOT_APPLICABLE

It never fetches data, never calls Kalshi or Coinbase, never changes a signal, call, size,
stop or post, and contains no execution code. Every policy is mode SHADOW_ONLY.

Conflict score (favoured side), using the policy's frozen Step 3 MODEL B and MODEL C:
    P_B_FAV = P_B_UP if fav == UP else 1 - P_B_UP
    P_C_FAV = P_C_UP if fav == UP else 1 - P_C_UP
    conflict_score = P_C_FAV - P_B_FAV
    WOULD_BLOCK  iff  conflict_score <= frozen threshold (< 0)
"""
import csv
import datetime as dt
import hashlib
import json
import math
import os
import threading
import time

import analyze_perp_predictive as ap

POLICY_SCHEMA_VERSION = 1
SHADOW_SCHEMA_VERSION = 1
MODE = "SHADOW_ONLY"

ALLOW = "ALLOW"
WOULD_BLOCK = "WOULD_BLOCK"
UNAVAILABLE = "UNAVAILABLE"
NOT_APPLICABLE = "NOT_APPLICABLE"

R_ALLOW = "ALLOW_SCORE_ABOVE_THRESHOLD"
R_BLOCK = "WOULD_BLOCK_PERP_CONFLICT"
R_NOT_READY = "UNAVAILABLE_ANALYSIS_NOT_READY"
R_FEATURE = "UNAVAILABLE_FEATURE_MISSING"
R_CONTROL = "UNAVAILABLE_CONTROL_MISSING"
R_BASE = "UNAVAILABLE_BASE_PROBABILITY_MISSING"
R_FAV = "UNAVAILABLE_FAV_MISSING"
R_SCALE = "UNAVAILABLE_POLICY_SCALE"
R_NUMERIC = "UNAVAILABLE_NUMERIC_ERROR"
R_MISSED = "UNAVAILABLE_FIRST_SIGNAL_ROW_MISSED"
R_NA_COIN = "NOT_APPLICABLE_COIN"
R_NA_COHORT = "NOT_APPLICABLE_COHORT"

REQUIRED_POLICY_KEYS = (
    "policy_id", "policy_hash", "policy_schema_version", "policy_version", "mode", "synthetic",
    "created_at_utc", "feature_name", "feature_family", "reliability_interaction", "cohort",
    "coin_or_group", "by_coin_scaling", "discovery_cutoff_utc", "discovery_cutoff_epoch",
    "controls", "scalers", "model_b_coefficients", "model_c_coefficients", "conflict_threshold",
    "telemetry_schema_version", "feature_version", "source_hashes")
FORBIDDEN_POLICY_KEY_WORDS = ("enabled", "trading", "live", "execute", "order", "size", "veto_level")

JOURNAL_COLUMNS = [
    "shadow_schema_version",
    "policy_id", "policy_hash", "policy_feature", "policy_coin_scope", "policy_discovery_cutoff",
    "policy_created_at", "policy_stale",
    "telemetry_session_id", "cycle_id",
    "binary_ticker", "coin", "signal_ts_utc", "signal_ts_epoch_ms", "binary_close_time", "minutes_left",
    "fav", "base_p_up", "base_conf", "side_ask", "raw_edge", "net_edge", "rec_stop",
    "analysis_ready", "quality_flags",
    "candidate_feature_value",
    "control_1_name", "control_1_value", "control_2_name", "control_2_value",
    "model_b_p_up", "model_c_p_up", "model_b_p_favored", "model_c_p_favored",
    "conflict_score", "conflict_threshold",
    "shadow_decision", "shadow_reason",
    "first_signal_verified", "expected_training_block_fraction",
    "journal_written_at_utc",
]


def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def compute_policy_hash(policy):
    """SHA-256 of the policy content (everything except policy_hash itself)."""
    return hashlib.sha256(canonical_json({k: v for k, v in policy.items() if k != "policy_hash"}).encode()).hexdigest()


def p_favored(p_up, fav):
    if fav == "UP":
        return p_up
    if fav == "DOWN":
        return 1.0 - p_up
    raise ValueError(f"fav must be UP/DOWN, got {fav!r}")


def _num(v):
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _truthy(v):
    return v is True or str(v).strip().lower() == "true"


def validate_policy(p, allow_synthetic=False):
    """Return a list of problems (empty = valid). Fail closed on anything unexpected."""
    errs = []
    if not isinstance(p, dict):
        return ["policy is not an object"]
    missing = [k for k in REQUIRED_POLICY_KEYS if k not in p]
    if missing:
        return [f"missing keys {missing}"]
    if p["policy_schema_version"] != POLICY_SCHEMA_VERSION:
        errs.append(f"unsupported policy_schema_version {p['policy_schema_version']}")
    if p["mode"] != MODE:
        errs.append(f"mode must be {MODE}")
    bad_keys = [k for k in p if any(w in k.lower() for w in FORBIDDEN_POLICY_KEY_WORDS)]
    if bad_keys:
        errs.append(f"execution-like keys not allowed: {bad_keys}")
    if p["synthetic"] is not False and not allow_synthetic:
        errs.append("synthetic/test policy refused")
    if str(p["telemetry_schema_version"]) != ap.EXPECTED_SCHEMA_VERSION:
        errs.append("telemetry schema mismatch")
    if p["feature_version"] != ap.EXPECTED_FEATURE_VERSION:
        errs.append("feature version mismatch")
    if p["cohort"] != "first_signal":
        errs.append("only first_signal policies can score calls")
    if p["feature_name"] not in ap.PRIMARY_FEATURES or p["feature_name"] in ap.EXPLORATORY_FUNDING:
        errs.append("feature is not a primary Step 3 feature")
    if p["coin_or_group"] not in ap.GROUPS:
        errs.append("unknown coin_or_group")
    thr = _num(p.get("conflict_threshold"))
    if thr is None or thr >= 0:
        errs.append("conflict_threshold must be a finite negative number")
    try:
        for c in p["controls"] + [p["feature_name"]]:
            ap.assert_allowed_predictor(c)
        mapped = ap.spot_controls_for(p["feature_name"])
        ctl = p["controls"]
        if not ctl or len(ctl) > 2 or ctl[0] != mapped[0] or \
                (len(ctl) == 2 and not (p["feature_name"] in ap.PREMIUM_BASIS and ctl[1] == "spot_vol_shock_60v300")):
            errs.append("controls do not follow the Step 3 mapping")
        if bool(p["reliability_interaction"]) != (ap.feature_family(p["feature_name"]) == "reliability"):
            errs.append("interaction flag does not match the feature family")
        nb, nc = len(p["model_b_coefficients"]), len(p["model_c_coefficients"])
        exp_b = 2 + len(p["controls"])
        exp_c = exp_b + (2 if p["reliability_interaction"] else 1)
        if nb != exp_b or nc != exp_c:
            errs.append("coefficient vector lengths do not match the model design")
        if not all(isinstance(b, (int, float)) and math.isfinite(b)
                   for b in p["model_b_coefficients"] + p["model_c_coefficients"]):
            errs.append("non-finite coefficients")
        for key, cols in p["scalers"].items():
            for col, st in cols.items():
                ap.Scaler.from_stats(st)
    except (ValueError, TypeError, KeyError) as e:
        errs.append(f"invalid model content: {e}")
    if compute_policy_hash(p) != p["policy_hash"]:
        errs.append("policy_hash does not match content (edited after freezing?)")
    return errs


def load_policy_registry(path, allow_synthetic=False):
    """Returns (policies, errors, status). A missing file is the normal zero-policy state."""
    if not os.path.exists(path):
        return [], [], "no_policy_file"
    try:
        with open(path) as f:
            reg = json.load(f)
    except (OSError, ValueError) as e:
        return [], [f"unreadable policy file: {e}"], "error"
    if not isinstance(reg, dict) or reg.get("policy_schema_version") != POLICY_SCHEMA_VERSION \
            or not isinstance(reg.get("policies"), list):
        return [], ["unsupported policy registry format"], "error"
    good, errs = [], []
    for p in reg["policies"]:
        e = validate_policy(p, allow_synthetic)
        if e:
            errs.append(f"{p.get('policy_id', '?') if isinstance(p, dict) else '?'}: {'; '.join(e)}")
        else:
            good.append(FrozenPolicy(p, allow_synthetic=allow_synthetic))
    status = "ok" if good and not errs else ("error" if errs else "no_policies")
    return good, errs, status


class FrozenPolicy:
    """Pure, frozen scoring. No refitting, no thresholds changed, no I/O."""

    def __init__(self, policy, allow_synthetic=False):
        e = validate_policy(policy, allow_synthetic)
        if e:
            raise ValueError("; ".join(e))
        self.p = policy
        self.id = policy["policy_id"]
        self.feature = policy["feature_name"]
        self.controls = list(policy["controls"])
        self.reliability = bool(policy["reliability_interaction"])
        self.by_coin = bool(policy["by_coin_scaling"])
        self.threshold = float(policy["conflict_threshold"])
        self.beta_b = [float(b) for b in policy["model_b_coefficients"]]
        self.beta_c = [float(b) for b in policy["model_c_coefficients"]]
        self.scalers = {(key, col): ap.Scaler.from_stats(st)
                        for key, cols in policy["scalers"].items() for col, st in cols.items()}

    def applies_to(self, coin):
        return self.p["coin_or_group"] == "ALL" or self.p["coin_or_group"] == coin

    def is_stale(self, now_ts, max_age_days):
        created = ap._parse_iso(self.p["created_at_utc"])
        return created is not None and max_age_days is not None and now_ts - created > max_age_days * 86400.0

    def score(self, row):
        """row: a causal telemetry row (dict of CSV-column -> value). Returns a dict with
        decision, reason, probabilities and the conflict score."""
        out = {"decision": UNAVAILABLE, "reason": None, "feature_value": None, "controls": [],
               "p_b_up": None, "p_c_up": None, "p_b_fav": None, "p_c_fav": None, "conflict": None}
        coin = row.get("coin")
        if not self.applies_to(coin):
            out.update(decision=NOT_APPLICABLE, reason=R_NA_COIN)
            return out
        fv = _num(row.get(self.feature))
        ctl = [(c, _num(row.get(c))) for c in self.controls]
        out["feature_value"], out["controls"] = fv, ctl
        if not _truthy(row.get("analysis_ready")):
            out["reason"] = R_NOT_READY; return out
        if fv is None:
            out["reason"] = R_FEATURE; return out
        if any(v is None for _, v in ctl):
            out["reason"] = R_CONTROL; return out
        p_pct = _num(row.get("base_p_up"))
        if p_pct is None:
            out["reason"] = R_BASE; return out
        fav = row.get("fav")
        if fav not in ("UP", "DOWN"):
            out["reason"] = R_FAV; return out
        key = coin if self.by_coin else "*"
        if any((key, c) not in self.scalers for c in self.controls + [self.feature]):
            out["reason"] = R_SCALE; return out
        try:
            o = {"coin": coin, "base_logit": ap.logit(p_pct / 100.0),
                 "f": dict(ctl, **{self.feature: fv})}
            xb = ap.model_design(o, "B", self.controls, self.feature, self.reliability, self.scalers, self.by_coin)
            xc = ap.model_design(o, "C", self.controls, self.feature, self.reliability, self.scalers, self.by_coin)
            pb = ap.sigmoid(self.beta_b[0] + sum(b * x for b, x in zip(self.beta_b[1:], xb)))
            pc = ap.sigmoid(self.beta_c[0] + sum(b * x for b, x in zip(self.beta_c[1:], xc)))
            pbf, pcf = p_favored(pb, fav), p_favored(pc, fav)
            conflict = pcf - pbf
            if not all(math.isfinite(v) for v in (pb, pc, conflict)):
                raise ValueError("non-finite")
        except (ValueError, OverflowError, TypeError, ZeroDivisionError):
            out["reason"] = R_NUMERIC; return out
        out.update(p_b_up=pb, p_c_up=pc, p_b_fav=pbf, p_c_fav=pcf, conflict=conflict)
        if conflict <= self.threshold:
            out.update(decision=WOULD_BLOCK, reason=R_BLOCK)
        else:
            out.update(decision=ALLOW, reason=R_ALLOW)
        return out


class ShadowJournal:
    """Append-only CSV with a fixed, versioned header. A file with a different header is
    renamed aside (never mixed, never deleted). (policy_id, ticker) keys already present in
    the journal — including renamed older files — are loaded so a restart never scores a
    ticker twice."""

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.keys = {}
        self._prepare()

    def _prepare(self):
        d = os.path.dirname(os.path.abspath(self.path))
        base = os.path.basename(self.path)
        for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if name == base or (name.startswith(base + ".schema-") and name.endswith(".bak")):
                try:
                    with open(os.path.join(d, name), newline="") as f:
                        for r in csv.DictReader(f):
                            if r.get("policy_id") and r.get("binary_ticker"):
                                self.keys.setdefault((r["policy_id"], r["binary_ticker"]), r.get("shadow_decision"))
                except (OSError, csv.Error):
                    pass
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, newline="") as f:
                hdr = next(csv.reader(f), None)
            if hdr != JOURNAL_COLUMNS:
                os.replace(self.path, f"{self.path}.schema-{int(time.time())}.bak")

    def has(self, policy_id, ticker):
        return (policy_id, ticker) in self.keys

    def append(self, row):
        with self._lock:
            key = (row["policy_id"], row["binary_ticker"])
            if key in self.keys:
                return False
            new = not os.path.exists(self.path) or os.path.getsize(self.path) == 0
            with open(self.path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_COLUMNS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in JOURNAL_COLUMNS})
                f.flush()
                os.fsync(f.fileno())
            self.keys[key] = row.get("shadow_decision")
            return True


class ShadowEvaluator:
    """Consumes telemetry rows AFTER the real call. First binary signal per (policy, ticker)
    only; an unavailable first signal is recorded as UNAVAILABLE and never replaced."""

    def __init__(self, policies, journal, first_signal_lookup=None, max_age_days=30,
                 load_errors=None, load_status="ok", clock=time.time):
        self.policies = list(policies)
        self.journal = journal
        self.lookup = first_signal_lookup
        self.max_age_days = max_age_days
        self.clock = clock
        self.load_errors = list(load_errors or [])
        self.load_status = load_status
        self.counts = {"shadow_rows": 0, ALLOW: 0, WOULD_BLOCK: 0, UNAVAILABLE: 0, NOT_APPLICABLE: 0}
        self._na_seen = set()
        self.last_decision = None
        self.last_error = None

    @classmethod
    def from_files(cls, policy_path, journal_path, first_signal_lookup=None, max_age_days=30,
                   allow_synthetic=False):
        pols, errs, status = load_policy_registry(policy_path, allow_synthetic=allow_synthetic)
        return cls(pols, ShadowJournal(journal_path), first_signal_lookup, max_age_days, errs, status)

    def process_rows(self, rows):
        for coin in sorted(rows or {}):
            try:
                self._process_row(dict(rows[coin]))
            except Exception as e:                             # fail open, never raise
                self.last_error = f"{type(e).__name__}: {e}"[:200]

    def _process_row(self, row):
        if not _truthy(row.get("binary_signal")):
            return
        tk = (row.get("binary_ticker") or "").strip()
        if not tk:
            return
        for pol in self.policies:
            if self.journal.has(pol.id, tk):
                continue                                       # first signal already recorded
            if not pol.applies_to(row.get("coin")):
                if (pol.id, tk) not in self._na_seen:
                    self._na_seen.add((pol.id, tk)); self.counts[NOT_APPLICABLE] += 1
                continue
            expected = self.lookup(tk) if self.lookup else None
            row_ms = _num(row.get("spot_observed_ts_epoch_ms"))
            verified = None
            if expected and expected.get("signal_ts_epoch_ms") is not None:
                verified = row_ms is not None and abs(row_ms - expected["signal_ts_epoch_ms"]) <= 1.0
            if verified is False:
                res = {"decision": UNAVAILABLE, "reason": R_MISSED, "feature_value": None, "controls": [],
                       "p_b_up": None, "p_c_up": None, "p_b_fav": None, "p_c_fav": None, "conflict": None}
            else:
                res = pol.score(row)
            if res["decision"] == NOT_APPLICABLE:
                continue
            self._write(pol, row, res, verified, (expected or {}).get("rec_stop"))

    def _write(self, pol, row, res, verified, rec_stop):
        p = pol.p
        now = self.clock()
        ctl = res["controls"] + [(None, None)] * 2
        ms = _num(row.get("spot_observed_ts_epoch_ms"))
        rec = {
            "shadow_schema_version": SHADOW_SCHEMA_VERSION,
            "policy_id": pol.id, "policy_hash": p["policy_hash"], "policy_feature": pol.feature,
            "policy_coin_scope": p["coin_or_group"], "policy_discovery_cutoff": p["discovery_cutoff_utc"],
            "policy_created_at": p["created_at_utc"], "policy_stale": pol.is_stale(now, self.max_age_days),
            "telemetry_session_id": row.get("telemetry_session_id"), "cycle_id": row.get("cycle_id"),
            "binary_ticker": row.get("binary_ticker"), "coin": row.get("coin"),
            "signal_ts_utc": dt.datetime.fromtimestamp(ms / 1000.0, dt.timezone.utc).isoformat() if ms else None,
            "signal_ts_epoch_ms": int(ms) if ms is not None else None,
            "binary_close_time": row.get("binary_close_time"), "minutes_left": row.get("minutes_left"),
            "fav": row.get("fav"), "base_p_up": row.get("base_p_up"), "base_conf": row.get("base_conf"),
            "side_ask": row.get("side_ask"), "raw_edge": row.get("raw_edge"), "net_edge": row.get("net_edge"),
            "rec_stop": rec_stop, "analysis_ready": _truthy(row.get("analysis_ready")),
            "quality_flags": row.get("quality_flags"),
            "candidate_feature_value": res["feature_value"],
            "control_1_name": ctl[0][0], "control_1_value": ctl[0][1],
            "control_2_name": ctl[1][0], "control_2_value": ctl[1][1],
            "model_b_p_up": res["p_b_up"], "model_c_p_up": res["p_c_up"],
            "model_b_p_favored": res["p_b_fav"], "model_c_p_favored": res["p_c_fav"],
            "conflict_score": res["conflict"], "conflict_threshold": pol.threshold,
            "shadow_decision": res["decision"], "shadow_reason": res["reason"],
            "first_signal_verified": verified,
            "expected_training_block_fraction": p.get("expected_training_block_fraction"),
            "journal_written_at_utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        }
        if self.journal.append(rec):
            self.counts["shadow_rows"] += 1
            self.counts[res["decision"]] += 1
            self.last_decision = {"policy_id": pol.id, "ticker": rec["binary_ticker"],
                                  "decision": res["decision"], "reason": res["reason"]}

    def health(self):
        status = self.load_status
        if self.load_errors:
            status = "error"
        return {"status": status, "mode": MODE, "policies_loaded": len(self.policies) + len(self.load_errors),
                "valid_policies": len(self.policies), "invalid_policies": len(self.load_errors),
                "policy_errors": self.load_errors[:5], "shadow_rows": self.counts["shadow_rows"],
                "would_block": self.counts[WOULD_BLOCK], "allow": self.counts[ALLOW],
                "unavailable": self.counts[UNAVAILABLE], "not_applicable": self.counts[NOT_APPLICABLE],
                "last_shadow_decision": self.last_decision, "last_error": self.last_error}
