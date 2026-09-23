#!/usr/bin/env python3
"""
perp_live.py — STEP 6 production LIVE VETO gate. VETO ONLY.

The single production action this module can cause is: SUPPRESS an already-valid legacy call
when a manually promoted, prospectively validated perp policy returns BLOCK. It can never
create a call, reverse a direction, change sizing, stops, entry mode, pricing or timing, and
it contains no order/transfer/leverage code and no network access of any kind.

Activation needs BOTH a valid promotion artifact AND PERP_LIVE_VETO_ENABLED=1. No environment
variable can bypass artifact validation. Integrity failures (bad hash, expiry, synthetic,
policy mismatch, strategy drift) refuse ACTIVATION; per-call data failures FAIL OPEN, i.e. the
legacy call proceeds untouched.

Layer 1  Step 4 frozen filter:  conflict = P_C_FAV - P_B_FAV <= frozen threshold -> BLOCK
Layer 2  Step 5 frozen overlay: p = clip(legacy_p_up + alpha*clip(P_C_UP-P_B_UP, +-0.05))
         then the LEGACY conf/edge formulas decide block-or-allow on the LEGACY side only.
"""
import csv
import datetime as dt
import hashlib
import json
import math
import os
import threading
import time
from collections import deque

import perp_probability as pp
import perp_shadow as ps
import strategy_fingerprint as sf

PROMOTION_SCHEMA_VERSION = 1
LIVE_VETO_SCHEMA_VERSION = 1
MODE = "LIVE_VETO_ONLY"
CONFIRM_PHRASE = "LIVE_VETO_ONLY"
DEFAULT_EXPIRY_DAYS = 14
MAX_EXPIRY_DAYS = 30
DEFAULT_KILL_FILE = "DISABLE_PERP_LIVE_VETO"

MAX_CONSECUTIVE_GATE_ERRORS = 3
LIVE_VETO_ROLLING_WINDOW = 50
LIVE_VETO_MIN_WINDOW = 30
LIVE_VETO_MAX_BLOCK_FRACTION = 0.35
LIVE_VETO_MAX_FAIL_OPEN_FRACTION = 0.50
MAX_GATE_LATENCY_MS = 25.0

ALLOW, BLOCK, INACTIVE, FAIL_OPEN = "ALLOW", "BLOCK", "INACTIVE", "FAIL_OPEN"
R_ALLOW = "ALLOW_PERP_GATE_PASSED"
R_NOT_APPLICABLE = "ALLOW_NOT_APPLICABLE_COIN"
R_NO_LEGACY = "NO_LEGACY_SIGNAL"
R_INACTIVE = "GATE_INACTIVE"
B_STEP4 = "BLOCK_STEP4_CONFLICT"
B_FLIP = "BLOCK_DIRECTION_FLIP"
B_CONF = "BLOCK_LOW_INTEGRATED_CONFIDENCE"
B_EDGE = "BLOCK_NO_INTEGRATED_EDGE"
FO_NOT_READY = "FAIL_OPEN_ANALYSIS_NOT_READY"
FO_FEATURE = "FAIL_OPEN_FEATURE_MISSING"
FO_CONTROL = "FAIL_OPEN_CONTROL_MISSING"
FO_STALE = "FAIL_OPEN_STALE_DATA"
FO_NUMERIC = "FAIL_OPEN_NUMERIC_ERROR"
FO_TIMEOUT = "FAIL_OPEN_GATE_TIMEOUT"
FO_INTERNAL = "FAIL_OPEN_GATE_INTERNAL_ERROR"
FO_PREVIEW = "FAIL_OPEN_NO_PREVIEW_ROW"
BLOCK_REASONS = (B_STEP4, B_FLIP, B_CONF, B_EDGE)
_SHADOW_FO = {ps.R_NOT_READY: FO_NOT_READY, ps.R_FEATURE: FO_FEATURE, ps.R_CONTROL: FO_CONTROL,
              ps.R_BASE: FO_FEATURE, ps.R_FAV: FO_FEATURE, ps.R_SCALE: FO_STALE, ps.R_NUMERIC: FO_NUMERIC}

JOURNAL_COLUMNS = [
    "live_veto_schema_version", "promotion_id", "promotion_hash", "experiment_id", "policy_id", "policy_hash",
    "feature_name", "coin_scope", "telemetry_session_id", "cycle_id", "binary_ticker", "coin",
    "signal_ts_utc", "signal_ts_epoch_ms", "binary_close_time", "minutes_left", "legacy_signal", "fav",
    "side_ask", "legacy_p_up", "legacy_conf", "legacy_raw_edge", "legacy_net_edge", "rec_stop", "contracts",
    "analysis_ready", "quality_flags", "candidate_feature_value", "model_b_p_up", "model_c_p_up",
    "model_b_p_favored", "model_c_p_favored", "conflict_score", "conflict_threshold", "step4_decision",
    "delta_raw", "delta_capped", "selected_alpha", "integrated_p_up", "integrated_p_favored",
    "integrated_conf", "integrated_raw_edge", "integrated_net_edge", "live_decision", "live_reason",
    "fail_open", "gate_latency_ms", "promotion_created_at", "promotion_expires_at", "journal_written_at_utc"]

REQUIRED_PROMOTION_KEYS = ("promotion_schema_version", "promotion_id", "promotion_hash", "mode", "experiment_id",
                           "candidate_sha256", "source_policy_id", "source_policy_hash", "feature_name",
                           "coin_or_group", "selected_alpha", "max_abs_perp_delta", "conflict_threshold",
                           "step5_baseline_dashboard_sha256", "legacy_strategy_fingerprint", "strategy_config_hash",
                           "fingerprint_format_version",
                           "created_at_utc", "expires_at_utc", "synthetic", "scope", "source_hashes")
REQUIRED_SCOPE = {"veto_only": True, "new_calls": False, "direction_reversal": False, "sizing_changes": False,
                  "stop_changes": False, "execution_changes": False}


def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def compute_promotion_hash(p):
    return hashlib.sha256(canonical_json({k: v for k, v in p.items() if k != "promotion_hash"}).encode()).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _num(v):
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def validate_promotion(p, now, allow_synthetic=False):
    """Activation integrity (fail closed). Returns a list of problems; empty = usable."""
    if not isinstance(p, dict):
        return ["promotion is not an object"]
    missing = [k for k in REQUIRED_PROMOTION_KEYS if k not in p]
    if missing:
        return [f"missing keys {missing}"]
    errs = []
    if p["promotion_schema_version"] != PROMOTION_SCHEMA_VERSION:
        errs.append("unsupported promotion schema")
    if p["mode"] != MODE:
        errs.append(f"mode must be {MODE}")
    if compute_promotion_hash(p) != p["promotion_hash"]:
        errs.append("promotion_hash does not match content (edited after promotion?)")
    if p["synthetic"] is not False and not allow_synthetic:
        errs.append("synthetic promotion refused")
    if dict(p.get("scope") or {}) != REQUIRED_SCOPE:
        errs.append("scope must be veto-only with every other capability false")
    exp = sf and p.get("expires_at_utc")
    t_exp = _parse_iso(p.get("expires_at_utc"))
    t_new = _parse_iso(p.get("created_at_utc"))
    if t_exp is None or t_new is None:
        errs.append("missing/invalid created_at/expires_at")
    else:
        if t_exp <= now:
            errs.append("promotion expired")
        if t_exp - t_new > MAX_EXPIRY_DAYS * 86400.0 + 1:
            errs.append(f"expiry exceeds {MAX_EXPIRY_DAYS} days")
    if p["fingerprint_format_version"] != sf.FINGERPRINT_FORMAT_VERSION:
        errs.append(f"{sf.UNSUPPORTED_FORMAT}: promotion uses fingerprint format "
                    f"{p['fingerprint_format_version']!r}, this code requires {sf.FINGERPRINT_FORMAT_VERSION}")
    if p["selected_alpha"] not in pp.ALPHA_GRID:
        errs.append("selected alpha is not in the frozen grid")
    if p["max_abs_perp_delta"] != pp.MAX_ABS_PERP_DELTA:
        errs.append("delta cap differs from the frozen overlay definition")
    if _num(p["conflict_threshold"]) is None or _num(p["conflict_threshold"]) >= 0:
        errs.append("conflict threshold must be negative")
    for k in p:
        if any(w in k.lower() for w in ("enable", "force", "bypass", "autotrade", "activate")):
            errs.append(f"forbidden key in promotion: {k}")
    return errs


def _parse_iso(s):
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


class LiveVetoJournal:
    """Append-only audit CSV. Fixed versioned header; a different header is rotated aside.
    (promotion_id, ticker) keys are reloaded at startup so a restart never re-treats a ticker
    as a new first signal."""

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.handled = {}
        self._prepare()

    def _prepare(self):
        d = os.path.dirname(os.path.abspath(self.path)) or "."
        base = os.path.basename(self.path)
        for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if name == base or (name.startswith(base + ".schema-") and name.endswith(".bak")):
                try:
                    with open(os.path.join(d, name), newline="") as f:
                        for r in csv.DictReader(f):
                            if r.get("promotion_id") and r.get("binary_ticker"):
                                self.handled.setdefault((r["promotion_id"], r["binary_ticker"]),
                                                        (r.get("live_decision"), r.get("live_reason")))
                except (OSError, csv.Error):
                    pass
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, newline="") as f:
                hdr = next(csv.reader(f), None)
            if hdr != JOURNAL_COLUMNS:
                os.replace(self.path, f"{self.path}.schema-{int(time.time())}.bak")

    def seen(self, promotion_id, ticker):
        return self.handled.get((promotion_id, ticker))

    def append(self, row):
        with self._lock:
            key = (row["promotion_id"], row["binary_ticker"])
            if key in self.handled:
                return False
            new = not os.path.exists(self.path) or os.path.getsize(self.path) == 0
            with open(self.path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_COLUMNS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in JOURNAL_COLUMNS})
                f.flush()
                os.fsync(f.fileno())
            self.handled[key] = (row.get("live_decision"), row.get("live_reason"))
            return True


class LiveVetoGate:
    """Frozen parameters only: no fitting, no threshold/alpha tuning, no reloading."""

    def __init__(self, promotion=None, policy=None, journal=None, env_enabled=False, status="INACTIVE",
                 problems=None, kill_file=DEFAULT_KILL_FILE, clock=time.time, baseline=None,
                 runtime_config=None, scorer=None):
        self.promotion = promotion
        self.policy = policy
        self.journal = journal
        self.env_enabled = bool(env_enabled)
        self.problems = list(problems or [])
        self.kill_file = kill_file
        self.clock = clock
        self.baseline = baseline or {}
        self.runtime_config = dict(runtime_config or {})
        self.scorer = scorer                     # injectable for tests only; default = frozen policy
        self.configured = promotion is not None
        self.status = status
        self.kill_latched = False
        self.breaker_latched = False
        self.breaker_reason = None
        self.consecutive_errors = 0
        self.counts = {"legacy_signals_seen": 0, ALLOW: 0, BLOCK: 0, FAIL_OPEN: 0, INACTIVE: 0}
        self.window = deque(maxlen=LIVE_VETO_ROLLING_WINDOW)
        self.last = None
        self.last_error = None
        self._lock = threading.Lock()

    # ---------- construction ----------
    @classmethod
    def from_files(cls, promotion_path, policies_path, baseline_path, dashboard_path, journal_path,
                   env_enabled=False, kill_file=DEFAULT_KILL_FILE, clock=time.time, allow_synthetic=False,
                   runtime_config=None):
        """Activation integrity is checked ONCE here. Any failure => the gate never activates and
        the bot keeps its legacy behaviour."""
        now = clock()
        problems, promotion, policy, baseline = [], None, None, None
        if not os.path.exists(promotion_path):
            return cls(None, None, None, env_enabled, "INACTIVE_NO_PROMOTION", [], kill_file, clock)
        try:
            with open(promotion_path) as f:
                promotion = json.load(f)
        except (OSError, ValueError) as e:
            return cls(None, None, None, env_enabled, "REFUSED", [f"unreadable promotion: {e}"], kill_file, clock)
        problems += validate_promotion(promotion, now, allow_synthetic)
        try:
            with open(baseline_path) as f:
                baseline = json.load(f)
        except (OSError, ValueError) as e:
            problems.append(f"unreadable Step 5 baseline manifest: {e}")
        if baseline and baseline.get("fingerprint_format_version") != sf.FINGERPRINT_FORMAT_VERSION:
            problems.append(f"{sf.UNSUPPORTED_FORMAT}: baseline manifest format "
                            f"{baseline.get('fingerprint_format_version', 1)!r}")
        if baseline and not problems:
            if promotion.get("step5_baseline_dashboard_sha256") != baseline.get("step5_dashboard_sha256"):
                problems.append("promotion was built against a different Step 5 dashboard baseline")
            if promotion.get("legacy_strategy_fingerprint") != baseline.get("legacy_strategy_fingerprint"):
                problems.append("promotion strategy fingerprint differs from the baseline manifest")
            ok, why = sf.verify(dashboard_path, baseline)
            if not ok:
                problems += [w if w.startswith(sf.UNSUPPORTED_FORMAT) else f"strategy drift: {w}" for w in why]
        if not problems:
            try:
                with open(policies_path) as f:
                    reg = json.load(f)
                pol = next((p for p in reg.get("policies", []) if p.get("policy_id") == promotion["source_policy_id"]), None)
                if pol is None:
                    problems.append("frozen Step 4 policy not found")
                elif pol.get("policy_hash") != promotion["source_policy_hash"]:
                    problems.append("frozen Step 4 policy hash differs from the promotion")
                elif abs(float(pol["conflict_threshold"]) - float(promotion["conflict_threshold"])) > 1e-12:
                    problems.append("policy threshold differs from the promotion")
                else:
                    policy = ps.FrozenPolicy(pol, allow_synthetic=allow_synthetic)
            except (OSError, ValueError, KeyError) as e:
                problems.append(f"policy registry unusable: {e}")
        rc = dict(runtime_config or {})
        if not problems and baseline:
            for k in baseline.get("runtime_mutable_constants", []):
                if k in rc and rc[k] != baseline.get("strategy_config_values", {}).get(k):
                    problems.append(f"runtime strategy setting {k} differs from the validated baseline")
        if problems:
            return cls(promotion, None, None, env_enabled, "REFUSED", problems, kill_file, clock, baseline, rc)
        journal = LiveVetoJournal(journal_path)
        status = "ACTIVE" if env_enabled else "READY_BUT_DISABLED"
        return cls(promotion, policy, journal, env_enabled, status, [], kill_file, clock, baseline, rc)

    # ---------- runtime ----------
    @property
    def active(self):
        return (self.status == "ACTIVE" and self.env_enabled and self.policy is not None
                and not self.kill_latched and not self.breaker_latched)

    def _check_kill(self):
        try:
            if self.kill_file and os.path.exists(self.kill_file):
                if not self.kill_latched:
                    self.kill_latched = True         # latched for the process: deleting it does not re-enable
                    print("  PERP LIVE VETO: kill file present — veto latched OFF for this process")
        except OSError:
            pass
        return self.kill_latched

    def _inactive(self, reason, legacy_signal=True):
        return {"active": False, "legacy_signal": legacy_signal, "decision": INACTIVE, "reason": reason,
                "fail_open": False, "policy_id": None, "promotion_id": (self.promotion or {}).get("promotion_id"),
                "latency_ms": 0.0}

    def _record(self, decision):
        self.counts[decision] = self.counts.get(decision, 0) + 1
        if decision in (ALLOW, BLOCK, FAIL_OPEN):
            self.window.append(decision)
        n = len(self.window)
        if n >= LIVE_VETO_MIN_WINDOW and not self.breaker_latched:
            blocks = sum(1 for d in self.window if d == BLOCK) / n
            fos = sum(1 for d in self.window if d == FAIL_OPEN) / n
            if blocks > LIVE_VETO_MAX_BLOCK_FRACTION:
                self.breaker_latched, self.breaker_reason = True, "CIRCUIT_BREAKER_BLOCK_RATE"
            elif fos > LIVE_VETO_MAX_FAIL_OPEN_FRACTION:
                self.breaker_latched, self.breaker_reason = True, "CIRCUIT_BREAKER_FAIL_OPEN_RATE"
            if self.breaker_latched:
                print(f"  PERP LIVE VETO: {self.breaker_reason} — veto latched OFF; legacy calls continue")

    def evaluate(self, coin, r, preview_row):
        """The only production entry point. Returns a result dict; never raises."""
        t0 = time.perf_counter()
        legacy_signal = bool((r or {}).get("signal"))
        try:
            if not legacy_signal:
                return self._inactive(R_NO_LEGACY, legacy_signal=False)     # never manufactures a call
            with self._lock:
                self.counts["legacy_signals_seen"] += 1
                if self._check_kill() or not self.active:
                    res = self._inactive(R_INACTIVE)
                    self.counts[INACTIVE] += 1
                    self.last = {"decision": INACTIVE, "reason": R_INACTIVE}
                    return res
                ticker = (r.get("ticker") or "").strip()
                pid = self.promotion["promotion_id"]
                prior = self.journal.seen(pid, ticker) if ticker else None
                if prior is not None:                                        # first-signal semantics survive restart
                    dec = BLOCK if prior[0] == BLOCK else ALLOW
                    out = self._inactive(f"REPLAY_{prior[0]}")
                    out.update(decision=dec, active=True, reason=f"REPLAY_{prior[0]}",
                               fail_open=prior[0] == FAIL_OPEN, policy_id=self.policy.id, promotion_id=pid)
                    return out
                res = self._score(coin, r, preview_row, t0)
                if res["decision"] in (BLOCK, ALLOW) and (res.get("latency_ms") or 0.0) > MAX_GATE_LATENCY_MS:
                    self.consecutive_errors += 1          # local compute only: over budget => fail open
                    res.update(decision=FAIL_OPEN, reason=FO_TIMEOUT, fail_open=True)
                if res["decision"] in (BLOCK, ALLOW, FAIL_OPEN) and ticker and res["reason"] != R_NOT_APPLICABLE:
                    self._write(coin, r, preview_row, res)
                    self._record(res["decision"])
                self.last = {"decision": res["decision"], "reason": res["reason"],
                             "ticker": ticker, "latency_ms": res["latency_ms"]}
                return res
        except Exception as e:                                               # never break the call path
            self.last_error = f"{type(e).__name__}: {e}"[:200]
            self.consecutive_errors += 1
            if self.consecutive_errors >= MAX_CONSECUTIVE_GATE_ERRORS and not self.breaker_latched:
                self.breaker_latched, self.breaker_reason = True, "CIRCUIT_BREAKER_GATE_ERRORS"
                print("  PERP LIVE VETO: repeated gate errors — veto latched OFF; legacy calls continue")
            out = self._inactive(FO_INTERNAL)
            out.update(decision=FAIL_OPEN, fail_open=True, reason=FO_INTERNAL,
                       latency_ms=(time.perf_counter() - t0) * 1000.0)
            return out

    def _score(self, coin, r, preview_row, t0):
        p = self.promotion
        k = self.baseline.get("strategy_config_values", {})
        consts = {"MIN_CONF": k.get("MIN_CONF"), "MIN_PRICE": k.get("MIN_PRICE"),
                  "EDGE_THRESH": k.get("EDGE_THRESH"), "ENTRY_COST_CENTS": k.get("ENTRY_COST_CENTS")}
        out = {"active": True, "legacy_signal": True, "decision": FAIL_OPEN, "reason": FO_INTERNAL,
               "fail_open": True, "policy_id": self.policy.id, "promotion_id": p["promotion_id"],
               "p_b_up": None, "p_c_up": None, "conflict_score": None,
               "conflict_threshold": self.policy.threshold, "delta_raw": None, "delta_capped": None,
               "alpha": p["selected_alpha"], "legacy_p_up": _num(r.get("p_up")),
               "integrated_p_up": None, "legacy_conf": _num(r.get("conf")), "integrated_conf": None,
               "legacy_raw_edge": _num(r.get("raw_edge")), "integrated_raw_edge": None,
               "legacy_net_edge": _num(r.get("net_edge")), "integrated_net_edge": None,
               "step4_decision": None, "latency_ms": 0.0}
        done = lambda: out.update(latency_ms=(time.perf_counter() - t0) * 1000.0) or out
        if not preview_row:
            out.update(reason=FO_PREVIEW)
            return done()
        scorer = self.scorer or self.policy.score
        s = scorer(preview_row)
        out["step4_decision"] = s["decision"]
        out.update(p_b_up=s.get("p_b_up"), p_c_up=s.get("p_c_up"), conflict_score=s.get("conflict"))
        if s["decision"] == ps.NOT_APPLICABLE:
            out.update(decision=ALLOW, reason=R_NOT_APPLICABLE, fail_open=False)
            return done()
        if s["decision"] == ps.UNAVAILABLE:
            out.update(reason=_SHADOW_FO.get(s.get("reason"), FO_NUMERIC))
            return done()
        if s["decision"] == ps.WOULD_BLOCK:                                  # Layer 1: frozen Step 4 filter
            out.update(decision=BLOCK, reason=B_STEP4, fail_open=False)
            return done()
        try:                                                                  # Layer 2: frozen Step 5 overlay
            legacy_p = _num(r.get("p_up"))
            fav, ask = r.get("fav"), _num(r.get("side_ask"))
            if legacy_p is None or ask is None or fav not in ("UP", "DOWN") or None in consts.values():
                out.update(reason=FO_FEATURE)
                return done()
            o = pp.overlay(legacy_p / 100.0, s["p_b_up"], s["p_c_up"], p["selected_alpha"], fav, ask,
                           consts, legacy_signal=True)
        except (ValueError, OverflowError, ZeroDivisionError, TypeError):
            out.update(reason=FO_NUMERIC)
            return done()
        out.update(delta_raw=o["delta_raw"], delta_capped=o["delta_capped"], integrated_p_up=o["integrated_p_up"],
                   integrated_p_favored=o["integrated_p_favored"], integrated_conf=o["integrated_conf"],
                   integrated_raw_edge=o["integrated_raw_edge"], integrated_net_edge=o["integrated_net_edge"])
        mapped = {pp.BLOCK_FLIP: B_FLIP, pp.BLOCK_LOW_CONF: B_CONF, pp.BLOCK_NO_EDGE: B_EDGE}
        if o["decision"] in mapped:
            out.update(decision=BLOCK, reason=mapped[o["decision"]], fail_open=False)
        elif o["decision"] == pp.WOULD_ALLOW:
            out.update(decision=ALLOW, reason=R_ALLOW, fail_open=False)
        else:
            out.update(reason=FO_INTERNAL)
        done()
        if out["decision"] in (BLOCK, ALLOW):
            self.consecutive_errors = 0
        return out

    def _write(self, coin, r, prev, res):
        p, now = self.promotion, self.clock()
        ms = _num((prev or {}).get("spot_observed_ts_epoch_ms")) or (_num(r.get("spot_observed_ts")) or 0) * 1000
        row = {"live_veto_schema_version": LIVE_VETO_SCHEMA_VERSION, "promotion_id": p["promotion_id"],
               "promotion_hash": p["promotion_hash"], "experiment_id": p["experiment_id"],
               "policy_id": self.policy.id, "policy_hash": p["source_policy_hash"],
               "feature_name": p["feature_name"], "coin_scope": p["coin_or_group"],
               "telemetry_session_id": (prev or {}).get("telemetry_session_id"),
               "cycle_id": (prev or {}).get("cycle_id"), "binary_ticker": r.get("ticker"), "coin": coin,
               "signal_ts_utc": dt.datetime.fromtimestamp(ms / 1000.0, dt.timezone.utc).isoformat() if ms else None,
               "signal_ts_epoch_ms": int(ms) if ms else None, "binary_close_time": r.get("close"),
               "minutes_left": r.get("remain"), "legacy_signal": True, "fav": r.get("fav"),
               "side_ask": r.get("side_ask"), "legacy_p_up": r.get("p_up"), "legacy_conf": r.get("conf"),
               "legacy_raw_edge": r.get("raw_edge"), "legacy_net_edge": r.get("net_edge"),
               "rec_stop": r.get("rec_stop"), "contracts": (prev or {}).get("legacy_contracts"),
               "analysis_ready": (prev or {}).get("analysis_ready"),
               "quality_flags": (prev or {}).get("quality_flags"),
               "candidate_feature_value": (prev or {}).get(p["feature_name"]),
               "model_b_p_up": res.get("p_b_up"), "model_c_p_up": res.get("p_c_up"),
               "model_b_p_favored": _fav(res.get("p_b_up"), r.get("fav")),
               "model_c_p_favored": _fav(res.get("p_c_up"), r.get("fav")),
               "conflict_score": res.get("conflict_score"), "conflict_threshold": res.get("conflict_threshold"),
               "step4_decision": res.get("step4_decision"), "delta_raw": res.get("delta_raw"),
               "delta_capped": res.get("delta_capped"), "selected_alpha": res.get("alpha"),
               "integrated_p_up": res.get("integrated_p_up"), "integrated_p_favored": res.get("integrated_p_favored"),
               "integrated_conf": res.get("integrated_conf"), "integrated_raw_edge": res.get("integrated_raw_edge"),
               "integrated_net_edge": res.get("integrated_net_edge"), "live_decision": res["decision"],
               "live_reason": res["reason"], "fail_open": res["fail_open"],
               "gate_latency_ms": round(res.get("latency_ms") or 0.0, 3),
               "promotion_created_at": p["created_at_utc"], "promotion_expires_at": p["expires_at_utc"],
               "journal_written_at_utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat()}
        try:
            self.journal.append(row)
        except OSError as e:
            self.last_error = f"journal: {e}"

    def health(self):
        n = len(self.window)
        p = self.promotion or {}
        return {"configured": self.configured, "active": self.active, "status": self.status,
                "mode": MODE, "problems": self.problems[:5],
                "promotion_id": p.get("promotion_id"), "policy_id": getattr(self.policy, "id", None),
                "experiment_id": p.get("experiment_id"), "expires_at": p.get("expires_at_utc"),
                "legacy_signals_seen": self.counts["legacy_signals_seen"], "allowed": self.counts[ALLOW],
                "blocked": self.counts[BLOCK], "fail_open": self.counts[FAIL_OPEN],
                "inactive": self.counts[INACTIVE],
                "rolling_block_fraction": (sum(1 for d in self.window if d == BLOCK) / n) if n else None,
                "rolling_fail_open_fraction": (sum(1 for d in self.window if d == FAIL_OPEN) / n) if n else None,
                "circuit_breaker_latched": self.breaker_latched, "circuit_breaker_reason": self.breaker_reason,
                "kill_switch_latched": self.kill_latched, "consecutive_errors": self.consecutive_errors,
                "last_decision": (self.last or {}).get("decision"), "last_reason": (self.last or {}).get("reason"),
                "last_latency_ms": (self.last or {}).get("latency_ms"), "last_error": self.last_error}

    def banner(self):
        if not self.configured:
            return "PERP LIVE VETO: OFF — legacy call behavior"
        if self.status == "REFUSED":
            return f"PERP LIVE VETO: REFUSED — {'; '.join(self.problems[:2])}; legacy behavior continues"
        if not self.env_enabled:
            return "PERP LIVE VETO: PROMOTION PRESENT, ACTIVATION DISABLED (set PERP_LIVE_VETO_ENABLED=1)"
        if self.kill_latched or self.breaker_latched:
            return "PERP LIVE VETO: LATCHED OFF — legacy call behavior"
        return (f"PERP LIVE VETO: ACTIVE — VETO ONLY (policy {getattr(self.policy, 'id', '?')}, "
                f"alpha {self.promotion['selected_alpha']}, expires {self.promotion['expires_at_utc']})")


def _fav(p_up, fav):
    if p_up is None or fav not in ("UP", "DOWN"):
        return None
    return p_up if fav == "UP" else 1.0 - p_up
