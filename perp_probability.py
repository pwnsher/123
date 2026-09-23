#!/usr/bin/env python3
"""
perp_probability.py — STEP 5 pure, deterministic probability-overlay math.

No I/O, no network, no trading, no application state. Used only by the OFFLINE Step 5
tools to ask: if the validated perp policy's INCREMENTAL probability shift were added to
the existing binary probability, would the existing call have survived the existing gates?

    delta_raw      = P_C_UP - P_B_UP                        (perp-only increment, Step 3/4 models)
    delta_capped   = clip(delta_raw, -0.05, +0.05)          (FIXED safety cap, never tuned)
    p_integrated   = clip(p_base + alpha * delta_capped, 1e-6, 1 - 1e-6)
    p_int_favored  = p_integrated if fav == UP else 1 - p_integrated    (legacy side is FIXED)
    integrated_conf     = 100 * p_int_favored
    integrated_raw_edge = integrated_conf - side_ask                    (legacy formula)
    integrated_net_edge = integrated_raw_edge - ENTRY_COST_CENTS        (legacy formula)

The overlay can only KEEP or REMOVE an existing first signal. It can never create a call,
reverse a call's direction, change size, stop, entry price or maker/taker behaviour.
MODEL C never replaces the base probability: only P_C - P_B is added.
"""
import math

OVERLAY_VERSION = "step5_overlay_v1"
MAX_ABS_PERP_DELTA = 0.05                       # fixed +-5 pp safety cap (not a parameter)
ALPHA_GRID = (0.25, 0.50, 0.75, 1.00)           # predeclared; nothing else is ever evaluated
LEGACY_ALPHA = 0.0                              # reference only; never "selected"
P_EPS = 1e-6
REQUIRED_CONSTANTS = ("MIN_CONF", "MIN_PRICE", "EDGE_THRESH", "ENTRY_COST_CENTS")

WOULD_ALLOW = "WOULD_ALLOW"
BLOCK_FLIP = "WOULD_BLOCK_DIRECTION_FLIP"
BLOCK_LOW_CONF = "WOULD_BLOCK_LOW_CONFIDENCE"
BLOCK_NO_EDGE = "WOULD_BLOCK_NO_EDGE"
NO_NEW_CALL = "NO_NEW_CALL"
BLOCK_DECISIONS = (BLOCK_FLIP, BLOCK_LOW_CONF, BLOCK_NO_EDGE)


def _finite(x, name):
    if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x):
        raise ValueError(f"{name} must be a finite number")
    return float(x)


def clamp_p(p):
    return min(max(_finite(p, "probability"), P_EPS), 1.0 - P_EPS)


def perp_delta(p_c_up, p_b_up):
    """The ONLY perp contribution Step 5 considers: MODEL C minus MODEL B."""
    pc, pb = _finite(p_c_up, "P_C_UP"), _finite(p_b_up, "P_B_UP")
    if not (0.0 <= pc <= 1.0 and 0.0 <= pb <= 1.0):
        raise ValueError("model probabilities must lie in [0, 1]")
    return pc - pb


def cap_delta(delta):
    d = _finite(delta, "delta")
    return max(-MAX_ABS_PERP_DELTA, min(MAX_ABS_PERP_DELTA, d))


def check_alpha(alpha):
    a = _finite(alpha, "alpha")
    if a != LEGACY_ALPHA and a not in ALPHA_GRID:
        raise ValueError(f"alpha {alpha} is not in the predeclared grid {ALPHA_GRID}")
    return a


def blend(p_base_up, delta_capped, alpha):
    a = check_alpha(alpha)
    d = _finite(delta_capped, "delta_capped")
    if abs(d) > MAX_ABS_PERP_DELTA + 1e-15:
        raise ValueError("delta must be capped before blending")
    return clamp_p(clamp_p(p_base_up) + a * d)


def favored(p_up, fav):
    p = _finite(p_up, "p_up")
    if fav == "UP":
        return p
    if fav == "DOWN":
        return 1.0 - p
    raise ValueError(f"fav must be UP or DOWN, got {fav!r}")


def direction_flips(p_up, fav):
    """True if the adjusted probability would favour the OTHER side. Mirrors the legacy
    rule fav_up = (p_up_percent >= 50.0)."""
    fav_up = _finite(p_up, "p_up") * 100.0 >= 50.0
    if fav not in ("UP", "DOWN"):
        raise ValueError(f"fav must be UP or DOWN, got {fav!r}")
    return (fav == "UP") != fav_up


def integrated_conf(p_favored):
    """Confidence of the LEGACY side (never max(p, 1-p) after a flip)."""
    return 100.0 * _finite(p_favored, "p_favored")


def edges(conf, side_ask, entry_cost_cents):
    """Exact legacy formula: raw = conf - side_ask; net = raw - ENTRY_COST_CENTS."""
    raw = _finite(conf, "conf") - _finite(side_ask, "side_ask")
    return raw, raw - _finite(entry_cost_cents, "ENTRY_COST_CENTS")


def check_constants(constants):
    missing = [k for k in REQUIRED_CONSTANTS if k not in (constants or {})]
    if missing:
        raise ValueError(f"missing strategy constants {missing}")
    return {k: _finite(constants[k], k) for k in REQUIRED_CONSTANTS}


def decide(p_up_integrated, fav, side_ask, constants, legacy_signal=True):
    """Hypothetical decision for ONE existing legacy call. Never creates or reverses a call."""
    c = check_constants(constants)
    out = {"decision": NO_NEW_CALL, "integrated_p_up": None, "integrated_p_favored": None,
           "integrated_conf": None, "integrated_raw_edge": None, "integrated_net_edge": None}
    if legacy_signal is not True:
        return out                                   # the overlay can never create a call
    p = clamp_p(p_up_integrated)
    pf = favored(p, fav)
    conf = integrated_conf(pf)
    raw, net = edges(conf, side_ask, c["ENTRY_COST_CENTS"])
    out.update(integrated_p_up=p, integrated_p_favored=pf, integrated_conf=conf,
               integrated_raw_edge=raw, integrated_net_edge=net)
    if direction_flips(p, fav):
        out["decision"] = BLOCK_FLIP
    elif conf < c["MIN_CONF"]:
        out["decision"] = BLOCK_LOW_CONF
    elif net <= 0 or net < c["EDGE_THRESH"]:
        out["decision"] = BLOCK_NO_EDGE
    else:
        out["decision"] = WOULD_ALLOW
    return out


def overlay(p_base_up, p_b_up, p_c_up, alpha, fav, side_ask, constants, legacy_signal=True):
    """Full Step 5 overlay for one existing first signal."""
    d = perp_delta(p_c_up, p_b_up)
    dc = cap_delta(d)
    a = check_alpha(alpha)
    p = blend(p_base_up, dc, a)
    out = decide(p, fav, side_ask, constants, legacy_signal)
    out.update(alpha=a, p_base_up=clamp_p(p_base_up), delta_raw=d, delta_capped=dc, applied_delta=a * dc,
               cap_hit=abs(d) > MAX_ABS_PERP_DELTA)
    if legacy_signal is not True:
        out["integrated_p_up"] = None
    return out
