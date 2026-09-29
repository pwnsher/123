"""
Resolution verification: reconstructed settlement vs Kalshi's official outcome.

For every market with official data it reports the reconstructed value and outcome, the official
result and expiration_value, their agreement, coverage and quality. It runs every requested window
convention so the boundary question (which 60 instants, which sampling) can be answered from data:
the convention whose values match expiration_value is the verified one. With too few markets it
says INSUFFICIENT_DATA instead of naming a winner.

Also (research): reconstructs the window ending at the market's OPEN with the same convention and
compares it with Kalshi's strike, to test whether the strike is the previous window's average.
"""
import statistics
from dataclasses import replace
from fractions import Fraction

from settlement.policy import WINDOW_POLICIES, reconstruction_policy, window_policy
from settlement.assets import series_of
from settlement.reconstruction import reconstruct
from settlement.rules import exact, rule_for
from settlement.schemas import iso_utc

MIN_MARKETS_FOR_CONVENTION_VERDICT = 30
# Step 6.1: there is NO universal value tolerance. A reconstructed value is checked against Kalshi's expiration_value at
# the CONTRACT's official precision (settlement.rules: settlement_decimal_places): exact match after official rounding,
# within half an official display unit (unrounded), outcome agreement and the raw unrounded difference are reported
# separately. A tolerance fit for BTC (2 dp) can never validate a 4-dp XRP value.


def _window_obs(observations_by_index, market, wpol, extra_before_ms=0):
    obs = observations_by_index.get(market.index_id, [])
    lo = wpol.lookback_start(market.close_ts_ms) - extra_before_ms
    return [o for o in obs if lo <= o.event_ts_ms <= market.close_ts_ms]


def index_observations(observations):
    out = {}
    for o in observations:
        out.setdefault(o.index_id, []).append(o)
    return out


def value_checks(unrounded, settlement_value, tie_candidates, official_value, decimal_places):
    """Precision-aware comparison with the official expiration value (exact decimal arithmetic)."""
    out = {"expiration_value_exact_after_rounding": None, "expiration_value_within_half_unit": None,
           "expiration_value_raw_abs_diff": None, "official_precision_dp": decimal_places,
           "half_unit": None}
    if official_value is None:
        return out
    ev = exact(official_value)
    if unrounded is not None:
        out["expiration_value_raw_abs_diff"] = float(abs(exact(unrounded) - ev))
    if decimal_places is None:
        return out                                              # precision unknown: nothing can be validated
    half = Fraction(1, 2) / Fraction(10) ** decimal_places
    out["half_unit"] = float(half)
    if settlement_value is not None:
        out["expiration_value_exact_after_rounding"] = exact(settlement_value) == ev
    elif tie_candidates:
        out["expiration_value_exact_after_rounding"] = any(exact(t) == ev for t in tie_candidates)
    if unrounded is not None:
        out["expiration_value_within_half_unit"] = abs(exact(unrounded) - ev) <= half
    return out


def verify_market(market, resolution, observations_by_index, wpol, rpol, issues=()):
    obs = _window_obs(observations_by_index, market, wpol)
    res = reconstruct(market, obs, wpol, rpol, as_of_ms=None, issues=issues)
    official = resolution.result if resolution is not None else None
    ev = resolution.expiration_value if resolution is not None else None
    agree = None
    if official is not None and res.reconstructed_outcome is not None:
        agree = res.reconstructed_outcome == official
    st_ = res.settlement or {}
    rule = rule_for(market.series or series_of(market.ticker), market.close_ts_ms)
    dp = rule.settlement_decimal_places if (rule is not None and rule.known) else None
    checks = value_checks(res.final_value, res.settlement_value, st_.get("tie_candidates"), ev, dp)
    if res.final_value is None:
        category = "NO_RECONSTRUCTION"
    elif official is None:
        category = "NO_OFFICIAL_RESULT"
    elif res.reconstructed_outcome is None:
        category = {"RULE_UNKNOWN": "RULE_UNKNOWN", "RULE_UNVERIFIED": "RULE_UNVERIFIED",
                    "ROUNDING_TIE_UNRESOLVED": "ROUNDING_TIE_UNRESOLVED"}.get(st_.get("status"), "NO_STRIKE")
    else:
        category = "AGREE" if agree else "DISAGREE"
    strike_check = None
    if market.open_ts_ms is not None and market.strike is not None:
        prev = replace(market, close_ts_ms=market.open_ts_ms, strike=None, ticker=market.ticker + "#open-window")
        pr = reconstruct(prev, _window_obs(observations_by_index, prev, wpol), wpol, rpol, as_of_ms=None, issues=issues)
        strike_check = {"open_window_value": pr.final_value, "open_window_settlement_value": pr.settlement_value,
                        "open_window_quality": pr.quality.value,
                        "abs_diff_vs_strike": abs(pr.final_value - market.strike) if pr.final_value is not None else None}
    st = res.state
    row = {"ticker": market.ticker, "asset": market.asset, "index_id": market.index_id,
           "close_utc": iso_utc(market.close_ts_ms), "strike": market.strike, "strike_source": market.strike_source,
           "reconstructed_value": res.settlement_value, "reconstructed_unrounded_mean": res.final_value,
           "reconstructed_outcome": res.reconstructed_outcome, "settlement_status": st_.get("status"),
           "settlement_rule_id": st_.get("rule_id"), "settlement_rule_fingerprint": st_.get("rule_fingerprint"),
           "tie_candidates": list(st_["tie_candidates"]) if st_.get("tie_candidates") else None,
           "at_strike": bool(st_.get("at_strike")),
           "official_result": official, "official_expiration_value": ev,
           "agreement": agree, "category": category,
           "coverage": (st.samples_filled / st.samples_expected) if st.samples_expected else None,
           "samples_filled": st.samples_filled, "samples_expected": st.samples_expected,
           "quality": st.quality.value, "flags": list(st.flags), "window_policy": wpol.policy_id,
           "reconstruction_policy": rpol.policy_id, "input_digest": res.provenance["input_digest"],
           "sources": res.provenance["sources"], "strike_check": strike_check}
    row.update(checks)
    return row


def summarize_rows(rows):
    n = len(rows)
    with_official = [r for r in rows if r["official_result"] is not None]
    adequate = [r for r in rows if r["reconstructed_unrounded_mean"] is not None]
    comparable = [r for r in rows if r["agreement"] is not None]
    raw = [r["expiration_value_raw_abs_diff"] for r in rows if r["expiration_value_raw_abs_diff"] is not None]
    checked = [r for r in rows if r["expiration_value_exact_after_rounding"] is not None]
    halfu = [r for r in rows if r["expiration_value_within_half_unit"] is not None]
    by_q = {}
    for r in rows:
        by_q[r["quality"]] = by_q.get(r["quality"], 0) + 1
    by_asset = {}
    for r in checked:
        e = by_asset.setdefault(r["asset"], {"compared": 0, "exact_after_rounding": 0,
                                             "official_precision_dp": r["official_precision_dp"]})
        e["compared"] += 1
        e["exact_after_rounding"] += bool(r["expiration_value_exact_after_rounding"])
    return {"markets": n, "with_official_result": len(with_official), "with_adequate_data": len(adequate),
            "compared": len(comparable), "agree": sum(1 for r in comparable if r["agreement"]),
            "disagree": sum(1 for r in comparable if not r["agreement"]),
            "disagreements": [r["ticker"] for r in comparable if not r["agreement"]],
            "missing_data": by_q.get("MISSING", 0), "insufficient_coverage": by_q.get("INSUFFICIENT_COVERAGE", 0),
            "schema_mismatch": by_q.get("SCHEMA_MISMATCH", 0), "quality_counts": dict(sorted(by_q.items())),
            "categories": {c: sum(1 for r in rows if r["category"] == c)
                           for c in sorted({r["category"] for r in rows})},
            "expiration_value": {"compared": len(checked),
                                 "exact_after_rounding": sum(1 for r in checked if r["expiration_value_exact_after_rounding"]),
                                 "within_half_unit": sum(1 for r in halfu if r["expiration_value_within_half_unit"]),
                                 "half_unit_compared": len(halfu),
                                 "precision_unknown": sum(1 for r in rows if r["official_expiration_value"] is not None
                                                          and r["official_precision_dp"] is None),
                                 "by_asset": dict(sorted(by_asset.items())),
                                 "max_raw_abs_diff": max(raw) if raw else None,
                                 "mean_raw_abs_diff": statistics.fmean(raw) if raw else None,
                                 "median_raw_abs_diff": statistics.median(raw) if raw else None,
                                 "rule": "exact match at each contract's official precision; no universal tolerance"}}


def verify_all(markets, resolutions, observations, window_policy_ids=None, rpol=None, issues=()):
    """markets: [SettlementMarket]; resolutions: {ticker: OfficialResolution}. Returns rows + summaries."""
    rpol = rpol or reconstruction_policy()
    ids = list(window_policy_ids or WINDOW_POLICIES)
    by_index = index_observations(observations)
    out = {"reconstruction_policy": rpol.policy_id, "policies": {}, "rows": []}
    for pid in ids:
        wpol = window_policy(pid)
        rows = [verify_market(m, resolutions.get(m.ticker), by_index, wpol, rpol, issues)
                for m in sorted(markets, key=lambda m: (m.close_ts_ms, m.ticker))]
        out["policies"][pid] = summarize_rows(rows)
        out["rows"] += rows
    ev_ok = {pid: s["expiration_value"]["exact_after_rounding"] for pid, s in out["policies"].items()}
    compared = max((s["expiration_value"]["compared"] for s in out["policies"].values()), default=0)
    if compared < MIN_MARKETS_FOR_CONVENTION_VERDICT:
        out["convention_verdict"] = {"status": "INSUFFICIENT_DATA", "markets_with_expiration_value": compared,
                                     "needed": MIN_MARKETS_FOR_CONVENTION_VERDICT}
    else:
        top = max(ev_ok.values())
        tied = sorted(p for p, v in ev_ok.items() if v == top)
        out["convention_verdict"] = {"status": "EVALUATED" if len(tied) == 1 else "EVALUATED_TIED",
                                     "best_policies": tied, "exact_after_rounding_by_policy": ev_ok,
                                     "markets_with_expiration_value": compared,
                                     "note": None if len(tied) == 1 else
                                     "these conventions are indistinguishable on this data (e.g. exact 1-s feeds)"}
    return out
