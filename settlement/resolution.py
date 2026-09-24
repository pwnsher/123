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

from settlement.policy import WINDOW_POLICIES, reconstruction_policy, window_policy
from settlement.reconstruction import reconstruct
from settlement.schemas import iso_utc

MIN_MARKETS_FOR_CONVENTION_VERDICT = 30
VALUE_TOLERANCE = 0.01                      # index points; |reconstructed - expiration_value|


def _window_obs(observations_by_index, market, wpol, extra_before_ms=0):
    obs = observations_by_index.get(market.index_id, [])
    lo = wpol.lookback_start(market.close_ts_ms) - extra_before_ms
    return [o for o in obs if lo <= o.event_ts_ms <= market.close_ts_ms]


def index_observations(observations):
    out = {}
    for o in observations:
        out.setdefault(o.index_id, []).append(o)
    return out


def verify_market(market, resolution, observations_by_index, wpol, rpol, issues=()):
    obs = _window_obs(observations_by_index, market, wpol)
    res = reconstruct(market, obs, wpol, rpol, as_of_ms=None, issues=issues)
    official = resolution.result if resolution is not None else None
    ev = resolution.expiration_value if resolution is not None else None
    agree = None
    if official is not None and res.reconstructed_outcome is not None:
        agree = res.reconstructed_outcome == official
    diff = abs(res.final_value - ev) if (res.final_value is not None and ev is not None) else None
    if res.final_value is None:
        category = "NO_RECONSTRUCTION"
    elif official is None:
        category = "NO_OFFICIAL_RESULT"
    elif res.reconstructed_outcome is None:
        category = "AT_STRIKE" if market.strike is not None else "NO_STRIKE"
    else:
        category = "AGREE" if agree else "DISAGREE"
    strike_check = None
    if market.open_ts_ms is not None and market.strike is not None:
        prev = replace(market, close_ts_ms=market.open_ts_ms, strike=None, ticker=market.ticker + "#open-window")
        pr = reconstruct(prev, _window_obs(observations_by_index, prev, wpol), wpol, rpol, as_of_ms=None, issues=issues)
        strike_check = {"open_window_value": pr.final_value, "open_window_quality": pr.quality.value,
                        "abs_diff_vs_strike": abs(pr.final_value - market.strike) if pr.final_value is not None else None}
    st = res.state
    return {"ticker": market.ticker, "asset": market.asset, "index_id": market.index_id,
            "close_utc": iso_utc(market.close_ts_ms), "strike": market.strike, "strike_source": market.strike_source,
            "reconstructed_value": res.final_value, "reconstructed_outcome": res.reconstructed_outcome,
            "official_result": official, "official_expiration_value": ev, "expiration_value_abs_diff": diff,
            "expiration_value_within_tolerance": (diff <= VALUE_TOLERANCE) if diff is not None else None,
            "agreement": agree, "category": category,
            "coverage": (st.samples_filled / st.samples_expected) if st.samples_expected else None,
            "samples_filled": st.samples_filled, "samples_expected": st.samples_expected,
            "quality": st.quality.value, "flags": list(st.flags), "window_policy": wpol.policy_id,
            "reconstruction_policy": rpol.policy_id, "input_digest": res.provenance["input_digest"],
            "sources": res.provenance["sources"], "strike_check": strike_check}


def summarize_rows(rows):
    n = len(rows)
    with_official = [r for r in rows if r["official_result"] is not None]
    adequate = [r for r in rows if r["reconstructed_value"] is not None]
    comparable = [r for r in rows if r["agreement"] is not None]
    diffs = [r["expiration_value_abs_diff"] for r in rows if r["expiration_value_abs_diff"] is not None]
    by_q = {}
    for r in rows:
        by_q[r["quality"]] = by_q.get(r["quality"], 0) + 1
    return {"markets": n, "with_official_result": len(with_official), "with_adequate_data": len(adequate),
            "compared": len(comparable), "agree": sum(1 for r in comparable if r["agreement"]),
            "disagree": sum(1 for r in comparable if not r["agreement"]),
            "disagreements": [r["ticker"] for r in comparable if not r["agreement"]],
            "missing_data": by_q.get("MISSING", 0), "insufficient_coverage": by_q.get("INSUFFICIENT_COVERAGE", 0),
            "schema_mismatch": by_q.get("SCHEMA_MISMATCH", 0), "quality_counts": dict(sorted(by_q.items())),
            "categories": {c: sum(1 for r in rows if r["category"] == c)
                           for c in sorted({r["category"] for r in rows})},
            "expiration_value": {"compared": len(diffs),
                                 "within_tolerance": sum(1 for d in diffs if d <= VALUE_TOLERANCE),
                                 "exact": sum(1 for d in diffs if d == 0.0),
                                 "max_abs_diff": max(diffs) if diffs else None,
                                 "mean_abs_diff": statistics.fmean(diffs) if diffs else None,
                                 "median_abs_diff": statistics.median(diffs) if diffs else None,
                                 "tolerance": VALUE_TOLERANCE}}


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
    ev_ok = {pid: s["expiration_value"]["within_tolerance"] for pid, s in out["policies"].items()}
    compared = max((s["expiration_value"]["compared"] for s in out["policies"].values()), default=0)
    if compared < MIN_MARKETS_FOR_CONVENTION_VERDICT:
        out["convention_verdict"] = {"status": "INSUFFICIENT_DATA", "markets_with_expiration_value": compared,
                                     "needed": MIN_MARKETS_FOR_CONVENTION_VERDICT}
    else:
        top = max(ev_ok.values())
        tied = sorted(p for p, v in ev_ok.items() if v == top)
        out["convention_verdict"] = {"status": "EVALUATED" if len(tied) == 1 else "EVALUATED_TIED",
                                     "best_policies": tied, "within_tolerance_by_policy": ev_ok,
                                     "markets_with_expiration_value": compared,
                                     "note": None if len(tied) == 1 else
                                     "these conventions are indistinguishable on this data (e.g. exact 1-s feeds)"}
    return out
