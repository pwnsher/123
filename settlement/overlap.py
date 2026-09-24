"""
Live-vs-history comparison: do live-collected CF observations agree with downloaded history, and do
they reconstruct the same settlement? Every disagreement is counted and listed; nothing is hidden.

compare_observations()  point-level: exact / tolerance matches, errors, missing, extra
compare_market()        window-level: per-instant sample agreement (value, which source instant was
                        used, membership), accumulated value at checkpoints, final values, qualities
compare_published()     our live reconstruction vs Kalshi-published final-minute averages
"""
import statistics

from settlement.checkpoints import DEFAULT_CHECKPOINTS_S
from settlement.policy import reconstruction_policy, window_policy
from settlement.reconstruction import reconstruct
from settlement.types import OBSERVED_KINDS


def _by_key(observations):
    """{(index_id, event_ts): value}; values that conflict within ONE source are reported, not merged."""
    out, conflicts = {}, set()
    for o in observations:
        k = (o.index_id, o.event_ts_ms)
        if k in out and out[k] != o.value:
            conflicts.add(k)
        out.setdefault(k, o.value)
    return out, conflicts


def _stats(errs):
    return {"max_abs_error": max(errs) if errs else None, "mean_abs_error": statistics.fmean(errs) if errs else None,
            "median_abs_error": statistics.median(errs) if errs else None}


def compare_observations(live, history, abs_tol=1e-6, rel_tol=0.0, max_listed=50):
    lv, lc = _by_key(live)
    hv, hc = _by_key(history)
    common = sorted(set(lv) & set(hv))
    errs = [abs(lv[k] - hv[k]) for k in common]
    exact = sum(1 for e in errs if e == 0.0)
    tol = sum(1 for k, e in zip(common, errs) if e <= max(abs_tol, rel_tol * abs(hv[k])))
    missing = sorted(set(hv) - set(lv))            # in history, not received live
    extra = sorted(set(lv) - set(hv))              # received live, absent from history
    worst = sorted(((e, k) for k, e in zip(common, errs) if e > max(abs_tol, rel_tol * abs(hv[k]))), reverse=True)
    return dict({"live_points": len(lv), "history_points": len(hv), "common": len(common), "exact_matches": exact,
                 "tolerance_matches": tol, "tolerance": {"abs": abs_tol, "rel": rel_tol},
                 "mismatches": len(common) - tol, "missing_in_live": len(missing), "extra_in_live": len(extra),
                 "conflicts_within_live": len(lc), "conflicts_within_history": len(hc),
                 "worst_mismatches": [{"index_id": k[0], "event_ts_ms": k[1], "abs_error": e} for e, k in worst[:max_listed]],
                 "missing_examples": [list(k) for k in missing[:max_listed]],
                 "extra_examples": [list(k) for k in extra[:max_listed]]}, **_stats(errs))


def compare_market(market, live, history, wpol=None, rpol_live=None, rpol_history=None,
                   checkpoints_s=DEFAULT_CHECKPOINTS_S, abs_tol=1e-6):
    wpol = wpol or window_policy()
    rl = rpol_live or reconstruction_policy()
    rh = rpol_history or reconstruction_policy()
    fl = reconstruct(market, live, wpol, rl, as_of_ms=None)
    fh = reconstruct(market, history, wpol, rh, as_of_ms=None)
    inst = []
    boundary = 0
    for a, b in zip(fl.samples, fh.samples):
        a_ok, b_ok = a.kind in OBSERVED_KINDS, b.kind in OBSERVED_KINDS
        membership_disagree = a_ok != b_ok
        source_instant_differs = a_ok and b_ok and a.source_event_ts_ms != b.source_event_ts_ms
        err = abs(a.value - b.value) if (a_ok and b_ok) else None
        if membership_disagree or source_instant_differs:
            boundary += 1
        if membership_disagree or source_instant_differs or (err is not None and err > abs_tol):
            inst.append({"grid_ts_ms": a.grid_ts_ms, "live_kind": a.kind.value, "history_kind": b.kind.value,
                         "live_source_ts_ms": a.source_event_ts_ms, "history_source_ts_ms": b.source_event_ts_ms,
                         "abs_error": err})
    cps = []
    for s in checkpoints_s:
        t = market.close_ts_ms - int(s * 1000)
        a = reconstruct(market, live, wpol, rl, as_of_ms=t).state
        b = reconstruct(market, history, wpol, rh, as_of_ms=t).state
        cps.append({"seconds_remaining": s, "live_accumulated_mean": a.accumulated_mean,
                    "history_accumulated_mean": b.accumulated_mean,
                    "abs_diff": (abs(a.accumulated_mean - b.accumulated_mean)
                                 if a.accumulated_mean is not None and b.accumulated_mean is not None else None),
                    "live_samples_filled": a.samples_filled, "history_samples_filled": b.samples_filled,
                    "live_quality": a.quality.value, "history_quality": b.quality.value})
    return {"ticker": market.ticker, "window_policy": wpol.policy_id,
            "live_final": fl.final_value, "history_final": fh.final_value,
            "final_abs_diff": abs(fl.final_value - fh.final_value) if (fl.final_value is not None and fh.final_value is not None) else None,
            "live_quality": fl.quality.value, "history_quality": fh.quality.value,
            "instant_disagreements": len(inst), "boundary_disagreements": boundary, "instants": inst,
            "checkpoints": cps}


def compare_published(markets, published, live, wpol=None, rpol=None):
    """Kalshi-published 'last 60 s windowed average (15 min)' vs our reconstruction of the same window.
    Uses the last published value whose frame time is <= close (and inside the window)."""
    wpol = wpol or window_policy()
    rpol = rpol or reconstruction_policy()
    rows = []
    for m in markets:
        start, _end = wpol.window_bounds(m.close_ts_ms)
        cand = [p for p in published if p.index_id == m.index_id and p.kind == "kalshi_last_60s_windowed_average_15min"
                and start <= p.event_ts_ms <= m.close_ts_ms]
        if not cand:
            continue
        pub = max(cand, key=lambda p: (p.event_ts_ms, p.receive_ts_ms or 0))
        ours = reconstruct(m, live, wpol, rpol, as_of_ms=None)
        rows.append({"ticker": m.ticker, "published_value": pub.value, "published_event_ts_ms": pub.event_ts_ms,
                     "reconstructed_value": ours.final_value, "quality": ours.quality.value,
                     "abs_diff": abs(pub.value - ours.final_value) if ours.final_value is not None else None})
    diffs = [r["abs_diff"] for r in rows if r["abs_diff"] is not None]
    return {"markets": len(rows), "compared": len(diffs), **_stats(diffs), "rows": rows}
