#!/usr/bin/env python3
"""
research_status.py - how much REAL research data exists and which predeclared Step-6 gates are still unmet. READ ONLY.

    py research_status.py [--sessions market_data_sessions] [--json analysis_output/step6_research_status.json]

Reports: real sessions (synthetic ones listed separately and never counted), capture hours, days covered, quality
verdicts, settled markets per asset (official results) and - when a research matrix has been built - usable gold-label
markets per asset / checkpoint / volatility slice, the settlement-convention verification status and every unmet
sample gate. It never estimates a completion date: data accrues only as fast as sessions are captured.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


def _series_asset(ticker):
    from settlement.assets import SERIES_ASSET
    return SERIES_ASSET.get(str(ticker).split("-", 1)[0])


def status(sessions_root, out_dir):
    from feature_eval.gates import SampleGates
    from feature_eval.quality import discover_sessions, is_synthetic, validate_session
    sess = discover_sessions(sessions_root)
    real = [d for d in sess if not is_synthetic(d)]
    out = {"sessions_root": sessions_root, "real_sessions": len(real),
           "synthetic_sessions_ignored": [os.path.basename(d) for d in sess if is_synthetic(d)],
           "capture_hours": 0.0, "days_covered": [], "quality": {}, "settled_markets_per_asset": {},
           "markets_per_asset": {}}
    days = set()
    for d in real:
        r = validate_session(d)
        out["quality"][r["session_id"]] = {"verdict": r["verdict"], "problems": r["problems"][:5]}
        sp = r.get("span") or {}
        if sp:
            out["capture_hours"] += sp["minutes"] / 60.0
            import datetime as _dt
            t = sp["first_receive_ms"]
            while t <= sp["last_receive_ms"]:
                days.add(_dt.datetime.fromtimestamp(t / 1000, _dt.timezone.utc).strftime("%Y-%m-%d"))
                t += 86_400_000
            days.add(_dt.datetime.fromtimestamp(sp["last_receive_ms"] / 1000, _dt.timezone.utc).strftime("%Y-%m-%d"))
        lab = (r.get("checks") or {}).get("labels") or {}
        if r["verdict"] != "REJECT":
            from market_data.features.dataset import discover_markets
            from market_data.replay import load_sessions
            from market_data.types import EventType
            s3 = load_sessions([d])
            mk = discover_markets(s3.events, ("BTC", "ETH", "SOL", "XRP"))
            res = {e.payload.get("ticker") for e in s3.events if e.event_type == EventType.RESOLUTION}
            for tk in mk:
                a = _series_asset(tk) or "?"
                out["markets_per_asset"][a] = out["markets_per_asset"].get(a, 0) + 1
                if tk in res:
                    out["settled_markets_per_asset"][a] = out["settled_markets_per_asset"].get(a, 0) + 1
        out["quality"][r["session_id"]]["labels"] = lab.get("with_official_resolution")
    out["capture_hours"] = round(out["capture_hours"], 2)
    out["days_covered"] = sorted(days)
    g = SampleGates()
    unmet = []
    tot = sum(out["settled_markets_per_asset"].values())
    if tot < g.min_total_markets:
        unmet.append({"requirement": "settled_independent_markets", "have": tot, "need": g.min_total_markets})
    for a in ("BTC", "ETH", "SOL", "XRP"):
        h = out["settled_markets_per_asset"].get(a, 0)
        if h < g.min_markets_per_asset:
            unmet.append({"requirement": f"markets_asset_{a}", "have": h, "need": g.min_markets_per_asset})
    # the built matrix (if any) adds per-checkpoint / regime / label-verification detail
    fa = os.path.join(out_dir, "step6_data_quality.json")
    if os.path.exists(fa):
        with open(fa, encoding="utf-8") as f:
            dq = json.load(f)
        sg = dq.get("sample_gate")
        if sg:
            out["sample_gate_from_last_run"] = {"status": sg["status"], "counts": sg["counts"],
                                                "failed_requirements": sg["failed_requirements"]}
            unmet = sg["failed_requirements"]
        ds = dq.get("dataset") or {}
        if ds.get("settlement_gate"):
            out["settlement_convention"] = {k: ds["settlement_gate"].get(k) for k in ("status", "convention", "reason")
                                            if k in ds["settlement_gate"]}
    out.setdefault("settlement_convention", {"status": "SETTLEMENT_UNVERIFIED", "reason": "no research matrix built yet"})
    out["unmet_gates"] = unmet
    out["status"] = "INSUFFICIENT_DATA" if (not real or unmet) else "SUFFICIENT_FOR_RESEARCH"
    out["eta"] = "not estimated - depends only on how many real sessions are captured"
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="Step-6 research data status (read only).")
    ap.add_argument("--sessions", default=os.path.join(HERE, "market_data_sessions"))
    ap.add_argument("--out", default=os.path.join(HERE, "analysis_output"))
    ap.add_argument("--json", default=None)
    a = ap.parse_args(argv)
    s = status(a.sessions, a.out)
    print(f"real sessions: {s['real_sessions']}   capture hours: {s['capture_hours']}   days covered: "
          f"{len(s['days_covered'])}")
    if s["synthetic_sessions_ignored"]:
        print(f"synthetic sessions ignored: {len(s['synthetic_sessions_ignored'])}")
    for sid, q in sorted(s["quality"].items()):
        print(f"  {sid}: {q['verdict']}  official results: {q.get('labels')}")
    print(f"settled markets per asset: {s['settled_markets_per_asset'] or '{}'}")
    print(f"settlement convention: {s['settlement_convention'].get('status')}")
    print(f"STATUS: {s['status']}")
    for u in s["unmet_gates"]:
        print(f"  unmet: {u['requirement']}: {u['have']} / {u['need']}")
    print(f"ETA: {s['eta']}")
    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)), exist_ok=True)
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=1, sort_keys=True, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
