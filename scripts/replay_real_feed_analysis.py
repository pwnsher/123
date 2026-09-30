#!/usr/bin/env python3
"""
READ-ONLY replay analysis of a captured REAL session against the CURRENT (Step 6.4) ingestion code.

    py scripts/replay_real_feed_analysis.py <session_dir> [--out analysis_output/<file>.json]

It never writes into the session directory and never rewrites a raw record. It re-runs, over the stored messages:
    * book ordering     the stored micro events in their ORIGINAL processing order (ingest_seq) through the current
                        per-book / per-chain BookReconstructor (how many ordering failures remain)
    * clock monitor     the stored (receive wall, receive mono) pairs in processing order (micro / perp / step3)
    * Kalshi REST       every stored markets / orderbook raw response through the current KalshiAdapter
    * Kalshi trades     every stored trade page, in order, through the current trade_id dedup
    * market rules      every captured market's rules_primary / rules_secondary through the current parser / resolver
    * HTTP failures     every recorded poll error classified with the current typed-HTTP semantics
    * quality           the current session validator (verdicts of the ORIGINAL data)
Counterfactual numbers (what would not have happened) are labelled PROJECTION: data that was never received during a
false reconnect window cannot be recreated, so those are estimates, never measurements.
"""
import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)


def _records(d):
    from market_data.storage import read_session
    return read_session(d).records if os.path.isdir(d) else []


def analyze(session_dir):
    from market_data.clock import ClockMonitor
    from market_data.sources.base import Ctx, Sequencer
    from market_data.sources.kalshi import KalshiAdapter
    from market_data.transport.http import classify
    from microstructure.reconstruction import BookOrderError, BookReconstructor
    from microstructure.replay import load_micro_sessions
    from settlement.kalshi_markets import parse_market
    from settlement.market_rules import resolve_market_rule

    out = {"session_dir_name": os.path.basename(os.path.normpath(session_dir)), "read_only": True,
           "note": "debugging evidence only - not research evidence; the raw session is not modified"}
    step3, micro, perp = (_records(session_dir), _records(os.path.join(session_dir, "micro")),
                          _records(os.path.join(session_dir, "perp")))

    # ---------------- 1. book ordering ----------------
    m5 = load_micro_sessions([session_dir]).events if micro else []
    evs = sorted(m5, key=lambda e: e.ingest_seq)
    rc, errors, gmax, greg = BookReconstructor(), 0, None, 0
    for e in evs:
        if gmax is not None and e.receive_ts_ms < gmax:
            greg += 1
        gmax = max(gmax or e.receive_ts_ms, e.receive_ts_ms)
        try:
            rc.apply(e)
        except BookOrderError:
            errors += 1
    feed = [d for k, d in micro if k == "feed"]
    disc = [d for d in feed if d.get("event") == "disconnect"]
    ordering_disc = [d for d in disc if "availability order" in str(d.get("error"))]
    genuine = Counter(str(d.get("error"))[:80] for d in disc if "availability order" not in str(d.get("error")))
    windows = defaultdict(int)
    for d in ordering_disc:
        nxt = [c for c in feed if c.get("event") == "connect" and c.get("source") == d["source"] and c["wall_ms"] >= d["wall_ms"]]
        if nxt:
            windows[d["source"]] += min(c["wall_ms"] for c in nxt) - d["wall_ms"]
    out["book_ordering"] = {
        "micro_events": len(evs), "global_receive_regressions_in_processing_order": greg,
        "ordering_errors_with_current_code": errors,
        "recorded_disconnects_from_the_global_ordering_check": len(ordering_disc),
        "recorded_disconnects_other": dict(genuine),
        "PROJECTION": {"false_disconnects_avoided": len(ordering_disc),
                       "false_reconnect_window_ms_by_source": dict(sorted(windows.items())),
                       "remaining_genuine_book_invalidations": dict(genuine)}}

    # ---------------- 2. clock monitor ----------------
    clk = {}
    for name, recs in (("step3", step3), ("micro", micro), ("perp", perp)):
        raws = sorted((d for k, d in recs if k == "raw" and d.get("receive_mono_ns") is not None),
                      key=lambda d: d["ingest_seq"])
        cm = ClockMonitor()
        for d in raws:
            cm.check(d["receive_ts_ms"], d["receive_mono_ns"])
        clk[name] = {"samples": len(raws), "anomalies": Counter(a.kind for a in cm.anomalies),
                     "reordered_samples_ignored": cm.reordered}
    for name, sub in (("step3", ""), ("micro", "micro"), ("perp", "perp")):
        p = os.path.join(session_dir, sub, "manifest.json")
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                clk[name]["recorded_anomalies"] = Counter(a["kind"] for a in (json.load(f).get("clock_anomalies") or []))
    out["clock_monitor"] = clk

    # ---------------- 3. Kalshi REST ----------------
    k = KalshiAdapter(["BTC", "ETH", "SOL", "XRP"])
    s3raw = sorted((d for kd, d in step3 if kd == "raw" and d.get("source") == "kalshi"), key=lambda d: d["ingest_seq"])
    rest = {"recorded_failures": Counter(str(d.get("reason"))[:70] for kd, d in step3 if kd == "failure"),
            "reparsed": Counter(), "failures_with_current_code": Counter(), "no_quote_states": 0}
    trades_by = defaultdict(lambda: {"rows": 0, "normalized": 0})
    zero_overlap_pairs, last_ids = Counter(), {}
    for d in s3raw:
        ctx = Ctx("replay", Sequencer(), d["receive_ts_ms"])
        body = json.loads(d["text"])
        if d["stream"] == "orderbook":
            res = k.parse_orderbook("?", "?", body, ctx)
        elif d["stream"] == "markets":
            ms = body.get("markets") or []
            asset = ms[0]["ticker"][2:5] if ms else "?"
            res, _m = k.parse_markets(asset, body, ctx, now_ms=d["receive_ts_ms"])
            rest["no_quote_states"] += sum("NO_QUOTE" in e.flags for e in res.events)
        elif d["stream"] == "trades":
            rows = body.get("trades") or []
            tk = rows[0]["ticker"] if rows else "?"
            ids = {t.get("trade_id") for t in rows}
            if tk in last_ids and not (ids & last_ids[tk]):
                zero_overlap_pairs[tk] += 1
            last_ids[tk] = ids
            res = k.parse_trades(tk[2:5], body, ctx)
            trades_by[tk]["rows"] += len(rows)
            trades_by[tk]["normalized"] += len(res.events)
        else:
            continue
        rest["reparsed"][d["stream"]] += 1
        for f in res.failures:
            rest["failures_with_current_code"][f.reason[:70]] += 1
    stored = Counter()
    for kd, d in step3:
        if kd == "event" and d.get("event_type") == "TRADE" and d.get("source") == "kalshi":
            stored[d["symbol"]] += 1
    out["kalshi_rest"] = rest
    out["kalshi_trades"] = {tk: dict(v, duplicates_recorded_originally=stored.get(tk, 0) - v["normalized"],
                                     stored_originally=stored.get(tk, 0),
                                     consecutive_polls_without_overlap=zero_overlap_pairs.get(tk, 0))
                            for tk, v in sorted(trades_by.items())}
    out["kalshi_trades_note"] = ("consecutive 100-trade polls WITHOUT any overlap mean trades between them were very likely "
                                 "never fetched by the old polling; the incremental min_ts + cursor polling fetches them")

    # ---------------- 4. market rules ----------------
    firsts = {}
    for d in s3raw:
        if d["stream"] == "markets":
            for m in json.loads(d["text"]).get("markets") or []:
                firsts.setdefault(m["ticker"], (m, d["receive_ts_ms"]))
    rules = {}
    for tk, (m, ts) in sorted(firsts.items()):
        mk, _r, _i = parse_market(m, source="kalshi_market_api", capture_ts_ms=ts)
        _rule, info = resolve_market_rule(mk)
        parsed = info.get("parsed") or {}
        rules[tk] = {"parser": parsed.get("status"), "status": info["status"], "basis": info["basis"],
                     "operator": parsed.get("comparison_operator"), "decimal_places": parsed.get("settlement_decimal_places")}
    out["market_rules"] = rules

    # ---------------- 5. HTTP failures ----------------
    http = Counter()
    for name, recs in (("micro", micro), ("perp", perp)):
        for kd, d in recs:
            if kd == "feed" and d.get("event") == "poll_error":
                m = re.match(r"^(\d{3}) ", str(d.get("error")))
                if m:
                    kind, _retry, term = classify(int(m.group(1)))
                    url = str(d["error"]).split("for url: ", 1)[-1].split("?", 1)[0]
                    http[(name, int(m.group(1)), kind, term, url)] += 1
                else:
                    http[(name, None, "AGGREGATE", False, str(d.get("error"))[:60])] += 1
    out["http_failures"] = [{"store": a, "status": b, "kind": c, "terminal": t, "endpoint_or_detail": e, "recorded": n}
                            for (a, b, c, t, e), n in sorted(http.items(), key=lambda x: str(x))]
    out["http_PROJECTION"] = ("with the current code each terminal (403 / 451) stream is requested ONCE and then recorded "
                              "UNAVAILABLE; the Binance full-book snapshots mark binance_usdm_book UNAVAILABLE and its "
                              "websocket is stopped; the aggregate 'all streams failed' backoffs caused only by terminal "
                              "streams disappear")

    # ---------------- 6. quality (current validator over the ORIGINAL data) ----------------
    try:
        from feature_eval.quality import validate_session
        q = validate_session(session_dir)
        out["quality_current_validator_on_original_data"] = {
            "verdict": q["verdict"], "problems": q.get("problems"),
            "book_verdicts": Counter(v["verdict"] for v in q["checks"].get("books", {}).values()),
            "source_verdicts": Counter(v["verdict"] for v in q["sources"].values()),
            "kalshi_sources": {kk: v["verdict"] for kk, v in q["sources"].items() if kk.startswith("kalshi:")}}
    except Exception as e:                                   # noqa: BLE001 - reported, never hidden
        out["quality_current_validator_on_original_data"] = {"error": f"{type(e).__name__}: {e}"[:300]}
    return out


def _plain(o):
    if isinstance(o, dict):
        return {str(k): _plain(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_plain(v) for v in o]
    return o


def main(argv=None):
    ap = argparse.ArgumentParser(description="Read-only replay analysis of a real captured session (Step 6.4 code).")
    ap.add_argument("session_dir")
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    res = _plain(analyze(a.session_dir))
    txt = json.dumps(res, indent=1, sort_keys=True, default=str)
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(txt + "\n")
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
