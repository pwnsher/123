#!/usr/bin/env python3
"""
collect_market_data.py — LOCAL high-resolution market-data COLLECTOR. RESEARCH / OBSERVATION ONLY.

    py collect_market_data.py --dry-run                              # plan + offline checks; no network
    py collect_market_data.py --assets BTC,ETH,SOL,XRP --duration 3600
    py collect_market_data.py --assets BTC --duration 900 --coinbase --secondary --kalshi
    py collect_market_data.py --cf --cf-source direct                # CF Benchmarks direct (needs CFB_API_ID/SECRET)

Sources (all enabled when none of --cf / --coinbase / --secondary / --kalshi is given):
    --cf         CF Benchmarks RTI (default via Kalshi's authenticated cfbenchmarks_value channel;
                 --cf-source direct|both). Needs credentials; without them the source is DISCONNECTED
                 with the reason and the others keep running. Coinbase is NEVER substituted for CF.
    --coinbase   Coinbase Exchange public websocket (ticker = BBO, matches = trades) + REST backfill
    --secondary  Kraken spot websocket v2 (trade + ticker/BBO)
    --kalshi     Kalshi public REST, read-only polling (market state, order book, trades, settled results)

Output: <output>/<session_id>/ (append-only gzip JSONL segments + manifest.json) and, for the Step-2
settlement tools, settlement_data/capture-<session_id>.jsonl (CF observations, published averages,
market metadata, official results). Research status page: http://127.0.0.1:<status-port>/ .

This process never imports the production watcher, never changes a call and cannot place an order:
every Kalshi request is a GET; the only signed request is the read-only websocket upgrade.
"""
import argparse
import os
import signal
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from market_data import APP_VERSION, FEATURE_SET_VERSION, RESEARCH_ONLY  # noqa: E402
from market_data.clock import SystemClock                              # noqa: E402

ALL_ASSETS = ("BTC", "ETH", "SOL", "XRP")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Research-only high-resolution market-data collector (read-only).")
    ap.add_argument("--assets", default=",".join(ALL_ASSETS))
    ap.add_argument("--duration", type=float, default=0, help="seconds (0 = until Ctrl+C)")
    ap.add_argument("--output", default=os.path.join(HERE, "market_data_sessions"))
    ap.add_argument("--cf", action="store_true")
    ap.add_argument("--cf-source", choices=("kalshi", "direct", "both"), default="kalshi")
    ap.add_argument("--coinbase", action="store_true")
    ap.add_argument("--secondary", action="store_true", help="Kraken spot")
    ap.add_argument("--kalshi", action="store_true")
    ap.add_argument("--kalshi-interval", type=float, default=1.0, help="seconds between Kalshi polls")
    ap.add_argument("--status-port", type=int, default=8766, help="0 disables the research status page")
    ap.add_argument("--settlement-store", default=None,
                    help="default settlement_data/capture-<session>.jsonl; 'none' disables")
    ap.add_argument("--no-raw", action="store_true", help="do not keep raw messages (NOT recommended)")
    ap.add_argument("--no-live-features", action="store_true")
    ap.add_argument("--dry-run", "--check", action="store_true", dest="dry_run",
                    help="print the plan and run offline checks; no network, nothing written")
    a = ap.parse_args(argv)
    a.assets = [x.strip().upper() for x in a.assets.split(",") if x.strip()]
    bad = [x for x in a.assets if x not in ALL_ASSETS]
    if bad or not a.assets:
        ap.error(f"unknown assets {bad}; choose from {','.join(ALL_ASSETS)}")
    if not (a.cf or a.coinbase or a.secondary or a.kalshi):
        a.cf = a.coinbase = a.secondary = a.kalshi = True
    return a


def build_sources(a):
    from market_data.sources.cf import CfDirectAdapter, CfViaKalshiAdapter
    from market_data.sources.coinbase import CoinbaseAdapter
    from market_data.sources.kalshi import KalshiAdapter
    from market_data.sources.kraken import KrakenAdapter
    src = {}
    if a.cf and a.cf_source in ("kalshi", "both"):
        src["cf_via_kalshi"] = CfViaKalshiAdapter(a.assets)
    if a.cf and a.cf_source in ("direct", "both"):
        src["cf_direct"] = CfDirectAdapter(a.assets)
    if a.coinbase:
        src["coinbase"] = CoinbaseAdapter(a.assets)
    if a.secondary:
        src["kraken"] = KrakenAdapter(a.assets)
    if a.kalshi:
        src["kalshi"] = KalshiAdapter(a.assets)
    return src


def credentials():
    from market_data.transport import kalshi_auth
    try:
        import cryptography  # noqa: F401
        crypto = True
    except ImportError:
        crypto = False
    return {"kalshi_ws_auth_configured": kalshi_auth.configured(), "cryptography_installed": crypto,
            "cfb_direct_configured": bool(os.environ.get("CFB_API_ID") and os.environ.get("CFB_API_SECRET"))}


def verify_code():
    """Offline: the production strategy, the settlement engine and this package match their baselines."""
    out = {}
    try:
        from kalshi_core import baseline
        ok, problems = baseline.verify()
        out["strategy"] = {"ok": ok, "problems": problems[:5]}
    except Exception as e:                                        # noqa: BLE001
        out["strategy"] = {"ok": False, "problems": [f"{type(e).__name__}: {e}"]}
    for name, mod in (("settlement", "settlement.fingerprint"), ("market_data", "market_data.fingerprint")):
        try:
            m = __import__(mod, fromlist=["verify"])
            ok, problems = m.verify()
            out[name] = {"ok": ok, "problems": problems[:5]}
        except Exception as e:                                    # noqa: BLE001
            out[name] = {"ok": False, "problems": [f"{type(e).__name__}: {e}"]}
    return out


def dry_run(a):
    srcs = build_sources(a)
    print(f"collect_market_data {APP_VERSION}  feature set {FEATURE_SET_VERSION}  research_only={RESEARCH_ONLY}")
    print(f"assets: {','.join(a.assets)}   duration: {a.duration or 'until Ctrl+C'} s   output: {a.output}")
    for n, ad in srcs.items():
        where = getattr(ad, "url", None) or "REST (GET only)"
        print(f"  source {n:14s} transport={ad.transport:4s} critical={ad.critical!s:5s} "
              f"auth={ad.auth_required!s:5s} {where}")
        for m in ad.subscribe_messages():
            print(f"      subscribe: {m}")
        if n == "kalshi":
            for asset in ad.assets:
                print(f"      GET {ad.markets_url(asset)[0]} {ad.markets_url(asset)[1]}")
    cred = credentials()
    print("credentials (presence only): " + ", ".join(f"{k}={v}" for k, v in cred.items()))
    if "cf_via_kalshi" in srcs and not (cred["kalshi_ws_auth_configured"] and cred["cryptography_installed"]):
        print("  NOTE: CF via Kalshi would be DISCONNECTED (credentials unavailable); other sources unaffected.")
    if "cf_direct" in srcs and not cred["cfb_direct_configured"]:
        print("  NOTE: CF direct would be DISCONNECTED (CFB_API_ID / CFB_API_SECRET not set).")
    ver = verify_code()
    for k, v in ver.items():
        print(f"code verification {k:11s}: {'MATCHES' if v['ok'] else 'MISMATCH'}" +
              ("" if v["ok"] else f"  {v['problems']}"))
    print("dry run: no network connection was made and nothing was written.")
    return 0 if all(v["ok"] for v in ver.values()) else 3


def run(a, clock=None, stop_event=None, getter=None, connect_fn=None, install_signals=True):
    from market_data.collector import Collector
    from market_data.kalshi_poller import KalshiPoller
    from market_data.runner import PollRunner, WsFeedRunner
    from market_data.transport import kalshi_auth
    from market_data.transport.http import HttpGetter

    clock = clock or SystemClock()
    stop = stop_event or threading.Event()
    srcs = build_sources(a)
    getter = getter or HttpGetter()
    sources_meta = {n: {"enabled": True, "critical": ad.critical, "transport": ad.transport,
                        "auth_required": ad.auth_required, "documentation": ad.documentation} for n, ad in srcs.items()}
    sources_meta["_credentials"] = credentials()
    session_root = a.output
    col = Collector(session_root, a.assets, {n: dict(m) for n, m in sources_meta.items()}, clock,
                    keep_raw=not a.no_raw, live_features=not a.no_live_features)
    store = a.settlement_store
    if store is None:
        store = os.path.join(HERE, "settlement_data", f"capture-{col.session_id}.jsonl")
    if store and store.lower() != "none":
        from settlement.cache import SettlementStore
        col.settlement_store = SettlementStore(store)
        col.manifest.settlement_store = store
    col.manifest.code_verification = verify_code()
    col.manifest.write(col.dir)

    runners = {}
    for n, ad in srcs.items():
        if n == "kalshi":
            poller = KalshiPoller(ad, getter, col, clock)
            runners[n] = PollRunner("kalshi", poller.poll, col, clock, interval_s=a.kalshi_interval, stop_event=stop)
            continue
        headers_fn = None
        backfill_fn = None
        if n == "cf_via_kalshi":
            from urllib.parse import urlparse
            path = urlparse(ad.url).path
            headers_fn = lambda path=path: kalshi_auth.ws_headers(path, clock.wall_ms())  # noqa: E731
        elif n == "cf_direct":
            headers_fn = kalshi_auth.cfb_direct_headers
        elif n == "coinbase":
            def backfill_fn(ad=ad):
                for asset in ad.assets:
                    url, params = ad.backfill_url(asset)
                    try:
                        body = getter.get_json(url, params)
                    except Exception as e:                        # noqa: BLE001 - backfill is best effort
                        col.on_poll_error("coinbase_backfill", clock.wall_ms(), str(e))
                        continue
                    import json as _json
                    col.on_rest("coinbase", "rest_backfill", _json.dumps(body),
                                lambda ctx, b=body, s=asset: ad.parse_backfill(s, b, ctx), clock.wall_ms(), clock.mono_ns())
        kw = {"connect_fn": connect_fn} if connect_fn else {}
        runners[n] = WsFeedRunner(ad, col, clock, headers_fn=headers_fn, backfill_fn=backfill_fn, stop_event=stop, **kw)

    def health():
        return {n: r.health.to_dict() for n, r in runners.items()}

    srv = None
    if a.status_port:
        from market_data.status_server import serve
        try:
            srv = serve(lambda: col.status(health()), a.status_port)
            print(f"research status page: http://127.0.0.1:{a.status_port}/  (read-only; no effect on calls)")
        except OSError as e:
            print(f"status page disabled: {e}")
    if install_signals:
        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
            if sig is not None:
                try:
                    signal.signal(sig, lambda *_: stop.set())
                except ValueError:
                    pass
    print(f"session {col.session_id} -> {col.dir}")
    for r in runners.values():
        r.start()
    t0 = time.monotonic()
    status = "COMPLETED"
    try:
        while not stop.is_set():
            if a.duration and time.monotonic() - t0 >= a.duration:
                break
            stop.wait(1.0)
            for r in runners.values():
                if hasattr(r, "monitor"):
                    r.monitor.evaluate(clock.mono_ns())
            col.tick()
    except KeyboardInterrupt:
        status = "ABORTED"
    finally:
        stop.set()
        for r in runners.values():
            r.join(timeout=5)
        col.close(status=status, health=health())
        if srv is not None:
            srv.shutdown()
    states = {n: h["state"] for n, h in health().items()}
    print(f"session {col.session_id} {status}: {col.counts} final states {states}")
    return 0


def main(argv=None):
    a = parse_args(argv)
    if a.dry_run:
        return dry_run(a)
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
