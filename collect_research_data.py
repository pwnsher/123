#!/usr/bin/env python3
"""
collect_research_data.py — Step-3 + Step-4 + Step-5 READ-ONLY research collection in ONE session (one arrival counter).

    py collect_research_data.py --dry-run                                        # plan + offline checks; no network
    py collect_research_data.py --assets BTC,ETH,SOL,XRP --all-research          # everything (Steps 3, 4 and 5)
    py collect_research_data.py --assets BTC,ETH,SOL,XRP --cf --coinbase --secondary --kalshi --perps --duration 3600
    py collect_research_data.py --assets BTC --coinbase --secondary --perps --perp-venues binance_usdm,bybit_linear
    py collect_research_data.py --assets BTC --micro --micro-venues coinbase_l2,binance_usdm_book --compression-level 9

Step-3 sources (flags as in collect_market_data.py): --cf [--cf-source kalshi|direct|both] --coinbase --secondary --kalshi
Step-4 sources: --perps [--perp-venues binance_usdm,bybit_linear,okx_swap,kalshi_perp] [--no-usdt] [--book-depth 5|10|20]
Step-5 sources: --micro [--micro-venues coinbase_l2,kraken_book,binance_usdm_book,bybit_linear_book,okx_swap_book,kalshi_ws]
    [--binance-book-depth 1000] [--kraken-book-depth 25] [--bybit-book-depth 200]
    storage: [--compression-level 1..9] [--segment-mb 64] [--segment-minutes N]   (retention: scripts/prune_sessions.py)
    With no source flag at all (or --all-research), everything is enabled. The USDT/USD rate (Coinbase USDT-USD) is
    collected with --perps unless --no-usdt, because USDT-quoted perps are converted before comparing with USD spot / CF.
    kalshi_ws needs the read-only Kalshi API key (as the CF-via-Kalshi feed); without it that source is disabled.

Output: <output>/<session_id>/ (Step-3 store + manifest), <output>/<session_id>/perp/ (derivatives) and
        <output>/<session_id>/micro/ (order books + Kalshi websocket; manifest with per-book reconstruction counters).
Then:   py scripts/build_research_dataset.py <output>/<session_id> --assets BTC --out analysis_output/research_btc
        py scripts/build_micro_dataset.py <output>/<session_id> --assets BTC --out analysis_output/joint_btc

READ ONLY: public (Kalshi: read-only authenticated) websockets and GET requests; no order endpoint exists in any
package; nothing here is imported by the production watcher, and the existing Kalshi-perp veto never sees this data.
"""
import argparse
import json
import os
import signal
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import collect_market_data as cmd                           # noqa: E402  (Step-3 source builder / credentials / code checks)
from market_data.clock import SystemClock                   # noqa: E402
from microstructure import APP_VERSION as MICRO_APP_VERSION, MICRO_FEATURE_SET_VERSION  # noqa: E402
from microstructure.venues import BOOK_VENUES, VENUES as BOOK_SPECS                     # noqa: E402
from perp_data import APP_VERSION, PERP_FEATURE_SET_VERSION  # noqa: E402
from perp_data.venues import PERP_VENUES, VENUES            # noqa: E402

ALL_ASSETS = cmd.ALL_ASSETS


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Read-only Step-3 + Step-4 research collection.")
    ap.add_argument("--assets", default=",".join(ALL_ASSETS))
    ap.add_argument("--duration", type=float, default=0, help="seconds (0 = until Ctrl+C)")
    ap.add_argument("--output", default=os.path.join(HERE, "market_data_sessions"))
    ap.add_argument("--cf", action="store_true")
    ap.add_argument("--cf-source", choices=("kalshi", "direct", "both"), default="kalshi")
    ap.add_argument("--coinbase", action="store_true")
    ap.add_argument("--secondary", action="store_true")
    ap.add_argument("--kalshi", action="store_true")
    ap.add_argument("--kalshi-interval", type=float, default=1.0)
    ap.add_argument("--perps", action="store_true")
    ap.add_argument("--perp-venues", default=",".join(PERP_VENUES))
    ap.add_argument("--no-usdt", action="store_true")
    ap.add_argument("--book-depth", type=int, default=10, choices=(5, 10, 20))
    ap.add_argument("--micro", action="store_true", help="Step-5 order books (+ Kalshi websocket)")
    ap.add_argument("--micro-venues", default=",".join(BOOK_VENUES))
    ap.add_argument("--binance-book-depth", type=int, default=1000, choices=(5, 10, 20, 50, 100, 500, 1000))
    ap.add_argument("--kraken-book-depth", type=int, default=25, choices=(10, 25, 100, 500, 1000))
    ap.add_argument("--bybit-book-depth", type=int, default=200, choices=(1, 50, 200, 500))
    ap.add_argument("--compression-level", type=int, default=6, choices=range(1, 10), metavar="1..9")
    ap.add_argument("--segment-mb", type=int, default=64)
    ap.add_argument("--segment-minutes", type=float, default=0, help="also rotate segments every N minutes (0 = size only)")
    ap.add_argument("--all-research", action="store_true", help="Steps 3, 4 and 5: every source")
    ap.add_argument("--status-port", type=int, default=8767)
    ap.add_argument("--settlement-store", default=None)
    ap.add_argument("--no-raw", action="store_true")
    ap.add_argument("--dry-run", "--check", action="store_true", dest="dry_run")
    a = ap.parse_args(argv)
    a.assets = [x.strip().upper() for x in a.assets.split(",") if x.strip()]
    bad = [x for x in a.assets if x not in ALL_ASSETS]
    if bad or not a.assets:
        ap.error(f"unknown assets {bad}; choose from {','.join(ALL_ASSETS)}")
    a.perp_venues = [x.strip() for x in a.perp_venues.split(",") if x.strip()]
    badv = [v for v in a.perp_venues if v not in PERP_VENUES]
    if badv:
        ap.error(f"unknown perp venues {badv}; choose from {','.join(PERP_VENUES)}")
    a.micro_venues = [x.strip() for x in a.micro_venues.split(",") if x.strip()]
    badm = [v for v in a.micro_venues if v not in BOOK_VENUES]
    if badm:
        ap.error(f"unknown book venues {badm}; choose from {','.join(BOOK_VENUES)}")
    if a.segment_mb <= 0 or a.segment_minutes < 0:
        ap.error("--segment-mb must be > 0 and --segment-minutes >= 0")
    if a.all_research or not (a.cf or a.coinbase or a.secondary or a.kalshi or a.perps or a.micro):
        a.cf = a.coinbase = a.secondary = a.kalshi = a.perps = a.micro = True
    a.no_live_features = True
    return a


def build_perp_adapters(a):
    from perp_data.sources.binance import BinanceUsdmAdapter
    from perp_data.sources.bybit import BybitLinearAdapter
    from perp_data.sources.kalshi_perp import KalshiPerpAdapter
    from perp_data.sources.okx import OkxSwapAdapter
    from perp_data.sources.stablecoin import CoinbaseUsdtAdapter
    if not a.perps:
        return {}
    mk = {"binance_usdm": lambda: BinanceUsdmAdapter(a.assets, depth=a.book_depth),
          "bybit_linear": lambda: BybitLinearAdapter(a.assets, depth=a.book_depth),
          "okx_swap": lambda: OkxSwapAdapter(a.assets, depth=5),
          "kalshi_perp": lambda: KalshiPerpAdapter(a.assets)}
    out = {v: mk[v]() for v in a.perp_venues}
    if not a.no_usdt:
        out["coinbase_usdt"] = CoinbaseUsdtAdapter()
    return out


def build_micro_adapters(a):
    from microstructure.sources.binance_depth import BinanceDepthAdapter
    from microstructure.sources.bybit_book import BybitBookAdapter
    from microstructure.sources.coinbase_l2 import CoinbaseL2Adapter
    from microstructure.sources.kalshi_ws import KalshiWsAdapter
    from microstructure.sources.kraken_book import KrakenBookAdapter
    from microstructure.sources.okx_books import OkxBooksAdapter
    if not a.micro:
        return {}
    mk = {"coinbase_l2": lambda: CoinbaseL2Adapter(a.assets),
          "kraken_book": lambda: KrakenBookAdapter(a.assets, depth=a.kraken_book_depth),
          "binance_usdm_book": lambda: BinanceDepthAdapter(a.assets, depth=a.binance_book_depth),
          "bybit_linear_book": lambda: BybitBookAdapter(a.assets, depth=a.bybit_book_depth),
          "okx_swap_book": lambda: OkxBooksAdapter(a.assets),
          "kalshi_ws": lambda: KalshiWsAdapter(a.assets)}
    return {v: mk[v]() for v in a.micro_venues}


def verify_micro_code():
    try:
        from microstructure import fingerprint as mf
        ok, problems = mf.verify()
        return {"ok": ok, "problems": problems[:5]}
    except Exception as e:                                   # noqa: BLE001
        return {"ok": False, "problems": [f"{type(e).__name__}: {e}"]}


def verify_perp_code():
    try:
        from perp_data import fingerprint as pf
        ok, problems = pf.verify()
        return {"ok": ok, "problems": problems[:5], "existing_perp_veto_fingerprint": pf.perp_veto_fingerprint()}
    except Exception as e:                                   # noqa: BLE001
        return {"ok": False, "problems": [f"{type(e).__name__}: {e}"]}


def dry_run(a):
    step3 = a.cf or a.coinbase or a.secondary or a.kalshi
    rc = 0
    if step3:
        rc = cmd.dry_run(a)                                   # prints the Step-3 plan + strategy/settlement/market-data checks
    else:
        print("Step-3 sources: none (perp / micro features that need the USD spot reference, CF or spot trades will be MISSING)")
    print(f"\ncollect_research_data {APP_VERSION}  perp feature set {PERP_FEATURE_SET_VERSION}")
    ads = build_perp_adapters(a)
    for n, ad in ads.items():
        sp = VENUES.get(n)
        where = getattr(ad, "url", None) or "REST only (GET)"
        print(f"  perp source {n:14s} transport={ad.transport:4s} {where}")
        if sp is not None and sp.symbols:
            print(f"      symbols: {', '.join(sp.symbols[x] for x in ad.assets)}   quote={sp.quote_ccy} size_unit={sp.size_unit}")
            print(f"      trade side: {sp.trade_side_semantics}; liquidation side: {sp.liquidation_side_semantics} "
                  f"({sp.liquidation_sampling}); funding interval: {sp.funding_interval_source}")
        elif n == "kalshi_perp":
            ov = {x: os.environ.get(f"KALSHI_PERP_TICKER_{x}") for x in ad.assets}
            print(f"      tickers discovered from /margin/markets at run time; overrides: "
                  f"{ {k: v for k, v in ov.items() if v} or 'none'}")
        for m in ad.subscribe_messages():
            print(f"      subscribe: {m[:220]}")
        for s, (url, params, iv) in ad.rest_urls().items():
            print(f"      GET every {iv:>4}s {url} {params or ''}  [{s}]")
    if ads:
        print("  credentials required for perp sources: none (public market data only)")
    pv = verify_perp_code()
    print(f"code verification perp_data  : {'MATCHES' if pv['ok'] else 'MISMATCH'}" + ("" if pv["ok"] else f"  {pv['problems']}"))
    print(f"existing perp-veto fingerprint: {pv.get('existing_perp_veto_fingerprint')}")
    mads = build_micro_adapters(a)
    mv = {"ok": True}
    if mads:
        print(f"\nmicrostructure {MICRO_APP_VERSION}  micro feature set {MICRO_FEATURE_SET_VERSION}")
        for n, ad in mads.items():
            sp = BOOK_SPECS[n]
            print(f"  book source {n:18s} {sp.book_classification} ({sp.level_type}); sequence policy {sp.sequence_policy}")
            print(f"      {ad.url}   channel: {sp.channel}; depth: {ad.depth or 'full diff stream'}; resnapshot: {sp.resnapshot}")
            if sp.symbols:
                print(f"      symbols: {', '.join(sp.symbols[x] for x in ad.assets)}   price {sp.price_unit}, size {sp.qty_unit}")
            else:
                print("      markets: the open 15-minute markets of each series, discovered by GET /markets every 30 s")
            for m in ad.subscribe_messages():
                print(f"      subscribe: {m[:200]}")
            if getattr(ad, "rest_snapshots", False):
                url, params = ad.snapshot_url(ad.assets[0])
                print(f"      GET {url} {params} (on connect / after a sequence gap; <= 1 per second per book)")
            if sp.limitations:
                print(f"      limitation: {sp.limitations}")
        needs = [n for n, ad in mads.items() if getattr(ad, "auth_required", False)]
        print(f"  credentials: {'read-only Kalshi API key (as the CF-via-Kalshi feed) for ' + ', '.join(needs) if needs else 'none'}"
              f"; configured: {'yes' if cmd.credentials().get('kalshi_ws_auth_configured') else 'no (kalshi_ws will be disabled)' if needs else '-'}")
        print(f"  storage: gzip level {a.compression_level}, segments {a.segment_mb} MiB"
              + (f" or {a.segment_minutes} min" if a.segment_minutes else "") + "; nothing is dropped silently "
              "(overloads / truncations are counted in micro/manifest.json); retention: scripts/prune_sessions.py")
        print("  storage estimate: see analysis_output/microstructure_performance.json (bytes/hour from the benchmark)")
        mv = verify_micro_code()
        print(f"code verification microstructure: {'MATCHES' if mv['ok'] else 'MISMATCH'}" + ("" if mv["ok"] else f"  {mv['problems']}"))
    print("dry run: no network connection was made, no market data was written, no order can be placed.")
    return rc if (rc or not pv["ok"] or not mv["ok"]) else 0


def run(a, clock=None, stop_event=None, getter=None, connect_fn=None, install_signals=True):
    from market_data.collector import Collector
    from market_data.kalshi_poller import KalshiPoller
    from market_data.runner import PollRunner, WsFeedRunner
    from market_data.transport import kalshi_auth
    from market_data.transport.http import HttpGetter
    from perp_data.collector import PerpCollector
    from perp_data.poller import PerpPoller, backfill
    from perp_data.transport import connect_fn_for
    from microstructure.collector import MicroCollector
    from microstructure.poller import SnapshotPoller
    from microstructure.sources.kalshi_ws import WS_PATH as KALSHI_WS_PATH
    from microstructure.transport import connect_fn_for as micro_connect_fn_for

    clock = clock or SystemClock()
    stop = stop_event or threading.Event()
    getter = getter or HttpGetter()
    srcs = cmd.build_sources(a) if (a.cf or a.coinbase or a.secondary or a.kalshi) else {}
    ads = build_perp_adapters(a)
    mads = build_micro_adapters(a)
    meta = {n: {"enabled": True, "family": "spot/cf/kalshi", "critical": ad.critical, "transport": ad.transport}
            for n, ad in srcs.items()}
    meta.update({n: {"enabled": True, "family": "perp", "transport": ad.transport} for n, ad in ads.items()})
    meta.update({n: {"enabled": True, "family": "micro", "transport": ad.transport} for n, ad in mads.items()})
    meta["_credentials"] = cmd.credentials()
    col = Collector(a.output, a.assets, meta, clock, keep_raw=not a.no_raw, live_features=False)
    store = a.settlement_store
    if store is None and srcs:
        store = os.path.join(HERE, "settlement_data", f"capture-{col.session_id}.jsonl")
    if store and store.lower() != "none":
        from settlement.cache import SettlementStore
        col.settlement_store = SettlementStore(store)
        col.manifest.settlement_store = store
    col.manifest.code_verification = dict(cmd.verify_code(), perp_data=verify_perp_code(),
                                          **({"microstructure": verify_micro_code()} if mads else {}))
    col.manifest.notes.append(f"derivatives store: {os.path.join(col.dir, 'perp')}")
    if mads:
        col.manifest.notes.append(f"microstructure store: {os.path.join(col.dir, 'micro')}")
    col.manifest.write(col.dir)
    pcol = PerpCollector(a.output, a.assets, ads, clock, session_id=col.session_id, seq=col.seq, keep_raw=not a.no_raw,
                         step3_session_dir=col.dir) if ads else None
    mcol = MicroCollector(a.output, a.assets, mads, clock, session_id=col.session_id, seq=col.seq, keep_raw=not a.no_raw,
                          step3_session_dir=col.dir, compression_level=a.compression_level,
                          segment_max_bytes=a.segment_mb << 20,
                          segment_max_s=(a.segment_minutes * 60) if a.segment_minutes else None) if mads else None

    runners = {}
    for n, ad in srcs.items():
        if n == "kalshi":
            runners[n] = PollRunner("kalshi", KalshiPoller(ad, getter, col, clock).poll, col, clock,
                                    interval_s=a.kalshi_interval, stop_event=stop)
            continue
        headers_fn = backfill_fn = None
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
                    except Exception as e:                   # noqa: BLE001
                        col.on_poll_error("coinbase_backfill", clock.wall_ms(), str(e))
                        continue
                    col.on_rest("coinbase", "rest_backfill", json.dumps(body),
                                lambda ctx, b=body, s=asset: ad.parse_backfill(s, b, ctx), clock.wall_ms(), clock.mono_ns())
        kw = {"connect_fn": connect_fn} if connect_fn else {}
        runners[n] = WsFeedRunner(ad, col, clock, headers_fn=headers_fn, backfill_fn=backfill_fn, stop_event=stop, **kw)
    for n, ad in ads.items():
        if ad.transport == "ws":
            runners[f"perp:{n}"] = WsFeedRunner(ad, pcol, clock, stop_event=stop,
                                                connect_fn=connect_fn_for(ad, clock, connect_fn),
                                                backfill_fn=(lambda ad=ad: backfill(ad, getter, pcol, clock)))
        if ad.rest_urls() or ad.transport == "rest":
            runners[f"perp-rest:{n}"] = PollRunner(f"perp-rest:{n}", PerpPoller(ad, getter, pcol, clock).poll, pcol, clock,
                                                   interval_s=1.0, stop_event=stop, critical=False)

    for n, ad in mads.items():
        hf = None
        if getattr(ad, "auth_required", False):
            hf = (lambda: kalshi_auth.ws_headers(KALSHI_WS_PATH, clock.wall_ms()))  # noqa: E731
        runners[f"micro:{n}"] = WsFeedRunner(ad, mcol, clock, headers_fn=hf, stop_event=stop,
                                             connect_fn=micro_connect_fn_for(ad, clock, connect_fn))
        if getattr(ad, "rest_snapshots", False):
            runners[f"micro-snap:{n}"] = PollRunner(f"micro-snap:{n}", SnapshotPoller(ad, getter, mcol, clock).poll, mcol, clock,
                                                    interval_s=0.25, stop_event=stop, critical=False)
        if ad.rest_urls():
            runners[f"micro-rest:{n}"] = PollRunner(f"micro-rest:{n}", PerpPoller(ad, getter, mcol, clock).poll, mcol, clock,
                                                    interval_s=1.0, stop_event=stop, critical=False)

    def health():
        return {n: r.health.to_dict() for n, r in runners.items()}

    srv = None
    if a.status_port:
        from market_data.status_server import serve
        try:
            srv = serve(lambda: dict(col.status({k: v for k, v in health().items()}),
                                     perp=({"counts": pcol.counts, "by_type": pcol.by_type} if pcol else None),
                                     micro=({"counts": mcol.counts, "books": mcol.book_status()} if mcol else None)), a.status_port)
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
    print(f"session {col.session_id} -> {col.dir}" + (" (+ perp/)" if pcol else "") + (" (+ micro/)" if mcol else ""))
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
                r.monitor.evaluate(clock.mono_ns())
            col.tick()
            if pcol:
                pcol.tick()
            if mcol:
                mcol.tick()
    except KeyboardInterrupt:
        status = "ABORTED"
    finally:
        stop.set()
        for r in runners.values():
            r.join(timeout=5)
        h = health()
        col.close(status=status, health={k: v for k, v in h.items() if not k.startswith(("perp", "micro"))})
        if pcol:
            pcol.close(status=status, health={k: v for k, v in h.items() if k.startswith("perp")})
        if mcol:
            mcol.close(status=status, health={k: v for k, v in h.items() if k.startswith("micro")})
        if srv is not None:
            srv.shutdown()
    print(f"session {col.session_id} {status}: step3 {col.counts}" + (f" perp {pcol.counts}" if pcol else "")
          + (f" micro {mcol.counts}" if mcol else ""))
    return 0


def main(argv=None):
    a = parse_args(argv)
    return dry_run(a) if a.dry_run else run(a)


if __name__ == "__main__":
    sys.exit(main())
