#!/usr/bin/env python3
"""
run_local.py — LOCAL-FIRST entry point. No Discord, no credentials, no orders.

    py run_local.py                  # watcher + local dashboard  http://127.0.0.1:8000
    py run_local.py --check          # offline self-check (config, fingerprints, fixtures); no network
    py run_local.py --port 8010      # dashboard on another port
    py run_local.py --no-web         # watcher only
    py run_local.py --journal decisions.jsonl   # also journal every SignalDecision (JSON lines)

It starts EXACTLY what `py kalshi_dashboard.py` starts (live-veto banner, poller, backtest worker,
weekly worker, localhost web server) with the unmodified strategy, and adds only:
  * structured logging (kalshi_core.logging_setup) with secret redaction;
  * the two notification hooks the legacy Discord bot uses (ON_CALL, POST_EMBED) are pointed at
    the local log instead of Discord: a call is logged, never posted anywhere;
  * a read-only observer that turns each cycle's legacy results into SignalDecision records and
    logs NO_CALL-reason / feed-health CHANGES (per-cycle detail only at DEBUG);
  * graceful shutdown on Ctrl+C / SIGTERM.
Nothing here reads or changes a strategy value, and nothing here can place an order.

Environment (all optional; see .env.example): KALSHI_LOG_LEVEL, KALSHI_LOG_FORMAT (text|json),
KALSHI_LOG_FILE, KALSHI_DECISION_JOURNAL, KALSHI_DASHBOARD_PORT, plus the legacy perp variables.
A .env file next to this script is read if present (never overriding the shell, and never for
PERP_LIVE_VETO_ENABLED, which must be set deliberately in the shell).
"""
import argparse
import logging
import os
import signal
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from kalshi_core import config as kc                     # noqa: E402  (no strategy import yet)
from kalshi_core import logging_setup as klog            # noqa: E402

OBSERVER_INTERVAL_S = 0.5


class LocalApp:
    """Owns the legacy threads + observer for one process. Never raises into the watcher."""

    def __init__(self, k, cfg, fingerprints=None, logger=None):
        self.k = k
        self.cfg = cfg
        self.fp = fingerprints or {}
        self.log = logger or klog.get_logger("app")
        self.slog = klog.get_logger("signal")
        self.flog = klog.get_logger("feed")
        self.server = None
        self.threads = {}
        self._stop = threading.Event()
        self._last_updated = None
        self._last_decision = {}
        self._last_health = {}
        self._journal_lock = threading.Lock()

    # ---------- hooks (called from the legacy poller thread; must never raise) ----------
    def on_call(self, coin, r, crec):
        try:
            from kalshi_core.adapter import from_evaluation
            sd = from_evaluation(r, coin=coin, series=(self.k.COINS.get(coin) or {}).get("series"),
                                 strategy_fingerprint=self.fp.get("legacy"))
            klog.log_event(self.slog, "signal_generated", coin=coin, ticker=sd.market_ticker,
                           side=sd.side.value if sd.side else None, p_up=sd.raw_probability_up,
                           conf=sd.confidence_pct, side_ask=sd.side_ask, net_edge=sd.net_edge_cents,
                           rec_stop=sd.recommended_stop_cents, minutes_left=sd.time_remaining_min,
                           record_w=(crec or {}).get("w"), record_n=(crec or {}).get("n"))
        except Exception as e:                                   # logging must never block a call
            klog.log_event(self.log, "hook_error", logging.ERROR, hook="on_call", error=str(e)[:200])

    def on_embed(self, dest, embed):
        try:
            klog.log_event(self.log, "notification", dest=dest, title=(embed or {}).get("title"))
        except Exception:
            pass

    # ---------- observer (read-only view of STATE) ----------
    def observe_once(self):
        k = self.k
        with k.LOCK:
            updated = k.STATE.get("updated")
            coins = dict(k.STATE.get("coins") or {})
            running = k.STATE.get("running")
        if not updated or updated == self._last_updated:
            return 0
        self._last_updated = updated
        from kalshi_core.adapter import from_evaluation
        from kalshi_core.data_health import overall
        n = 0
        for coin, r in sorted(coins.items()):
            sd = from_evaluation(r, coin=coin, series=(k.COINS.get(coin) or {}).get("series"),
                                 strategy_fingerprint=self.fp.get("legacy"))
            n += 1
            reasons = [x.value for x in sd.no_call_reasons]
            key = (sd.market_ticker, sd.decision.value, tuple(reasons))
            fields = dict(coin=coin, ticker=sd.market_ticker, decision=sd.decision.value, reasons=reasons,
                          legacy_reason=sd.legacy_reason, conf=sd.confidence_pct, minutes_left=sd.time_remaining_min)
            if key != self._last_decision.get(coin):
                self._last_decision[coin] = key
                if sd.decision.value == "NO_CALL":
                    klog.log_event(self.slog, "no_call", **fields)
                else:
                    klog.log_event(self.slog, "decision", logging.DEBUG, **fields)
            else:
                klog.log_event(self.slog, "decision", logging.DEBUG, **fields)
            h = overall(sd.data_health).value
            if h != self._last_health.get(coin):
                prev = self._last_health.get(coin)
                self._last_health[coin] = h
                lvl = logging.INFO if h == "HEALTHY" else logging.WARNING
                ev = "feed_recovered" if (h == "HEALTHY" and prev) else ("feed_status" if h == "HEALTHY" else "feed_degraded")
                klog.log_event(self.flog, ev, lvl, coin=coin, status=h, previous=prev,
                               legacy_status=sd.legacy_status,
                               detail=[x.detail for x in sd.data_health if x.detail][:2])
            if str(sd.legacy_status or "").startswith("error"):
                klog.log_event(self.log, "evaluation_error", logging.WARNING, coin=coin, status=sd.legacy_status)
            if self.cfg.decision_journal:
                self._journal(sd)
        klog.log_event(self.log, "cycle", logging.DEBUG, updated=updated, coins=n, running=running)
        return n

    def _journal(self, sd):
        try:
            with self._journal_lock, open(self.cfg.decision_journal, "a", encoding="utf-8") as f:
                f.write(sd.to_json() + "\n")
        except (OSError, ValueError) as e:
            klog.log_event(self.log, "journal_error", logging.ERROR, error=str(e)[:200])

    def _observer(self):
        while not self._stop.is_set():
            try:
                self.observe_once()
            except Exception as e:
                klog.log_event(self.log, "observer_error", logging.ERROR, error=str(e)[:200])
            self._stop.wait(OBSERVER_INTERVAL_S)

    # ---------- lifecycle ----------
    def start(self, web=True):
        k = self.k
        k.ON_CALL = self.on_call
        k.POST_EMBED = self.on_embed
        k._gate_banner()                                          # same first step as kalshi_dashboard.main()
        gate = dict(k.STATE.get("perp_live_veto") or {})
        klog.log_event(self.log, "model_initialized", model_id=self.fp.get("model_id"),
                       legacy_strategy_fingerprint=self.fp.get("legacy"),
                       extended_strategy_fingerprint=self.fp.get("extended"),
                       perp_live_veto=gate.get("status"), perp_telemetry=k.PERP_TELEMETRY_ENABLED,
                       perp_shadow=k.PERP_SHADOW_ENABLED)
        for name, target in (("poller", k.poller), ("backtest", k.backtest_worker), ("weekly", k.weekly_worker)):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self.threads[name] = t
        if web:
            from http.server import ThreadingHTTPServer
            try:
                self.server = ThreadingHTTPServer(("127.0.0.1", self.cfg.dashboard_port), k.Handler)
                t = threading.Thread(target=self.server.serve_forever, name="web", daemon=True)
                t.start()
                self.threads["web"] = t
                klog.log_event(self.log, "dashboard_started", url=f"http://127.0.0.1:{self.server.server_address[1]}")
            except OSError as e:
                klog.log_event(self.log, "dashboard_not_started", logging.ERROR, port=self.cfg.dashboard_port,
                               error=str(e)[:200])
        t = threading.Thread(target=self._observer, name="observer", daemon=True)
        t.start()
        self.threads["observer"] = t
        return self

    def stop(self):
        self._stop.set()
        if self.server is not None:
            try:
                self.server.shutdown()
                self.server.server_close()
            except Exception:
                pass
        k = self.k
        if k.ON_CALL == self.on_call:
            k.ON_CALL = None
        if k.POST_EMBED == self.on_embed:
            k.POST_EMBED = None
        klog.log_event(self.log, "shutdown", reason="stop requested")


def _install_exception_hooks(log):
    def _sys_hook(tp, val, tb):
        klog.log_event(log, "unexpected_exception", logging.CRITICAL, exc_info=(tp, val, tb), where="main")

    def _thread_hook(a):
        if a.exc_type is SystemExit:
            return
        klog.log_event(log, "unexpected_exception", logging.CRITICAL,
                       exc_info=(a.exc_type, a.exc_value, a.exc_traceback),
                       where=getattr(a.thread, "name", "thread"))
    sys.excepthook = _sys_hook
    threading.excepthook = _thread_hook


def self_check(k, log):
    """Offline checks. Returns (ok, fingerprints)."""
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    m = baseline.build_manifest()
    fps = {"legacy": m["fingerprints"]["legacy_strategy_fingerprint"],
           "extended": m["fingerprints"]["extended_strategy_fingerprint"], "model_id": m["strategy"]["model_id"]}
    klog.log_event(log, "strategy_fingerprint", logging.INFO if ok else logging.ERROR,
                   status="MATCHES" if ok else "MISMATCH", legacy=fps["legacy"], extended=fps["extended"],
                   problems=problems[:10])
    return ok, fps


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Run the Kalshi 15m watcher locally (no Discord, no orders).")
    ap.add_argument("--check", action="store_true", help="offline self-check, then exit")
    ap.add_argument("--port", type=int, default=None, help="dashboard port (default 8000 / KALSHI_DASHBOARD_PORT)")
    ap.add_argument("--no-web", action="store_true", help="do not start the local web dashboard")
    ap.add_argument("--journal", default=None, help="append every SignalDecision to this JSON-lines file")
    ap.add_argument("--log-level", default=None)
    ap.add_argument("--log-format", choices=("text", "json"), default=None)
    ap.add_argument("--env-file", default=os.path.join(HERE, ".env"), help="optional .env file (names in .env.example)")
    return ap.parse_args(argv)


def main(argv=None):
    a = parse_args(argv)
    loaded, refused = kc.load_dotenv(a.env_file)          # BEFORE the legacy module reads os.environ
    base = kc.InfraConfig.from_env()
    cfg = kc.InfraConfig(log_level=(a.log_level or base.log_level).upper(), log_format=a.log_format or base.log_format,
                         log_file=base.log_file, decision_journal=a.journal if a.journal is not None else base.decision_journal,
                         dashboard_port=a.port or base.dashboard_port, open_web=not a.no_web)
    klog.configure(cfg.log_level, cfg.log_format, cfg.log_file)
    log = klog.get_logger("app")
    _install_exception_hooks(log)
    klog.log_event(log, "startup", mode="check" if a.check else "local", python=sys.version.split()[0],
                   pid=os.getpid(), cwd=os.getcwd(), discord="not loaded (legacy adapter: kalshi_bot.py)",
                   live_order_execution="not available")
    if refused:
        klog.log_event(log, "dotenv_refused", logging.WARNING, names=refused,
                       why="safety activation variables must be set in the shell, not a file")
    import kalshi_dashboard as k
    klog.log_event(log, "config_loaded", infra=cfg.to_dict(), dotenv_loaded=loaded, secrets=kc.describe_secrets(),
                   strategy=kc.strategy_config_snapshot(k), perp_telemetry=k.PERP_TELEMETRY_ENABLED,
                   perp_shadow=k.PERP_SHADOW_ENABLED, perp_live_veto_env=k.PERP_LIVE_VETO_ENABLED)
    ok, fps = self_check(k, log)
    if a.check:
        print("LOCAL SELF-CHECK: " + ("OK" if ok else "STRATEGY BASELINE MISMATCH (see log)"))
        return 0 if ok else 3
    if not ok:
        klog.log_event(log, "strategy_baseline_warning", logging.ERROR,
                       msg="running code differs from config/strategy_baseline.json; results are NOT the baseline")
    app = LocalApp(k, cfg, fps, log).start(web=cfg.open_web)
    stop = threading.Event()

    def _request_stop(signum, _frame):
        klog.log_event(log, "signal_received", signum=signum)
        stop.set()
    for name in ("SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            try:
                signal.signal(getattr(signal, name), _request_stop)
            except (ValueError, OSError):
                pass
    try:
        while not stop.wait(0.5):
            pass
    except KeyboardInterrupt:
        klog.log_event(log, "signal_received", signum="SIGINT")
    finally:
        app.stop()
        logging.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
