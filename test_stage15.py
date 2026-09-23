#!/usr/bin/env python3
"""Stage 15 tests: DASHBOARD REAL-TIME RESPONSIVENESS (presentation layer only). No network.
Run:  py test_stage15.py        Every patch is restored afterwards.

The page script is executed for real in node (when node is on PATH) against a fake DOM, a fake
Lightweight-Charts, a fake clock/timers and a fake fetch, so countdown, /data loop, live chart
bars, reconciliation, chart-type switch, strike line and rollover are tested behaviourally.
Without node those behavioural checks are reported as SKIPPED; every Python-side check runs.
"""
import ast
import hashlib
import http.client
import inspect
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from http.server import ThreadingHTTPServer

import kalshi_dashboard as k
import strategy_fingerprint as sf
import test_stage7 as t7

HERE = os.path.dirname(os.path.abspath(__file__))
TRUSTED_V2 = "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"   # == test_stage12/13/14
# live-veto / telemetry modules are NOT touched by this pass (sha256 of the stage-14 release)
UNCHANGED_FILES = {
    "perp_live.py": "95b0ce6f1f6ce70b62cae1176757dbb27574501817a0d6c868fec4bbd7ba48a2",
    "perp_probability.py": "344114534aef2a90ba41b07a52a454bd1e5ebc3b06b4b2facebde5c90eb76546",
    "perp_shadow.py": "ba3b3444e83fe384f5556050abc7d543d9c06fbc147a51319a314dddf12794c3",
    "perp_telemetry.py": "5e1de6d6553032bd2902d0b9766086b826876c632bb41cfcfe52234c90b4d99e",
}
NODE = shutil.which("node")
JS = {}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def page_script():
    """The inline <script> of the page (the chart library <script src> is not included)."""
    start = k.PAGE.index("<script>\nconst COINS")
    return k.PAGE[start + len("<script>"):k.PAGE.rindex("</script>")]


# ═══════════════════ node harness: fake DOM / charts / clock / fetch ═══════════════════
HARNESS = r"""
const vm = require("vm");
const realImmediate = setImmediate;
const out = {};
const T0 = Date.parse("2026-09-01T12:00:10Z");
let NOW = T0;
Date.now = () => NOW;
// ---------- fake timers (driven by advance) ----------
let timers = [], tid = 0;
globalThis.setTimeout = (fn, ms) => { const t = {id: ++tid, at: NOW + (ms || 0), fn, every: null, name: fn.name}; timers.push(t); return t.id; };
globalThis.setInterval = (fn, ms) => { const t = {id: ++tid, at: NOW + ms, fn, every: ms, name: fn.name}; timers.push(t); return t.id; };
globalThis.clearTimeout = globalThis.clearInterval = id => { timers = timers.filter(t => t.id !== id); };
const flush = async () => { for (let i = 0; i < 6; i++) await new Promise(r => realImmediate(r)); };
async function advance(ms, step = 50) {
  const end = NOW + ms;
  while (true) {
    await flush();
    const due = timers.filter(t => t.at <= Math.min(end, NOW + step)).sort((a, b) => a.at - b.at || a.id - b.id)[0];
    if (due) {
      NOW = Math.max(NOW, due.at);
      if (due.every) due.at += due.every; else timers = timers.filter(t => t !== due);
      due.fn();
      continue;
    }
    if (NOW >= end) break;
    NOW = Math.min(end, NOW + step);
  }
  await flush();
}
// ---------- fake fetch ----------
const log = []; let inflight = 0, maxInflight = 0;
let DATA = {}, BARS = {}, DATA_MODE = "ok";
const hung = [];
globalThis.AbortController = class { constructor() { this.signal = {aborted: false, fns: []}; }
  abort() { this.signal.aborted = true; this.signal.fns.forEach(f => f()); } };
globalThis.fetch = (url, opts = {}) => {
  log.push({url, t: NOW, cache: opts.cache || null});
  const isData = url === "/data";
  if (isData) { inflight++; maxInflight = Math.max(maxInflight, inflight); }
  const done = v => { if (isData) inflight--; return v; };
  const resp = body => ({json: async () => JSON.parse(JSON.stringify(body))});
  return new Promise((resolve, reject) => {
    if (isData && DATA_MODE === "fail") { done(); return reject(new Error("net")); }
    if (isData && DATA_MODE === "hang") {
      const sig = opts.signal;
      if (sig) sig.fns.push(() => { done(); reject(new Error("aborted")); });
      hung.push(1); return;
    }
    if (isData) return resolve(done(resp(DATA)));
    if (url.startsWith("/candles?coin=")) return resolve(resp(BARS[url.split("=")[1]] || []));
    if (url === "/ping") return resolve(resp({t: NOW}));
    reject(new Error("unexpected url " + url));
  });
};
// ---------- fake DOM ----------
const els = {}, listeners = {};
function mkEl(id) { return {id, textContent: "", className: "", title: "", innerHTML: "", value: "", style: {}, dataset: {},
  classList: {toggle() {}, contains() { return false; }}, appendChild() {}, querySelectorAll() { return []; }, onclick: null}; }
globalThis.document = {
  getElementById: id => (els[id] = els[id] || mkEl(id)), createElement: () => mkEl(null),
  querySelectorAll: () => [], addEventListener: (ev, fn) => { (listeners[ev] = listeners[ev] || []).push(fn); },
  activeElement: null, hidden: false};
globalThis.window = globalThis;
globalThis.performance = globalThis.performance || {now: () => NOW};
// ---------- fake Lightweight Charts (update() rejects writes behind the newest bar, like v4) ----------
const allSeries = [];
function mkSeries(kind) {
  const s = {kind, calls: [], lines: [], removedLines: [], last: null, removed: false,
    setData(d) { this.calls.push({op: "setData", n: d.length, first: d[0], lastBar: d[d.length - 1]});
      this.last = d.length ? d[d.length - 1].time : null; },
    update(b) { if (this.last != null && b.time < this.last) throw new Error("Cannot update oldest data");
      this.calls.push({op: "update", bar: b}); this.last = b.time; },
    createPriceLine(o) { const h = {price: o.price}; this.lines.push(h); return h; },
    removePriceLine(h) { this.removedLines.push(h.price); }};
  allSeries.push(s); return s;
}
globalThis.LightweightCharts = {createChart: () => ({
  addLineSeries: () => mkSeries("line"), addCandlestickSeries: () => mkSeries("cand"),
  removeSeries(s) { s.removed = true; }})};
// ---------- helpers ----------
const run = code => vm.runInThisContext(code);
const S = c => run(`series[${JSON.stringify(c)}]`);
const txt = id => document.getElementById(id).textContent;
const cls = id => document.getElementById(id).className;
const count = pfx => log.filter(l => l.url.startsWith(pfx)).length;
const iso = ms => new Date(ms).toISOString().replace(".000Z", "Z");
const minute = ms => Math.floor(ms / 60000) * 60;
function bars(endMin, n, base) { const o = []; for (let i = n - 1; i >= 0; i--) { const t = endMin - 60 * i;
  o.push({t, o: base, h: base + 2, l: base - 2, c: base + 1}); } return o; }
function coin(c, extra) { return Object.assign({coin: c, status: "ok", ticker: "KX" + c + "15M-A", spot: 100,
  spot_observed_ts: NOW / 1000 - 1, strike: 100, dist: 0, close: iso(T0 + 350000), conf: 90, raw_edge: 1,
  net_edge: 1, up_bid: 1, up_ask: 2, dn_bid: 1, dn_ask: 2, rec_stop: 1, signal: false, reason: "WAIT", verdict: "w"}, extra); }

(async () => {
  const m0 = minute(T0);
  ["BTC", "ETH", "SOL", "XRP"].forEach(c => BARS[c] = bars(m0, 60, 100));
  DATA = {updated: "12:00:10", coins: {BTC: coin("BTC", {spot: 101, spot_observed_ts: T0 / 1000 - 0.8}),
          ETH: coin("ETH", {close: "not a date"}), SOL: {coin: "SOL", status: "stale", spot: 50, spot_observed_ts: T0 / 1000 - 7}}};
  run(process.env.PAGE_JS);                                          // boot the real page script
  await advance(0);
  out.consts = run("({DATA_REFRESH_MS, COUNTDOWN_MS, CANDLE_RECONCILE_MS, DATA_TIMEOUT_MS, AGE_WARN_S, AGE_STALE_S})");
  out.boot = {data: count("/data"), candles: count("/candles"), ping: count("/ping"),
              cd: {BTC: txt("cd_BTC"), ETH: txt("cd_ETH"), SOL: txt("cd_SOL"), XRP: txt("cd_XRP")},
              link: txt("link"), dataNoStore: log.filter(l => l.url === "/data").every(l => l.cache === "no-store"),
              candleNoStore: log.filter(l => l.url.startsWith("/candles")).every(l => l.cache === "no-store"),
              urls: [...new Set(log.map(l => l.url.split("?")[0]))]};
  out.intervals = timers.filter(t => t.every).map(t => [t.every, t.name]);
  // ---- countdown: smooth local clock, no requests of its own ----
  const seq = []; const f0 = log.length;
  for (let i = 0; i < 12; i++) { seq.push(txt("cd_BTC")); await advance(250); }
  out.countdownSeq = seq;
  const before = log.length; for (let i = 0; i < 200; i++) run("updateCountdowns()");
  out.countdownRequests = log.length - before;
  out.dataCalls3s = log.slice(f0).filter(l => l.url === "/data").length;
  // ---- data age classes ----
  DATA.coins.BTC.spot_observed_ts = NOW / 1000 - 2; DATA.coins.SOL.spot_observed_ts = NOW / 1000 - 6;
  await advance(1000);                                               // read ~1 s later: ages ~3 s and ~7 s
  out.ageNormal = [txt("age_BTC"), cls("age_BTC")];
  out.ageAmber = [txt("age_SOL"), cls("age_SOL")];
  await advance(5000);                                               // no new SOL observation: it ages past 10 s
  out.ageRed = [txt("age_SOL"), cls("age_SOL")];
  // ---- refresh rate over 10 s, and no history reload in the meantime ----
  let d0 = count("/data"), c0 = count("/candles"); const setData0 = S("BTC").calls.filter(x => x.op === "setData").length;
  await advance(10000);
  out.dataPer10s = count("/data") - d0;
  out.candlesIn10s = count("/candles") - c0;
  out.setDataIn10s = S("BTC").calls.filter(x => x.op === "setData").length - setData0;
  // ---- line chart: new spot observations patch the newest point with update() ----
  const lineBefore = S("BTC").calls.length;
  for (const px of [101.5, 102.25, 101.75]) { DATA.coins.BTC.spot = px; DATA.coins.BTC.spot_observed_ts = NOW / 1000 - 0.2; await advance(1000); }
  out.lineCalls = S("BTC").calls.slice(lineBefore);
  out.lineMinute = minute(NOW - 200);
  // ---- chart type switch -> candles: redraw from cached history (no new fetch), live bar re-applied ----
  const cBefore = count("/candles"); const oldSeries = S("BTC");
  run('setKind("BTC","cand")');
  await advance(0);
  const cs = S("BTC");
  out.switchCand = {kind: cs.kind, oldRemoved: oldSeries.removed, candlesFetched: count("/candles") - cBefore,
                    ops: cs.calls.map(x => x.op), lines: cs.lines.map(h => h.price)};
  // ---- candlestick H/L/C inside one minute ----
  const sameMin = minute(NOW);
  const startOfNext = (sameMin + 60) * 1000;
  NOW = Math.max(NOW, sameMin * 1000 + 1000);
  for (const px of [100.5, 104.0, 97.5, 101.0]) { DATA.coins.BTC.spot = px; DATA.coins.BTC.spot_observed_ts = NOW / 1000; await advance(1000); }
  out.candleSame = {minute: sameMin, last: cs.calls[cs.calls.length - 1]};
  // ---- minute rollover starts a new bar ----
  NOW = startOfNext + 500;
  DATA.coins.BTC.spot = 102.0; DATA.coins.BTC.spot_observed_ts = NOW / 1000; await advance(1000);
  const afterRoll = cs.calls[cs.calls.length - 1];
  DATA.coins.BTC.spot = 103.5; DATA.coins.BTC.spot_observed_ts = NOW / 1000; await advance(1000);
  out.rollover = {minute: sameMin + 60, first: afterRoll, second: cs.calls[cs.calls.length - 1]};
  out.updateTimesMonotone = cs.calls.filter(x => x.op === "update").every((x, i, a) => i === 0 || x.bar.time >= a[i - 1].bar.time);
  // ---- periodic reconciliation with /candles (authoritative bar replaces sampled H/L) ----
  const live = sameMin + 60;
  BARS.BTC = bars(live, 60, 100).map(b => b.t === live ? {t: live, o: 101.9, h: 109.0, l: 99.0, c: 103.0} : b);
  const rc0 = count("/candles?coin=BTC"), sd0 = cs.calls.filter(x => x.op === "setData").length;
  await advance(61000);
  out.reconcile = {fetches: count("/candles?coin=BTC") - rc0,
                   setData: cs.calls.filter(x => x.op === "setData").length - sd0,
                   last: cs.calls[cs.calls.length - 1], live: run('liveCandle["BTC"]')};
  // ---- overlapping candle loads are refused ----
  const p1 = run('loadCandles("ETH")'), p2 = run('loadCandles("ETH")');
  out.loadGuard = [await p1, await p2];
  // ---- back to line: value format, strike line on the new series ----
  run('setKind("BTC","line")'); await advance(0);
  const ls = S("BTC");
  out.switchLine = {kind: ls.kind, firstSetData: (ls.calls.find(x => x.op === "setData") || {}).first,
                    lines: ls.lines.map(h => h.price)};
  // ---- strike line: unchanged strike is not redrawn; a new strike replaces it ----
  const nl = ls.lines.length; await advance(3000);
  out.strikeSame = ls.lines.length - nl;
  // ---- market rollover: new ticker, new close, new strike ----
  const newClose = T0 + 20 * 60000;
  DATA.coins.BTC = coin("BTC", {ticker: "KXBTC15M-B", close: iso(newClose), strike: 104, spot: 103.5,
                                spot_observed_ts: NOW / 1000});
  await advance(1000);
  out.marketRoll = {closeMs: run('marketCloseMs["BTC"]'), expected: newClose, cd: txt("cd_BTC"),
                    remainS: Math.ceil((newClose - NOW) / 1000), lines: ls.lines.map(h => h.price),
                    removed: ls.removedLines, ticker: txt("tk_BTC")};
  // ---- missing close / no market ----
  DATA.coins.XRP = coin("XRP", {close: undefined}); delete DATA.coins.XRP.close;
  DATA.coins.ETH = {coin: "ETH", status: "no market"};
  await advance(1000);
  out.missing = {XRP: txt("cd_XRP"), ETH: txt("cd_ETH")};
  // ---- failed /data: UI kept, retried about once per second, never hammered ----
  const tkKeep = txt("tk_BTC"), cdKeep = txt("cd_BTC");
  DATA_MODE = "fail";
  const f1 = count("/data"); const tf = NOW;
  await advance(10000);
  const failTimes = log.filter(l => l.url === "/data" && l.t >= tf).map(l => l.t);
  out.failure = {calls: count("/data") - f1, link: txt("link"), linkCls: cls("link"), ticker: txt("tk_BTC"),
                 tickerBefore: tkKeep, cd: txt("cd_BTC"), cdBefore: cdKeep,
                 gaps: failTimes.slice(1).map((t, i) => t - failTimes[i])};
  DATA_MODE = "ok"; await advance(1500);
  out.recovered = txt("link");
  // ---- hung /data: never overlapping, abandoned after the timeout, loop continues ----
  DATA_MODE = "hang"; maxInflight = 0; const h0 = count("/data");
  await advance(4000);
  out.hangDuring = count("/data") - h0;
  await advance(3000);
  out.hangAfter = count("/data") - h0;
  out.maxInflight = maxInflight;
  DATA_MODE = "ok"; await advance(2000);
  // ---- background tab: timers throttled, clock jumps; visible again -> exact time at once ----
  NOW += 90000;
  (listeners.visibilitychange || []).forEach(f => f());
  out.background = {cd: txt("cd_BTC"), expected: Math.ceil((newClose - NOW) / 1000)};
  out.allUrls = [...new Set(log.map(l => l.url.split("?")[0]))];
  out.totalCandleFetches = count("/candles");
  out.simulatedSeconds = (NOW - T0) / 1000;
  console.log(JSON.stringify(out));
})().catch(e => { console.log(JSON.stringify({harness_error: String(e && e.stack || e)})); });
"""


def js():
    """Run the harness once (cached). Returns None when node is unavailable."""
    if not NODE:
        return None
    if "out" not in JS:
        d = tempfile.mkdtemp(prefix="_t15js")
        try:
            path = os.path.join(d, "harness.js")
            open(path, "w").write(HARNESS)
            p = subprocess.run([NODE, path], capture_output=True, text=True, timeout=120,
                               env=dict(os.environ, PAGE_JS=page_script()))
        finally:
            shutil.rmtree(d, ignore_errors=True)
        assert p.returncode == 0, p.stderr[-1500:]
        out = json.loads(p.stdout.strip().splitlines()[-1])
        assert "harness_error" not in out, out.get("harness_error")
        JS["out"] = out
    return JS["out"]


def needs_js(fn):
    def wrapped():
        o = js()
        if o is None:
            print("  (node not found: behavioural JS check SKIPPED; static checks still ran)")
            return
        fn(o)
    wrapped.__name__ = fn.__name__
    return wrapped


def _mmss(s):
    return f"{s // 60}:{s % 60:02d}"


# ═══════════════════ 1-3 strategy and polling unchanged ═══════════════════
def test_fingerprint():
    man = json.load(open(os.path.join(HERE, "step5_baseline_manifest.json")))
    ok, why = sf.verify(os.path.join(HERE, "kalshi_dashboard.py"), man)
    assert ok, why
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == TRUSTED_V2
    assert man["legacy_strategy_fingerprint"] == TRUSTED_V2


def _literal(name):
    tree = ast.parse(open(os.path.join(HERE, "kalshi_dashboard.py")).read())
    vals = [ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and len(n.targets) == 1
            and isinstance(n.targets[0], ast.Name) and n.targets[0].id == name]
    assert len(vals) == 1, (name, vals)
    return vals[0]


def test_poll_seconds():
    assert _literal("POLL_SECONDS") == 4 and k.POLL_SECONDS == 4
    assert "time.sleep(POLL_SECONDS)" in inspect.getsource(k.poller)


def test_perp_sample_seconds():
    assert _literal("PERP_SAMPLE_SECONDS") == 4 and k.PERP_SAMPLE_SECONDS == 4


def test_backend_caches_unchanged():
    import test_stage9 as t9
    for fn in ("candles", "spot", "evaluate"):                      # byte-identical strategy data paths
        assert hashlib.sha256(inspect.getsource(getattr(k, fn)).encode()).hexdigest() == t9.STRATEGY_FUNCTION_HASHES[fn], fn
    assert "now - ts < 30" in inspect.getsource(k.candles)
    assert "nowt - ts < 12" in inspect.getsource(k.web_candles)


# ═══════════════════ 4-9 countdown + /data loop ═══════════════════
@needs_js
def test_countdown_absolute(o):
    assert o["boot"]["cd"]["BTC"] == "5:50"                          # close = T0 + 350 s, from r.close
    js_src = page_script()
    assert "Date.parse(r.close)" in js_src and "r.remain" not in js_src   # remain is no longer the display clock
    assert '"remain": round(remain, 1)' in inspect.getsource(k.evaluate)     # backend still provides it


@needs_js
def test_countdown_own_timer(o):
    assert o["consts"]["COUNTDOWN_MS"] == 250
    assert [250, "updateCountdowns"] in o["intervals"]
    seq = o["countdownSeq"]                                          # sampled every 250 ms for 3 s
    secs = [int(x.split(":")[0]) * 60 + int(x.split(":")[1]) for x in seq]
    assert all(b <= a for a, b in zip(secs, secs[1:])) and secs[0] - secs[-1] in (2, 3)
    assert all(a - b <= 1 for a, b in zip(secs, secs[1:]))           # smooth: never jumps several seconds


@needs_js
def test_countdown_no_requests(o):
    assert o["countdownRequests"] == 0
    src = page_script()
    body = src[src.index("function updateCountdowns"):src.index("function renderCoin")]
    assert "fetch" not in body


@needs_js
def test_data_refresh_1s(o):
    assert o["consts"]["DATA_REFRESH_MS"] == 1000
    assert 9 <= o["dataPer10s"] <= 11, o["dataPer10s"]
    assert "setInterval(tick" not in page_script()


@needs_js
def test_data_loop_no_overlap(o):
    assert o["maxInflight"] == 1
    assert o["hangDuring"] == 1                                      # 4 s of a hung request: no second request
    assert 2 <= o["hangAfter"] <= 3                                  # abandoned after 5 s, loop resumed
    src = page_script()
    assert re.search(r"async function dataLoop\(\)\{[^}]*await tick\(\);\}finally\{setTimeout\(dataLoop,DATA_REFRESH_MS\)", src)


@needs_js
def test_failed_data_retries_safely(o):
    f = o["failure"]
    assert 9 <= f["calls"] <= 11 and all(g >= 1000 for g in f["gaps"]), f      # ~1/s, never hammered
    assert f["link"] == "retrying" and f["linkCls"] == "amb"
    assert f["ticker"] == f["tickerBefore"] and f["ticker"]                     # values kept on screen
    assert f["cd"] != "–" and f["cd"] != f["cdBefore"]                          # countdown keeps running
    assert o["recovered"] == "live"


# ═══════════════════ 10-16 chart ═══════════════════
@needs_js
def test_no_15s_full_reload(o):
    src = page_script()
    assert "candleTick" not in src and "%5===0" not in src
    assert o["candlesIn10s"] == 0 and o["setDataIn10s"] == 0
    assert o["consts"]["CANDLE_RECONCILE_MS"] == 60000
    assert o["boot"]["candles"] == 4                                 # one history bootstrap per coin


@needs_js
def test_line_live_update(o):
    ups = [c for c in o["lineCalls"] if c["op"] == "update"]
    assert len(ups) == 3 and not [c for c in o["lineCalls"] if c["op"] == "setData"]
    assert [u["bar"]["value"] for u in ups] == [101.5, 102.25, 101.75]
    assert all(u["bar"]["time"] == o["lineMinute"] and set(u["bar"]) == {"time", "value"} for u in ups)


@needs_js
def test_candle_hlc(o):
    b = o["candleSame"]["last"]["bar"]
    assert o["candleSame"]["last"]["op"] == "update" and b["time"] == o["candleSame"]["minute"]
    assert b["high"] >= 104.0 and b["low"] <= 97.5 and b["close"] == 101.0
    assert b["open"] == 100                                          # continued from the historical bar's open
    assert b["high"] == 104.0 and b["low"] == 97.5                   # hist h/l 102/98 widened by spot


@needs_js
def test_minute_rollover(o):
    r = o["rollover"]
    assert r["first"]["bar"] == {"time": r["minute"], "open": 102.0, "high": 102.0, "low": 102.0, "close": 102.0}
    assert r["second"]["bar"] == {"time": r["minute"], "open": 102.0, "high": 103.5, "low": 102.0, "close": 103.5}
    assert o["updateTimesMonotone"]


@needs_js
def test_reconciliation(o):
    rc = o["reconcile"]
    assert rc["fetches"] == 1 and rc["setData"] == 1
    assert rc["live"]["high"] == 109.0 and rc["live"]["low"] == 99.0 and rc["live"]["open"] == 101.9
    assert rc["live"]["close"] == 103.5                               # the newest observed spot stays the close
    assert rc["last"]["op"] == "update" and rc["last"]["bar"]["high"] == 109.0
    assert o["loadGuard"] == [True, False]                            # second concurrent load refused


@needs_js
def test_chart_type_change(o):
    s = o["switchCand"]
    assert s["kind"] == "cand" and s["oldRemoved"] and s["candlesFetched"] == 0
    assert s["ops"][0] == "setData" and s["ops"][-1] == "update"
    l = o["switchLine"]
    assert l["kind"] == "line" and set(l["firstSetData"]) == {"time", "value"}


@needs_js
def test_strike_line(o):
    assert o["switchCand"]["lines"] == [100] and o["switchLine"]["lines"][0] == 100
    assert o["strikeSame"] == 0                                       # unchanged strike is not redrawn
    assert o["marketRoll"]["lines"][-1] == 104 and 100 in o["marketRoll"]["removed"]


# ═══════════════════ 17-19 rollover / missing close / no extra Coinbase ═══════════════════
@needs_js
def test_market_rollover(o):
    m = o["marketRoll"]
    assert m["closeMs"] == m["expected"] and m["cd"] == _mmss(m["remainS"]) and m["ticker"] == "KXBTC15M-B"
    b = o["background"]                                               # throttled tab catches up at once
    assert b["cd"] == _mmss(b["expected"])


@needs_js
def test_missing_close(o):
    assert o["boot"]["cd"]["XRP"] == "–"                               # coin absent from /data
    assert o["boot"]["cd"]["ETH"] == "–"                               # unparseable close
    assert o["boot"]["cd"]["SOL"] == "–"                               # stale, never had a close
    assert o["missing"] == {"XRP": "–", "ETH": "–"}                    # ok-without-close, no market


@needs_js
def test_no_extra_coinbase(o):
    assert set(o["allUrls"]) <= {"/data", "/candles", "/ping"}, o["allUrls"]
    src = page_script()
    for bad in ("coinbase.com", "api.exchange", "://", "WebSocket", "EventSource", "io("):   # no external endpoint
        assert bad not in src, bad
    targets = set(re.findall(r'fetch\("([^"?]+)', src))           # local routes only
    assert targets == {"/data", "/candles", "/ping", "/control", "/paper_enter"}, targets   # last two: user clicks
    per_min_after = o["totalCandleFetches"] / (o["simulatedSeconds"] / 60.0)
    assert per_min_after <= 4 * 3.5, per_min_after                    # was 16/min (4 coins / 15 s)
    assert o["boot"]["dataNoStore"] and o["boot"]["candleNoStore"]


def test_data_age_indicator():
    o = js()
    src = page_script()
    assert "AGE_WARN_S=5, AGE_STALE_S=10" in src and "spot_observed_ts" in src
    if o is None:
        print("  (node not found: behavioural JS check SKIPPED)"); return
    age = lambda t: float(t[len("spot "):-1])
    assert o["ageNormal"][1] == "mini" and 2.5 <= age(o["ageNormal"][0]) < 5          # < 5 s: normal
    assert o["ageAmber"][1] == "mini amb" and 5 <= age(o["ageAmber"][0]) <= 10         # 5-10 s: amber
    assert o["ageRed"][1] == "mini red" and age(o["ageRed"][0]) > 10                   # > 10 s: red


# ═══════════════════ backend: /data route, headers, lock, latency, CPU ═══════════════════
class _Wfile:
    def __init__(self):
        self.buf, self.lock_held = b"", []
    def write(self, b):
        self.lock_held.append(k.LOCK.locked()); self.buf += b
    def flush(self):
        pass


def _call_get(path):
    h = k.Handler.__new__(k.Handler)
    h.path, h.wfile, h.request_version, h.requestline, h.command = path, _Wfile(), "HTTP/1.1", f"GET {path}", "GET"
    h.client_address = ("127.0.0.1", 0)
    h.close_connection = True
    h._headers_buffer = []
    h.do_GET()
    return h.wfile


def test_data_route_headers_and_lock():
    w = _call_get("/data")
    head = w.buf.split(b"\r\n\r\n", 1)[0].decode()
    assert "Cache-Control: no-store" in head and "application/json" in head
    assert not any(w.lock_held), "LOCK held while writing to the socket"
    json.loads(w.buf.split(b"\r\n\r\n", 1)[1])
    page = _call_get("/")
    assert b"Cache-Control" not in page.buf.split(b"\r\n\r\n", 1)[0]     # static page: default caching
    with t7.patched(k, web_candles=lambda product, minutes=60: [{"t": 60, "o": 1, "h": 1, "l": 1, "c": 1}]):
        c = _call_get("/candles?coin=BTC")
    assert b"Cache-Control: no-store" in c.buf and b'"t": 60' in c.buf


def _realistic_state():
    import test_stage14 as t14
    tel, t = t14._tel(coins=tuple(k.COINS)), 10_000.0
    for c in k.COINS:
        t14._history(tel, t, coin=c)
    rows = tel.record_cycle({c: {"status": "ok", "spot_raw": 50.0} for c in k.COINS}, {c: t for c in k.COINS},
                            now=t + 0.5)
    coins = {c: {"coin": c, "status": "ok", "ticker": f"KX{c}15M-X", "spot": 100.5, "spot_observed_ts": t,
                 "close": "2026-09-01T12:15:00Z", "remain": 6.0, "conf": 91.0, "verdict": "x", "reason": "WAIT"}
             for c in k.COINS}
    return {"coins": coins, "perps": rows, "perp_vol": k._perp_vol_views(rows), "stats": {}, "calls": {},
            "updated": "12:00:00", "controls": {"running": True}}


def test_data_latency_and_cpu():
    saved = dict(k.STATE)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), k.Handler)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        with k.LOCK:
            k.STATE.update(_realistic_state())
        conn = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
        lat, size = [], 0
        cpu0, wall0 = time.process_time(), time.perf_counter()
        for _ in range(120):                                       # two minutes of 1 s refreshes
            t0 = time.perf_counter()
            conn.request("GET", "/data")
            r = conn.getresponse(); body = r.read()
            lat.append((time.perf_counter() - t0) * 1000.0); size = len(body)
            assert r.status == 200 and r.getheader("Cache-Control") == "no-store"
            conn.close(); conn = http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
        cpu = time.process_time() - cpu0
        lat.sort()
        tracemalloc.start()
        for _ in range(20):
            with k.LOCK:
                json.dumps(k.STATE)
        peak = tracemalloc.get_traced_memory()[1]; tracemalloc.stop()
        JS["perf"] = {"median_ms": round(lat[len(lat) // 2], 3), "p95_ms": round(lat[int(len(lat) * 0.95)], 3),
                      "max_ms": round(lat[-1], 3), "bytes": size, "cpu_ms_per_request": round(cpu / 120 * 1000, 3),
                      "cpu_pct_at_1hz": round(cpu / 120 * 100, 3), "serialize_peak_bytes": peak,
                      "data_calls_per_minute": 60}
        print(f"  /data: median {JS['perf']['median_ms']} ms, p95 {JS['perf']['p95_ms']} ms, "
              f"{size} bytes; server CPU ~{JS['perf']['cpu_ms_per_request']} ms/request "
              f"(~{JS['perf']['cpu_pct_at_1hz']}% of one core at 1 req/s); serialise peak {peak} B")
        assert lat[len(lat) // 2] < 50.0, lat[len(lat) // 2]        # generous: local JSON of in-memory state
        assert cpu / 120 < 0.05                                     # < 50 ms CPU per request
    finally:
        srv.shutdown(); srv.server_close()
        with k.LOCK:
            k.STATE.clear(); k.STATE.update(saved)


# ═══════════════════ 20-23 strategy / telemetry / live veto / earlier stages ═══════════════════
def test_evaluate_unchanged():
    t7.test_golden_unchanged()
    import test_stage8 as t8
    t8.test_strategy_identical()
    for fn in sf.STRATEGY_FUNCTIONS:
        src = inspect.getsource(getattr(k, fn))
        assert "perp_vol" not in src and "DATA_REFRESH" not in src, fn


def test_telemetry_unchanged():
    for f in ("perp_telemetry.py",):
        assert hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest() == UNCHANGED_FILES[f], f
    import test_stage12 as t12
    t12.test_preview_parity_and_causality()
    t12.test_preview_is_read_only()


def test_live_veto_unchanged():
    for f in ("perp_live.py", "perp_probability.py", "perp_shadow.py"):
        assert hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest() == UNCHANGED_FILES[f], f
    import test_stage12 as t12
    t12.test_live_math()
    t12.test_gate_cannot_upgrade()
    assert "_gate_decision(coin, r)" in inspect.getsource(k.poller)


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 15):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    if not NODE:
        print("NOTE: node not found - behavioural browser checks are skipped (static + backend checks run)")
    run("1  legacy strategy fingerprint unchanged", test_fingerprint)
    run("2  POLL_SECONDS remains 4", test_poll_seconds)
    run("3  PERP_SAMPLE_SECONDS unchanged", test_perp_sample_seconds)
    run("+  strategy candles()/spot()/evaluate() + caches unchanged", test_backend_caches_unchanged)
    run("4  countdown from absolute r.close", test_countdown_absolute)
    run("5  countdown has its own local timer", test_countdown_own_timer)
    run("6  countdown makes zero HTTP requests", test_countdown_no_requests)
    run("7  /data refresh ~1 s", test_data_refresh_1s)
    run("8  /data loop cannot overlap itself", test_data_loop_no_overlap)
    run("9  failed /data retried safely; UI kept", test_failed_data_retries_safely)
    run("10 no full chart reload every ~15 s", test_no_15s_full_reload)
    run("11 spot updates line chart via series.update()", test_line_live_update)
    run("12 spot updates candlestick H/L/C", test_candle_hlc)
    run("13 minute rollover starts a new candle", test_minute_rollover)
    run("14 history reconciles with /candles periodically", test_reconciliation)
    run("15 chart type change works", test_chart_type_change)
    run("16 strike line works", test_strike_line)
    run("17 market rollover replaces the close; background tab catches up", test_market_rollover)
    run("18 missing close renders –", test_missing_close)
    run("19 no extra Coinbase REST from the browser refresh", test_no_extra_coinbase)
    run("+  data-age indicator (normal / amber / red)", test_data_age_indicator)
    run("+  /data: no-store, lock not held during socket write", test_data_route_headers_and_lock)
    run("+  /data latency, CPU and memory at 1 Hz", test_data_latency_and_cpu)
    run("20 evaluate() output unchanged", test_evaluate_unchanged)
    run("21 telemetry behaviour unchanged", test_telemetry_unchanged)
    run("22 live-veto behaviour unchanged", test_live_veto_unchanged)
    run("23 all previous stage suites (1-14)", test_previous_stages)
    print("\nAll Stage 15 tests passed.")
