#!/usr/bin/env python3
"""
label_binary_outcomes.py — map each binary ticker seen in the perp telemetry to its
final Kalshi settlement result. Step 3 research tooling; changes nothing in the bot.

    python label_binary_outcomes.py
    python label_binary_outcomes.py --telemetry kalshi_perp_telemetry.csv --output kalshi_binary_outcomes.csv

Source of truth: the SAME public endpoint the watcher already uses for settlement
(kalshi_dashboard.market_result):
    GET https://external-api.kalshi.com/trade-api/v2/markets/{ticker}  ->  market.result
    "yes" -> outcome_up = 1      "no" -> outcome_up = 0      anything else -> unresolved
The result is never inferred from Coinbase or from spot vs strike.

Rules
  * The telemetry CSV is only READ. Labels live in their own file (this file is its cache).
  * A ticker is fetched only when now >= binary_close_time + grace (default 90 s).
  * A final yes/no label is never refetched (unless --verify-final) and never overwritten:
    a contradictory later answer marks the row CONFLICTING_SETTLEMENT_RESULT and keeps both.
  * GET only; timeout, rate limit, retry with backoff; the HTTP function is injectable.
"""
import argparse
import csv
import datetime as dt
import os
import sys
import tempfile
import time

import requests

import http_session

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"      # same as kalshi_dashboard.KALSHI_BASE
SOURCE = "kalshi GET /trade-api/v2/markets/{ticker} market.result"
LABEL_GRACE_SECONDS = 90
REQUEST_TIMEOUT_SECONDS = 10
MIN_REQUEST_INTERVAL_SECONDS = 0.25     # <= 4 requests/second
MAX_RETRIES = 3                         # per ticker per run, for timeouts / 429 / 5xx
BACKOFF_BASE_SECONDS = 1.0              # 1 s, 2 s, 4 s ...
RETRY_UNSETTLED_AFTER_SECONDS = 300     # don't re-poll an eligible-but-unsettled ticker more often
MAX_REQUESTS_PER_RUN = 2000

ST_FINAL = "final"
ST_PENDING = "pending"
ST_ERROR = "error"
ST_UNEXPECTED = "unexpected_result"
ST_CONFLICT = "conflict"
ST_META = "metadata_conflict"
FLAG_CONFLICT = "CONFLICTING_SETTLEMENT_RESULT"
FLAG_META = "INCONSISTENT_TICKER_METADATA"

OUT_COLUMNS = ["binary_ticker", "coin", "binary_close_time", "settle_result", "outcome_up",
               "label_status", "labeled_at_utc", "source", "source_error", "first_seen_utc",
               "last_checked_utc", "attempt_count", "conflicting_result", "quality_flags"]


def map_result(result):
    """'yes' -> 1, 'no' -> 0, anything else -> None (never guessed)."""
    r = (result or "").strip().lower() if isinstance(result, str) else ""
    return {"yes": 1, "no": 0}.get(r)


def parse_close(s):
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat()


class OutcomeFetcher:
    """GET-only client with timeout, rate limit and retry/backoff. Injectable for tests."""

    def __init__(self, http_get=None, sleep=time.sleep, monotonic=time.monotonic,
                 base=KALSHI_BASE, timeout=REQUEST_TIMEOUT_SECONDS,
                 min_interval=MIN_REQUEST_INTERVAL_SECONDS, max_retries=MAX_RETRIES,
                 backoff=BACKOFF_BASE_SECONDS):
        self.http_get = http_get or http_session.get      # pooled, thread-local; same request semantics
        self.sleep, self.monotonic = sleep, monotonic
        self.base, self.timeout = base.rstrip("/"), timeout
        self.min_interval, self.max_retries, self.backoff = min_interval, max_retries, backoff
        self._last = None
        self.requests_made = 0

    def _throttle(self):
        if self._last is not None:
            wait = self.min_interval - (self.monotonic() - self._last)
            if wait > 0:
                self.sleep(wait)
        self._last = self.monotonic()

    def fetch(self, ticker):
        """Returns (result_string_or_None, error_or_None)."""
        url = f"{self.base}/markets/{ticker}"
        err = None
        for attempt in range(self.max_retries):
            self._throttle()
            self.requests_made += 1
            try:
                r = self.http_get(url, timeout=self.timeout)
                code = getattr(r, "status_code", 200)
                if code == 429 or code >= 500:
                    err = f"HTTP {code}"
                elif code >= 400:
                    return None, f"HTTP {code}"                 # 4xx: not retryable
                else:
                    data = r.json()
                    if not isinstance(data, dict) or not isinstance(data.get("market"), dict):
                        return None, "malformed response: 'market' missing"
                    res = data["market"].get("result")
                    return (res if isinstance(res, str) else None), None
            except (requests.RequestException, ValueError) as e:
                err = f"{type(e).__name__}: {e}"[:200]
            if attempt < self.max_retries - 1:
                self.sleep(self.backoff * (2 ** attempt))
        return None, err


def collect_tickers(telemetry_path):
    """Stream the telemetry (read-only) -> {ticker: {coins, closes, first_seen}}."""
    seen = {}
    with open(telemetry_path, newline="") as f:
        for row in csv.DictReader(f):
            tk = (row.get("binary_ticker") or "").strip()
            if not tk:
                continue
            e = seen.setdefault(tk, {"coins": set(), "closes": {}, "first_seen": None})
            if row.get("coin"):
                e["coins"].add(row["coin"])
            ct = (row.get("binary_close_time") or "").strip()
            if ct:
                e["closes"][parse_close(ct)] = ct
            ts = row.get("ts_utc") or ""
            if ts and (e["first_seen"] is None or ts < e["first_seen"]):
                e["first_seen"] = ts
    return seen


def load_cache(path):
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as f:
        return {r["binary_ticker"]: dict(r) for r in csv.DictReader(f) if r.get("binary_ticker")}


def write_cache(path, entries):
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".labels-", suffix=".csv", dir=d)
    with os.fdopen(fd, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for e in sorted(entries.values(), key=lambda e: (e.get("binary_close_time") or "", e["binary_ticker"])):
            w.writerow({c: ("" if e.get(c) is None else e.get(c)) for c in OUT_COLUMNS})
    os.replace(tmp, path)                       # atomic: a crash never leaves a half-written cache


def apply_result(entry, result, err, now):
    """Merge one fetch into a cache entry. Final labels are immutable."""
    entry["attempt_count"] = int(entry.get("attempt_count") or 0) + 1
    entry["last_checked_utc"] = iso(now)
    entry["source"] = SOURCE
    status = entry.get("label_status")
    mapped = map_result(result)
    if status == ST_CONFLICT:
        return entry                                           # never auto-resolved
    if status == ST_FINAL:
        if err is None and mapped is not None and result.strip().lower() != entry.get("settle_result"):
            entry["label_status"] = ST_CONFLICT
            entry["conflicting_result"] = result.strip().lower()
            entry["outcome_up"] = ""                           # unusable until a human resolves it
            entry["quality_flags"] = FLAG_CONFLICT
            entry["source_error"] = (f"{FLAG_CONFLICT}: cached '{entry.get('settle_result')}' "
                                     f"vs API '{result.strip().lower()}' at {iso(now)}")
        return entry
    if err is not None:
        entry["label_status"], entry["source_error"] = ST_ERROR, err
    elif mapped is not None:
        entry.update(label_status=ST_FINAL, settle_result=result.strip().lower(), outcome_up=mapped,
                     labeled_at_utc=iso(now), source_error="")
    elif result in (None, ""):
        entry["label_status"], entry["source_error"] = ST_PENDING, "result not yet published"
    else:
        entry.update(label_status=ST_UNEXPECTED, settle_result=result, outcome_up="",
                     source_error=f"unexpected result '{result}'")
    return entry


def run_labeler(telemetry_path, output_path, now=None, grace=LABEL_GRACE_SECONDS, fetcher=None,
                verify_final=False, retry_after=RETRY_UNSETTLED_AFTER_SECONDS,
                max_requests=MAX_REQUESTS_PER_RUN, log=print):
    now = time.time() if now is None else now
    fetcher = fetcher or OutcomeFetcher()
    seen = collect_tickers(telemetry_path)
    cache = load_cache(output_path)
    counts = {"tickers": len(seen), "fetched": 0, "skipped_final": 0, "not_eligible": 0,
              "throttled": 0, "metadata_conflict": 0}
    for tk in sorted(seen):
        info = seen[tk]
        closes = {k for k in info["closes"] if k is not None}
        e = cache.get(tk) or {"binary_ticker": tk, "attempt_count": 0, "label_status": ST_PENDING,
                              "first_seen_utc": info["first_seen"] or ""}
        e.setdefault("first_seen_utc", info["first_seen"] or "")
        if len(info["coins"]) > 1 or len(info["closes"]) > 1:
            if e.get("label_status") != ST_FINAL:
                e["label_status"] = ST_META
            e["quality_flags"] = FLAG_META
            e["source_error"] = f"{FLAG_META}: coins={sorted(info['coins'])} closes={sorted(info['closes'].values())}"
            cache[tk] = e; counts["metadata_conflict"] += 1
            continue
        e["coin"] = next(iter(info["coins"]), e.get("coin", ""))
        e["binary_close_time"] = next(iter(info["closes"].values()), e.get("binary_close_time", ""))
        cache[tk] = e
        status = e.get("label_status")
        if status == ST_FINAL and not verify_final:
            counts["skipped_final"] += 1
            continue
        if status == ST_CONFLICT:
            continue
        close = next(iter(closes), None)
        if close is None:
            e["label_status"], e["source_error"] = ST_ERROR, "unparseable binary_close_time"
            continue
        if now < close + grace:                               # never ask about an open market
            if status != ST_FINAL:
                e["label_status"] = ST_PENDING
                e["source_error"] = f"not eligible until {iso(close + grace)}"
            counts["not_eligible"] += 1
            continue
        last = parse_close(e.get("last_checked_utc"))
        if status != ST_FINAL and last is not None and now - last < retry_after:
            counts["throttled"] += 1
            continue
        if counts["fetched"] >= max_requests:
            counts["throttled"] += 1
            continue
        result, err = fetcher.fetch(tk)
        counts["fetched"] += 1
        apply_result(e, result, err, now)
    write_cache(output_path, cache)
    by_status = {}
    for e in cache.values():
        by_status[e.get("label_status")] = by_status.get(e.get("label_status"), 0) + 1
    counts["by_status"] = by_status
    counts["conflicts"] = sorted(tk for tk, e in cache.items() if e.get("label_status") == ST_CONFLICT)
    if log:
        log(f"labeler: {counts['tickers']} tickers in telemetry; fetched {counts['fetched']}; "
            f"final cached {counts['skipped_final']}; not yet eligible {counts['not_eligible']}; "
            f"throttled {counts['throttled']}; metadata conflicts {counts['metadata_conflict']}")
        log(f"labels by status: {by_status}")
        if counts["conflicts"]:
            log(f"WARNING {FLAG_CONFLICT}: {counts['conflicts']}")
    return counts


def main(argv=None):
    ap = argparse.ArgumentParser(description="Label binary tickers with Kalshi settlement results (GET only).")
    ap.add_argument("--telemetry", default="kalshi_perp_telemetry.csv")
    ap.add_argument("--output", default="kalshi_binary_outcomes.csv")
    ap.add_argument("--grace", type=float, default=LABEL_GRACE_SECONDS)
    ap.add_argument("--verify-final", action="store_true",
                    help="re-check already-final labels and flag any contradiction (never overwrites)")
    a = ap.parse_args(argv)
    if not os.path.exists(a.telemetry):
        print(f"no such telemetry file: {a.telemetry}", file=sys.stderr)
        return 2
    run_labeler(a.telemetry, a.output, grace=a.grace, verify_final=a.verify_final)
    return 0


if __name__ == "__main__":
    sys.exit(main())
