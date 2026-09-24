"""
Asset -> CF Benchmarks index mapping and Kalshi ticker / close-time consistency checks.

The index ids follow CF Benchmarks' RTI naming (BRTI for bitcoin, <COIN>USD_RTI otherwise). They
are an ASSUMPTION until confirmed by a real capture (Kalshi's cfbenchmarks_value channel supports an
"indexlist" action; the importer records whatever id each frame carries, and reconstruction only
uses observations whose id equals the market's index_id, so a wrong mapping yields MISSING, never a
wrong value).

Kalshi tickers such as KXBTC15M-25DEC220415 encode the close in US Eastern time (YY MON DD HH MM).
The authoritative close is the API's close_time (UTC ISO); the ticker is only a cross-check, done
with IANA tz rules so it is correct across daylight-saving changes, and reported as ambiguous in the
repeated fall-back hour instead of guessed.
"""
import datetime as dt
import re

ASSET_INDEX = {"BTC": "BRTI", "ETH": "ETHUSD_RTI", "SOL": "SOLUSD_RTI", "XRP": "XRPUSD_RTI"}
SERIES_ASSET = {"KXBTC15M": "BTC", "KXETH15M": "ETH", "KXSOL15M": "SOL", "KXXRP15M": "XRP"}
INDEX_ASSET = {v: k for k, v in ASSET_INDEX.items()}
MARKET_TZ = "America/New_York"

_TICKER = re.compile(r"^(?P<series>[A-Z0-9]+)-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})(?P<hh>\d{2})(?P<mi>\d{2})(?:-.*)?$")
_MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT",
                                       "NOV", "DEC"), start=1)}


def series_of(ticker):
    return (ticker or "").split("-")[0].upper()


def asset_of(ticker):
    return SERIES_ASSET.get(series_of(ticker))


def ticker_close_candidates_ms(ticker, tz_name=MARKET_TZ):
    """UTC epoch-ms candidates for the Eastern close encoded in a ticker.
    [] if the ticker has no time or the time does not exist (spring-forward gap); two values in the
    ambiguous fall-back hour; None if the tz database is unavailable."""
    m = _TICKER.match((ticker or "").upper())
    if not m or m.group("mon") not in _MONTHS:
        return []
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
    except Exception:
        return None
    try:
        naive = dt.datetime(2000 + int(m.group("yy")), _MONTHS[m.group("mon")], int(m.group("dd")),
                            int(m.group("hh")), int(m.group("mi")))
    except ValueError:
        return []
    out = []
    for fold in (0, 1):
        local = naive.replace(tzinfo=tz, fold=fold)
        back = local.astimezone(dt.timezone.utc).astimezone(tz).replace(tzinfo=None)
        if back == naive:                                  # exists (not in the spring-forward gap)
            ms = int(local.astimezone(dt.timezone.utc).timestamp() * 1000)
            if ms not in out:
                out.append(ms)
    return sorted(out)


def check_ticker_close(ticker, close_ts_ms):
    """True / False, or None when the check cannot be made (no encoded time, no tz database)."""
    c = ticker_close_candidates_ms(ticker)
    if c is None or not c:
        return None
    return close_ts_ms in c
