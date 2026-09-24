"""
Kalshi market metadata -> SettlementMarket (features may use) + OfficialResolution (labels only).

Strike: floor_strike, else cap_strike, else strike — the same precedence as the production
kalshi_dashboard.strike_of(). close_time / open_time must carry an explicit timezone (no guessing).
expiration_value is Kalshi's settled index value; result is "yes"/"no" once settled (anything else
is kept as unresolved, never guessed).
"""
from settlement.assets import ASSET_INDEX, asset_of, check_ticker_close, series_of
from settlement.schemas import KALSHI_MARKET, parse_iso_utc_ms, parse_value, validate
from settlement.types import OfficialResolution, ParseIssue, SettlementMarket

SOURCE = "kalshi_market_api"


def parse_market(obj, source=SOURCE):
    """Returns (SettlementMarket | None, OfficialResolution | None, [ParseIssue])."""
    if isinstance(obj, dict) and isinstance(obj.get("market"), dict):
        obj = obj["market"]                              # GET /markets/{ticker} envelope
    chk = validate(obj, KALSHI_MARKET)
    if not chk.ok:
        return None, None, [ParseIssue(source, "SCHEMA_MISMATCH", "; ".join(chk.problems)[:300],
                                       location=str(obj.get("ticker")) if isinstance(obj, dict) else "")]
    ticker = obj["ticker"]
    issues = []
    close_ms, err = parse_iso_utc_ms(obj["close_time"])
    if err:
        return None, None, [ParseIssue(source, "INVALID_TIMESTAMP", "close_time: " + err, location=ticker)]
    open_ms = None
    if obj.get("open_time"):
        open_ms, err = parse_iso_utc_ms(obj["open_time"])
        if err:
            issues.append(ParseIssue(source, "INVALID_TIMESTAMP", "open_time: " + err, location=ticker))
    asset = asset_of(ticker)
    if asset is None:
        return None, None, issues + [ParseIssue(source, "UNSUPPORTED_SERIES", series_of(ticker), location=ticker)]
    strike, strike_src = None, ""
    for k in ("floor_strike", "cap_strike", "strike"):
        if obj.get(k) is not None:
            v, err = parse_value(obj[k])
            if err:
                issues.append(ParseIssue(source, "MALFORMED_VALUE", f"{k}: {err}", location=ticker))
            else:
                strike, strike_src = v, k
            break
    if check_ticker_close(ticker, close_ms) is False:
        issues.append(ParseIssue(source, "TICKER_CLOSE_MISMATCH", f"close_time {obj['close_time']} not the ticker's ET time",
                                 location=ticker))
    market = SettlementMarket(ticker=ticker, asset=asset, close_ts_ms=close_ms, index_id=ASSET_INDEX[asset],
                              strike=strike, strike_source=strike_src, open_ts_ms=open_ms, series=series_of(ticker),
                              metadata_source=source, metadata_schema_fingerprint=chk.fingerprint)
    res = obj.get("result")
    result = res.strip().lower() if isinstance(res, str) and res.strip().lower() in ("yes", "no") else None
    ev = None
    if obj.get("expiration_value") not in (None, ""):
        ev, err = parse_value(obj["expiration_value"])
        if err:
            issues.append(ParseIssue(source, "MALFORMED_VALUE", "expiration_value: " + err, location=ticker))
    resolution = OfficialResolution(ticker=ticker, result=result, expiration_value=ev, source=source,
                                    schema_fingerprint=chk.fingerprint) if (result or ev is not None) else None
    return market, resolution, issues
