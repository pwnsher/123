"""
Perp feature registry: name, FAMILY (for family-level ablation later), venue, window, unit, meaning,
and a completeness note. RESEARCH ONLY. Statuses are Step 3's (READY / NOT_READY / MISSING / UNAVAILABLE /
UNDEFINED / PARTIAL); a value is None unless READY or PARTIAL.

Conventions
    * Horizons / windows reuse Step 3's (1s 5s 15s 30s 60s 3m 5m). Trade / liquidation windows are (T - w, T].
    * Prices are compared only in the same unit: USDT-quoted perps are converted to USD with the USDT/USD rate
      available at T before any comparison with the USD spot reference or the CF RTI ("_bps" vs ref / cf).
      `*_raw_bps` variants deliberately skip the conversion (the USDT premium is then inside them).
    * Flow / liquidation / OI notionals are in the contract QUOTE currency (USDT for all three size venues), so
      they aggregate across venues without a conversion; sizes are base-coin quantities (never raw contracts).
    * Nothing is labelled bullish / bearish: positive basis, funding, OI change or flow are stored as numbers.
    * Kalshi perps: prices are per contract with unverified units; only ratio features are READY, the rest
      UNAVAILABLE (no trades with sides, OI, liquidations or depth are published).
"""
from dataclasses import asdict, dataclass

from market_data.features.definitions import HORIZONS_MS, Status  # noqa: F401  (shared vocabulary)
from perp_data.venues import PERP_VENUES

FAMILIES = ("PRICE", "BASIS", "FUNDING", "OPEN_INTEREST", "LIQUIDATION", "FLOW", "CVD", "ORDERBOOK",
            "CROSS_EXCHANGE", "PERP_SPOT_DIVERGENCE")
RET_H = ("1s", "5s", "15s", "30s", "60s", "3m", "5m")
BASIS_CHG_H = ("5s", "15s", "30s", "60s", "3m", "5m")
BASIS_ACC_H = ("15s", "60s")
FUND_CHG_H = ("60s", "5m")
OI_H = ("5s", "15s", "30s", "60s", "3m", "5m")
OI_ACC_H = ("15s", "60s", "5m")
LIQ_W = ("1s", "5s", "15s", "30s", "60s", "3m")
FLOW_W = ("1s", "5s", "15s", "30s", "60s", "3m", "5m")
CVD_W = ("5s", "15s", "30s", "60s", "3m", "5m")
BOOK_CHG_H = ("5s", "60s")
MOVE_RV_H = ("15s", "60s")
X_RET_H = ("5s", "15s", "30s", "60s", "3m", "5m")
X_OI_H = ("60s", "5m")
DIV_H = ("5s", "15s", "30s", "60s", "3m")
DIV_FLOW_W = ("15s", "60s")
DIV_RV = ("60s", "5m")
DEPTHS = (5, 10)


@dataclass(frozen=True)
class PerpFeatureConfig:
    asof_max_age_ms: int = 5000          # price / mark / index / book as-of lookups (1-s grid)
    usdt_max_age_ms: int = 60_000        # USDT/USD rate used for conversion
    oi_max_age_ms: int = 15_000          # open interest as-of (REST-polled on Binance)
    funding_max_age_ms: int = 120_000
    source_alive_ms: int = 10_000        # a venue with no message for this long is not "observed"
    retention_ms: int = 1_500_000
    funding_pct_min_n: int = 30          # extreme-funding percentile needs this many observations ...
    funding_pct_min_span_ms: int = 300_000   # ... spanning at least this long (session-relative percentile)
    partial_windows: bool = False        # named research mode (PARTIAL); default strict
    partial_min_coverage: float = 0.8

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class PerpFeatureDef:
    name: str
    family: str
    venue: str
    window: str
    unit: str
    description: str
    completeness: str = ""


def _defs():
    d = []

    def add(n, fam, v, w, u, t, c=""):
        d.append(PerpFeatureDef(n, fam, v, w, u, t, c))
    for v in PERP_VENUES:
        p = f"perp.{v}"
        q = "USD/contract" if v == "kalshi_perp" else "USDT"
        # PRICE
        add(f"{p}.last", "PRICE", v, "-", q, "last trade price (never used as mark)")
        add(f"{p}.mid", "PRICE", v, "-", q, "(best bid + best ask) / 2")
        add(f"{p}.mark", "PRICE", v, "-", q, "venue mark price (never used as index)")
        add(f"{p}.index", "PRICE", v, "-", q, "venue spot index price")
        for h in RET_H:
            add(f"{p}.mid_logret.{h}", "PRICE", v, h, "log", f"ln(mid_T / mid_(T-{h})), as-of on the 1-s grid")
            add(f"{p}.mark_logret.{h}", "PRICE", v, h, "log", f"ln(mark_T / mark_(T-{h}))")
        for w in ("60s", "5m"):
            add(f"{p}.mid_rv.{w}", "PRICE", v, w, "log", f"sqrt(sum r^2) of 1-s mid log returns over {w}")
        for h in MOVE_RV_H:
            add(f"{p}.move_over_rv.{h}", "PRICE", v, h, "sigmas", f"mid_logret_{h} / (rv_5m * sqrt({h} / 300 s))")
        # BASIS
        add(f"{p}.mark_minus_index_bps", "BASIS", v, "-", "bps", "(mark - venue index) / venue index (same venue, same unit)")
        add(f"{p}.mid_minus_index_bps", "BASIS", v, "-", "bps", "(mid - venue index) / venue index")
        add(f"{p}.basis_z_5m", "BASIS", v, "5m", "z", "z-score of mark_minus_index_bps within its own 1-s grid over 5m")
        add(f"{p}.mark_minus_ref_usd", "BASIS", v, "-", "USD", "mark x USDT/USD - Step-3 spot reference (median of fresh Coinbase/Kraken mids)")
        add(f"{p}.mark_minus_ref_bps", "BASIS", v, "-", "bps", "(mark x USDT/USD - ref) / ref")
        add(f"{p}.mark_minus_ref_raw_bps", "BASIS", v, "-", "bps", "(mark - ref) / ref WITHOUT the USDT/USD conversion")
        add(f"{p}.mid_minus_coinbase_bps", "BASIS", v, "-", "bps", "(mid x USDT/USD - Coinbase spot mid) / Coinbase mid")
        add(f"{p}.mid_minus_kraken_bps", "BASIS", v, "-", "bps", "(mid x USDT/USD - Kraken spot mid) / Kraken mid")
        add(f"{p}.mark_minus_cf_bps", "BASIS", v, "-", "bps", "(mark x USDT/USD - CF RTI) / CF RTI")
        for h in BASIS_CHG_H:
            add(f"{p}.basis_change_bps.{h}", "BASIS", v, h, "bps", f"mark_minus_ref_bps(T) - mark_minus_ref_bps(T-{h})")
        for h in BASIS_ACC_H:
            add(f"{p}.basis_accel_bps.{h}", "BASIS", v, h, "bps", f"basis change over {h} minus the previous {h} change")
        add(f"{p}.basis_over_vol", "BASIS", v, "5m", "ratio", "mark_minus_ref_bps / (mid rv_5m in bps)")
        # FUNDING
        add(f"{p}.funding_rate_native", "FUNDING", v, "-", "fraction/interval", "published rate for the upcoming settlement (native)")
        add(f"{p}.funding_rate_8h", "FUNDING", v, "-", "fraction/8h", "native / interval_hours * 8 (None if the interval is unknown)")
        add(f"{p}.funding_rate_annual", "FUNDING", v, "-", "fraction/yr", "simple (non-compounded) annualization")
        add(f"{p}.funding_interval_h", "FUNDING", v, "-", "h", "funding interval in hours")
        add(f"{p}.funding_secs_to_next", "FUNDING", v, "-", "s", "seconds until the next settlement")
        add(f"{p}.funding_predicted_8h", "FUNDING", v, "-", "fraction/8h", "published forecast for the FOLLOWING period (only OKX publishes one)")
        for h in FUND_CHG_H:
            add(f"{p}.funding_change_8h.{h}", "FUNDING", v, h, "fraction/8h", f"rate_8h(T) - rate_8h(T-{h})")
        add(f"{p}.funding_accel_8h.5m", "FUNDING", v, "5m", "fraction/8h", "5m change minus the previous 5m change")
        add(f"{p}.funding_percentile", "FUNDING", v, "session", "fraction", "rank of the current rate_8h among this session's retained observations", "session-relative")
        # OPEN_INTEREST / LIQUIDATION / FLOW / CVD need sizes
        add(f"{p}.oi_coin", "OPEN_INTEREST", v, "-", "coin", "open interest in base coin (never raw contracts)")
        add(f"{p}.oi_notional", "OPEN_INTEREST", v, "-", "USDT", "oi_coin x mark")
        add(f"{p}.oi_age_s", "OPEN_INTEREST", v, "-", "s", "age of the newest OI observation")
        for h in OI_H:
            add(f"{p}.oi_change_coin.{h}", "OPEN_INTEREST", v, h, "coin", f"oi_coin(T) - oi_coin(T-{h})")
            add(f"{p}.oi_pct_change.{h}", "OPEN_INTEREST", v, h, "fraction", f"oi change over {h} / oi level at T-{h}")
            add(f"{p}.oi_notional_change.{h}", "OPEN_INTEREST", v, h, "USDT", f"oi_notional(T) - oi_notional(T-{h})")
        for h in OI_ACC_H:
            add(f"{p}.oi_accel_coin.{h}", "OPEN_INTEREST", v, h, "coin", f"oi change over {h} minus the previous {h} change")
        samp = "lower bound: the venue pushes a sample" if v in ("binance_usdm", "okx_swap") else ""
        for w in LIQ_W:
            for k, u, t in (("forced_sell_notional", "USDT", "notional of forced SELL orders (= long positions liquidated)"),
                            ("forced_buy_notional", "USDT", "notional of forced BUY orders (= short positions liquidated)"),
                            ("long_liq_notional", "USDT", "liquidated LONG positions (documented or derived from the forced-order side)"),
                            ("short_liq_notional", "USDT", "liquidated SHORT positions"),
                            ("total_notional", "USDT", "all liquidations"),
                            ("imbalance", "ratio", "(forced sell - forced buy) / total (UNDEFINED with none)"),
                            ("count", "events", "number of liquidation events"),
                            ("max_notional", "USDT", "largest liquidation (UNDEFINED with none)"),
                            ("accel", "USDT", f"total over (T-{w}, T] minus total over the previous {w}"),
                            ("over_trade_notional", "ratio", "total liquidation notional / aggressive trade notional")):
                add(f"{p}.liq_{k}.{w}", "LIQUIDATION", v, w, u, t, samp)
        for w in FLOW_W:
            for k, u, t in (("buy_notional", "USDT", "aggressive (taker) buy notional"),
                            ("sell_notional", "USDT", "aggressive sell notional"),
                            ("signed_notional", "USDT", "buy - sell"),
                            ("imbalance", "ratio", "(buy - sell) / (buy + sell)"),
                            ("count_imbalance", "ratio", "(n_buy - n_sell) / n"),
                            ("count", "trades", "number of trades"),
                            ("avg_size_coin", "coin", "average aggressive trade size"),
                            ("max_size_coin", "coin", "largest aggressive trade")):
                add(f"{p}.flow_{k}.{w}", "FLOW", v, w, u, t)
        for w in CVD_W:
            add(f"{p}.cvd_notional.{w}", "CVD", v, w, "USDT", f"cumulative signed aggressive notional over (T-{w}, T] (no endless accumulator)")
            add(f"{p}.cvd_slope.{w}", "CVD", v, w, "USDT/s", "OLS slope of the running CVD on the 1-s grid of the window")
            add(f"{p}.cvd_accel.{w}", "CVD", v, w, "USDT", f"cvd over (T-{w}, T] minus cvd over the previous {w}")
        # ORDERBOOK
        for k, u, t in (("bid", q, "best bid"), ("ask", q, "best ask"), ("bid_qty_coin", "coin", "best bid size"),
                        ("ask_qty_coin", "coin", "best ask size"), ("spread_bps", "bps", "(ask - bid) / mid"),
                        ("top_imbalance", "ratio", "(bid_qty - ask_qty) / (bid_qty + ask_qty)"),
                        ("weighted_mid", q, "size-weighted mid (bid*ask_qty + ask*bid_qty) / (bid_qty + ask_qty)"),
                        ("depth_notional_5", "USDT", "sum of price x size over the top 5 levels, both sides"),
                        ("spread_ratio_5m", "ratio", "spread / median spread on the 1-s grid over 5m")):
            add(f"{p}.book_{k}", "ORDERBOOK", v, "-" if k != "spread_ratio_5m" else "5m", u, t)
        for n in DEPTHS:
            add(f"{p}.book_depth_imbalance_{n}", "ORDERBOOK", v, "-", "ratio", f"(sum bid qty - sum ask qty) / total over the top {n} levels")
        for h in BOOK_CHG_H:
            add(f"{p}.book_spread_change_bps.{h}", "ORDERBOOK", v, h, "bps", f"spread_bps(T) - spread_bps(T-{h})")
            add(f"{p}.book_depth_change_pct.{h}", "ORDERBOOK", v, h, "fraction", f"top-5 depth notional change over {h} / its level at T-{h}")
            add(f"{p}.book_pressure_change.{h}", "ORDERBOOK", v, h, "ratio", f"top_imbalance(T) - top_imbalance(T-{h})")
    # CROSS_EXCHANGE
    add("x.n_venues_fresh", "CROSS_EXCHANGE", "*", "-", "count", "perp venues with a fresh mid")
    for h in X_RET_H:
        add(f"x.median_mid_logret.{h}", "CROSS_EXCHANGE", "*", h, "log", "median across venues of mid_logret")
        add(f"x.disagreement_bps.{h}", "CROSS_EXCHANGE", "*", h, "bps", "(max - min) venue mid_logret (UNDEFINED with < 2 venues)")
        add(f"x.max_venue_dev_bps.{h}", "CROSS_EXCHANGE", "*", h, "bps", "largest |venue logret - median| (one venue behaving unusually)")
        add(f"x.sign_agreement.{h}", "CROSS_EXCHANGE", "*", h, "ratio", "(n_up - n_down) / n over venues")
        add(f"x.oi_weighted_logret.{h}", "CROSS_EXCHANGE", "*", h, "log", "mid_logret weighted by oi_notional (size venues)")
    add("x.oi_total_notional", "CROSS_EXCHANGE", "*", "-", "USDT", "sum of oi_notional over size venues (all required)")
    for h in X_OI_H:
        add(f"x.oi_total_change_notional.{h}", "CROSS_EXCHANGE", "*", h, "USDT", "sum of oi_notional_change (all size venues required)")
    add("x.funding_median_8h", "CROSS_EXCHANGE", "*", "-", "fraction/8h", "median of venue rate_8h")
    add("x.funding_dispersion_8h", "CROSS_EXCHANGE", "*", "-", "fraction/8h", "max - min venue rate_8h (UNDEFINED with < 2)")
    add("x.basis_median_bps", "CROSS_EXCHANGE", "*", "-", "bps", "median venue mark_minus_ref_bps")
    add("x.basis_dispersion_bps", "CROSS_EXCHANGE", "*", "-", "bps", "max - min venue mark_minus_ref_bps")
    add("x.spread_median_bps", "CROSS_EXCHANGE", "*", "-", "bps", "median venue spread")
    for w in LIQ_W:
        for k in ("total_notional", "forced_sell_notional", "forced_buy_notional"):
            add(f"x.liq_{k}.{w}", "CROSS_EXCHANGE", "*", w, "USDT", f"sum of venue liq_{k} (all size venues must be observed)",
                "lower bound where a venue samples liquidations")
    for w in CVD_W:
        add(f"x.cvd_total_notional.{w}", "CROSS_EXCHANGE", "*", w, "USDT", "sum of venue cvd_notional (all size venues required)")
    for w in DIV_FLOW_W:
        add(f"x.flow_imbalance.{w}", "CROSS_EXCHANGE", "*", w, "ratio", "sum signed / sum total aggressive notional over size venues")
    # PERP_SPOT_DIVERGENCE
    for h in DIV_H:
        add(f"div.perp_minus_spot_ret.{h}", "PERP_SPOT_DIVERGENCE", "*", h, "log", "x.median_mid_logret - Step-3 spot reference logret")
        add(f"div.perp_minus_cf_ret.{h}", "PERP_SPOT_DIVERGENCE", "*", h, "log", "x.median_mid_logret - CF RTI logret")
    for w in DIV_FLOW_W:
        add(f"div.perp_minus_spot_flow_imb.{w}", "PERP_SPOT_DIVERGENCE", "*", w, "ratio", "x.flow_imbalance - Coinbase spot trade-flow imbalance")
    for w in DIV_RV:
        add(f"div.perp_over_spot_rv.{w}", "PERP_SPOT_DIVERGENCE", "*", w, "ratio", "median venue mid rv / spot reference rv")
    add("div.basis_accel_median_bps.60s", "PERP_SPOT_DIVERGENCE", "*", "60s", "bps", "median venue basis_accel_bps.60s")
    return tuple(d)


FEATURES = _defs()
FEATURE_NAMES = tuple(f.name for f in FEATURES)
FAMILY_OF = {f.name: f.family for f in FEATURES}
assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES), "duplicate perp feature names"
assert set(FAMILY_OF.values()) <= set(FAMILIES)


def names_by_family():
    out = {f: [] for f in FAMILIES}
    for f in FEATURES:
        out[f.family].append(f.name)
    return out
