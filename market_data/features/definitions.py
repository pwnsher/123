"""
Feature registry: every research feature, its group, source, window and meaning. RESEARCH_ONLY.

Status of every value (a value is None unless the status is READY or PARTIAL):
    READY        computed from complete inputs
    NOT_READY    warm-up: the collected history does not yet reach back far enough for the window
    MISSING      the inputs should exist but do not (no data, stale feed, gap overlapping the window)
    UNAVAILABLE  the source cannot provide it (e.g. no documented aggressor side, no depth)
    UNDEFINED    mathematically undefined for these inputs (0/0: e.g. imbalance with no trades) —
                 deliberately NOT reported as 0
    PARTIAL      only in the named research mode FeatureConfig(partial_windows=True)

Time conventions: availability = receive_ts <= T (alignment.py). Within a stream, values are placed at
series time (source event time clamped to receive time). Trade windows are (T - w, T]. Grid features
(returns, slope, path, persistence, RV) sample the price AS OF each 1-s grid instant T - k s, using
the latest value at or before the instant, and only if it is at most `asof_max_age_ms` old (a missing
grid point makes the feature MISSING; nothing is forward-filled beyond that bound).
"""
from dataclasses import dataclass, asdict
from enum import Enum

HORIZONS_MS = {"1s": 1000, "5s": 5000, "15s": 15000, "30s": 30000, "60s": 60000, "3m": 180000, "5m": 300000}
RV_WINDOWS = ("5s", "15s", "30s", "60s", "3m", "5m")
VOLUME_WINDOWS = ("5s", "15s", "30s", "60s", "3m")
SLOPE_WINDOWS = ("15s", "60s", "5m")
RV_RATIOS = (("15s", "5m"), ("5s", "60s"), ("60s", "5m"))
XEX_RET_HORIZONS = ("5s", "15s", "60s")
CFSPOT_VEL_HORIZONS = ("5s", "15s")
KALSHI_VEL_HORIZONS = ("5s", "15s", "60s")
KALSHI_SPREAD_HORIZONS = ("15s", "60s")
VOLUME_BASELINE_MS = 900_000
PRICE_SOURCES = ("cf", "coinbase", "kraken", "ref")
RV_SOURCES = ("cf", "coinbase", "kraken", "ref")
TRADE_SOURCES = ("coinbase", "kraken", "kalshi")
STRIKE_SOURCES = ("cf", "coinbase", "ref")
LEADLAG_MAX_LAG_S = 3


class Status(str, Enum):
    READY = "READY"
    NOT_READY = "NOT_READY"
    MISSING = "MISSING"
    UNAVAILABLE = "UNAVAILABLE"
    UNDEFINED = "UNDEFINED"
    PARTIAL = "PARTIAL"


VALUE_STATUSES = (Status.READY, Status.PARTIAL)


@dataclass(frozen=True)
class FeatureConfig:
    asof_max_age_ms: int = 5000            # as-of price lookups (1-s grid) may use a value at most this old
    trade_asof_max_age_ms: int = 60_000    # last-trade lookups
    kalshi_max_age_ms: int = 10_000        # Kalshi market state / book staleness
    source_alive_ms: int = 10_000          # a source with no message for this long is not "observed"
    retention_ms: int = 1_500_000          # history kept in memory (25 min: 15-min volume baseline + margin)
    partial_windows: bool = False          # named research mode; default is strict
    partial_min_coverage: float = 0.8

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class FeatureDef:
    name: str
    group: str
    source: str
    window: str
    unit: str
    description: str


def _defs():
    d = []
    add = lambda n, g, s, w, u, t: d.append(FeatureDef(n, g, s, w, u, t))  # noqa: E731
    for s in PRICE_SOURCES:
        unit = "index points" if s == "cf" else "USD"
        add(f"price.{s}.last", "price", s, "-", unit, {"cf": "latest CF RTI value", "ref": "median of fresh venue mids",
                                                        }.get(s, "latest trade price"))
        if s in ("coinbase", "kraken"):
            add(f"price.{s}.mid", "price", s, "-", unit, "latest (bid+ask)/2")
        for h in HORIZONS_MS:
            add(f"price.{s}.logret.{h}", "price", s, h, "log", f"ln(p_T / p_(T-{h})), p = {'value' if s == 'cf' else 'mid'} as of")
            add(f"price.{s}.ret.{h}", "price", s, h, "fraction", f"p_T / p_(T-{h}) - 1")
            add(f"price.{s}.accel.{h}", "price", s, h, "log", f"logret(T,{h}) - logret(T-{h},{h})")
        for w in SLOPE_WINDOWS:
            add(f"price.{s}.slope.{w}", "price", s, w, "log/s", f"OLS slope of ln p on time over the 1-s grid of {w}")
            add(f"price.{s}.path.{w}", "price", s, w, "log", f"distance travelled: sum |d ln p| over the 1-s grid of {w}")
            add(f"price.{s}.persistence.{w}", "price", s, w, "ratio", f"|ln p_T - ln p_(T-{w})| / path (UNDEFINED if path 0)")
    for s in RV_SOURCES:
        for w in RV_WINDOWS:
            add(f"vol.{s}.rv.{w}", "volatility", s, w, "log", f"sqrt(sum r^2) of 1-s log returns over {w}")
            add(f"vol.{s}.rvar.{w}", "volatility", s, w, "log^2", f"sum r^2 of 1-s log returns over {w}")
            add(f"vol.{s}.absvol.{w}", "volatility", s, w, "log", f"mean |r| of 1-s log returns over {w}")
            add(f"vol.{s}.accel.{w}", "volatility", s, w, "log", f"rv_{w}(T) - rv_{w}(T-{w})")
        for a, b in RV_RATIOS:
            add(f"vol.{s}.ratio.{a}_{b}", "volatility", s, f"{a}/{b}", "ratio",
                f"per-second vol ratio (rv_{a}/sqrt({a})) / (rv_{b}/sqrt({b})) (UNDEFINED if the denominator is 0)")
    for s in TRADE_SOURCES:
        for w in VOLUME_WINDOWS:
            add(f"volume.{s}.vol.{w}", "volume", s, w, "base units" if s != "kalshi" else "contracts", f"sum of trade sizes in (T-{w}, T]")
            add(f"volume.{s}.count.{w}", "volume", s, w, "trades", "number of trades")
            add(f"volume.{s}.avg_size.{w}", "volume", s, w, "size", "vol / count (UNDEFINED with no trades)")
            add(f"volume.{s}.max_size.{w}", "volume", s, w, "size", "largest trade (UNDEFINED with no trades)")
            add(f"volume.{s}.rel.{w}", "volume", s, w, "ratio",
                "volume rate vs the preceding 15-min baseline rate (UNDEFINED if the baseline is empty)")
            add(f"flow.{s}.buy_vol.{w}", "flow", s, w, "size", "aggressive-buy volume")
            add(f"flow.{s}.sell_vol.{w}", "flow", s, w, "size", "aggressive-sell volume")
            add(f"flow.{s}.signed_vol.{w}", "flow", s, w, "size", "buy - sell volume")
            add(f"flow.{s}.imbalance.{w}", "flow", s, w, "ratio", "(buy - sell) / (buy + sell) (UNDEFINED with no volume)")
            add(f"flow.{s}.count_imbalance.{w}", "flow", s, w, "ratio", "(n_buy - n_sell) / n (UNDEFINED with no trades)")
        add(f"flow.{s}.cvd_since_open", "flow", s, "open", "size", "cumulative signed volume since the market open")
    add("xex.cb_minus_kr.last", "cross", "coinbase,kraken", "-", "USD", "last trade Coinbase - Kraken")
    add("xex.cb_minus_kr.mid", "cross", "coinbase,kraken", "-", "USD", "mid Coinbase - Kraken")
    add("xex.cb_minus_kr.mid_bps", "cross", "coinbase,kraken", "-", "bps", "mid divergence in bps of the Kraken mid")
    for h in XEX_RET_HORIZONS:
        add(f"xex.retdiv.{h}", "cross", "coinbase,kraken", h, "log", f"logret_coinbase({h}) - logret_kraken({h})")
    add("xex.dispersion_bps", "cross", "spot", "-", "bps", "(max - min) / median of fresh venue mids")
    add("xex.n_sources", "cross", "spot", "-", "count", "venues with a fresh mid")
    add("xex.leadlag.lag_s", "cross", "coinbase,kraken", "60s", "s",
        f"lag in [-{LEADLAG_MAX_LAG_S}, {LEADLAG_MAX_LAG_S}] maximising corr(r_cb(t), r_kr(t+lag)) of 1-s returns over 60 s")
    add("xex.leadlag.corr", "cross", "coinbase,kraken", "60s", "corr", "that correlation")
    for s in ("coinbase", "kraken", "ref"):
        add(f"cfspot.cf_minus_{s}", "cf_vs_spot", f"cf,{s}", "-", "USD", f"CF RTI - {s} mid")
        add(f"cfspot.cf_minus_{s}_bps", "cf_vs_spot", f"cf,{s}", "-", "bps", f"(CF RTI - {s} mid) / {s} mid")
    for h in XEX_RET_HORIZONS:
        add(f"cfspot.retdiv.{h}", "cf_vs_spot", "cf,ref", h, "log", f"logret_cf({h}) - logret_ref({h})")
    for h in CFSPOT_VEL_HORIZONS:
        add(f"cfspot.div_velocity.{h}", "cf_vs_spot", "cf,ref", h, "bps/s", f"(basis_bps(T) - basis_bps(T-{h})) / {h}")
    add("strike.seconds_remaining", "strike", "kalshi", "-", "s", "(close - T) / 1000")
    for s in STRIKE_SOURCES:
        add(f"strike.{s}.dist", "strike", s, "-", "price", "p - strike")
        add(f"strike.{s}.dist_pct", "strike", s, "-", "fraction", "(p - strike) / strike")
        add(f"strike.{s}.dist_volnorm", "strike", s, "5m", "sigmas",
            "ln(p/strike) / (sigma_1s * sqrt(max(seconds_remaining, 1))), sigma_1s = rv_5m / sqrt(300)")
    for n, u, t in (("current_index", "index points", "newest available CF value"),
                    ("accumulated_mean", "index points", "mean of the settlement samples so far"),
                    ("observations_seen", "count", "CF observations in the settlement lookback"),
                    ("samples_expected", "count", "instants in the settlement window"),
                    ("samples_filled", "count", "observed instants so far"),
                    ("coverage_elapsed", "fraction", "filled / elapsed instants"),
                    ("last_observation_age_s", "s", "age of the newest CF value"),
                    ("seconds_remaining", "s", "seconds to close"),
                    ("accum_minus_strike", "index points", "accumulated_mean - strike"),
                    ("quality", "enum", "settlement quality (Step 2)"),
                    ("phase", "enum", "PRE_WINDOW / IN_WINDOW / CLOSED")):
        add(f"settle.{n}", "settlement", "cf", "-", u, t)
    for n, u, t in (("yes_bid", "cents", "YES bid"), ("yes_ask", "cents", "YES ask"), ("no_bid", "cents", "NO bid"),
                    ("no_ask", "cents", "NO ask"), ("yes_mid", "cents", "(YES bid + ask) / 2"),
                    ("yes_spread", "cents", "YES ask - bid"), ("no_mid", "cents", "(NO bid + ask) / 2"),
                    ("no_spread", "cents", "NO ask - bid"), ("exec_yes_ask", "cents", "executable YES ask"),
                    ("exec_no_ask", "cents", "executable NO ask"), ("implied_prob", "fraction", "yes_mid / 100"),
                    ("model_vs_market", "fraction", "PLACEHOLDER for a future model; always UNAVAILABLE"),
                    ("state_age_s", "s", "age of the market state"),
                    ("depth.yes_bid_qty", "contracts", "top YES bid size"),
                    ("depth.yes_ask_qty", "contracts", "top YES ask size (= top NO bid size)"),
                    ("depth.imbalance", "ratio", "(bid_qty - ask_qty) / (bid_qty + ask_qty)"),
                    ("depth.weighted_mid", "cents", "(bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)"),
                    ("depth.book_age_s", "s", "age of the order book")):
        add(f"kalshi.{n}", "kalshi", "kalshi", "-", u, t)
    for h in KALSHI_VEL_HORIZONS:
        add(f"kalshi.yes_mid_velocity.{h}", "kalshi", "kalshi", h, "cents/s", f"(yes_mid(T) - yes_mid(T-{h})) / {h}")
    for h in KALSHI_SPREAD_HORIZONS:
        add(f"kalshi.spread_change.{h}", "kalshi", "kalshi", h, "cents", f"yes_spread(T) - yes_spread(T-{h})")
    return tuple(d)


FEATURES = _defs()
FEATURE_NAMES = tuple(f.name for f in FEATURES)
assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES), "duplicate feature names"
