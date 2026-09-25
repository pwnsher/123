"""
Microstructure feature registry: name, FAMILY (ten families - the unit of later family-level evaluation / ablation),
venue, window, unit, meaning and a completeness note. RESEARCH ONLY. Statuses are Step 3's (READY / NOT_READY /
MISSING / UNAVAILABLE / UNDEFINED / PARTIAL); a value is None unless READY (or PARTIAL in the named research mode).

Families
    SPOT_BOOK / PERP_BOOK / KALSHI_BOOK   book state from the reconstructed local book (+ its change over windows and
                                          feed telemetry: latency / update count / book age)
    LIQUIDITY          depth within N bps, depth added / removed within the top 10, net depth change, vacuum
    ORDER_FLOW         order-flow imbalance (L1 and 5-level), cancel / execution ESTIMATES
    TRADE_INTENSITY    trade counts / rates, buy-sell volume imbalance, inter-arrival, large trades (causal threshold)
    SWEEP              evidence-based sweeps (levels cleared in one update + same-side trades)
    REPLENISHMENT      REPLENISHMENT_PATTERN counts (a depleted best level restored at the same price) - never
                       called an iceberg
    TOXICITY           VPIN-style trade-sign imbalance (a documented modification, NOT canonical VPIN), trade-sign
                       autocorrelation, causal adverse move
    CROSS_VENUE_MICROSTRUCTURE   dispersion / agreement across the exchange books

Time axis: RECEIVE time. The book at T is the book after every book event received at or before T; windows are
(T - w, T] by receive time. Venue event times are used only for latency telemetry.
Mathematics (documented once here; implemented in features/engine.py):
    microprice      = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)            (L1 sizes)
    imbalance_N     = (B_N - A_N) / (B_N + A_N), B_N / A_N = bid / ask size in the best N levels
    OFI (L1, Cont-Kukanov-Stoikov 2014), per book update n with best bid b, size qb, best ask a, size qa:
        e_n = qb_n*1[b_n >= b_{n-1}] - qb_{n-1}*1[b_n <= b_{n-1}] - qa_n*1[a_n <= a_{n-1}] + qa_{n-1}*1[a_n >= a_{n-1}]
        ofi_l1.w = sum of e_n over updates received in (T - w, T]
    MLOFI (5 levels): the same e_n computed on the m-th best level (m = 1..5) and summed over m (Xu, Gould, Howison
        2018 aggregate form); sizes in base coin (Kalshi: contracts)
    depth added / removed within the top 10: for every level change at a price whose rank (before the change) is < 10,
        increases add to "added", decreases to "removed". Removed depth is NOT called cancellation: it mixes
        cancellations and executions.
    est_cancel_<side>.w = max(0, removed_<side>.w - same-side traded volume.w)   (ESTIMATE; trades and books arrive
        on different connections, so the split is approximate - never labelled exact)
    VPIN-style: session-relative bucket volume V = (volume in the venue's first 10 observed minutes) / 100, fixed
        afterwards; for the last 50 COMPLETE buckets, sum |buy - sell| / (50 V), using the trades' AGGRESSOR flags
        (canonical VPIN uses bulk-volume classification and a daily bucket size - this is a modification). NOT_READY
        until the bucket size is set and 50 buckets are complete.
    adverse move = mean over trades received in [T-65 s, T-5 s] of sign * (mid(t + 5 s) - mid(t)) / mid(t) * 1e4
        (every mid used is received <= T, so it is causal)
    large trade = size above the 95th percentile of the venue's trade sizes received in (T - 15 min, T - 60 s]
        (>= 50 trades, else NOT_READY) - defined causally, never with future trades.
"""
from dataclasses import asdict, dataclass

from market_data.features.definitions import Status  # noqa: F401  (shared vocabulary)
from microstructure.venues import EXCHANGE_BOOKS, VENUES

FAMILIES = ("SPOT_BOOK", "PERP_BOOK", "KALSHI_BOOK", "ORDER_FLOW", "TRADE_INTENSITY", "LIQUIDITY", "SWEEP",
            "REPLENISHMENT", "TOXICITY", "CROSS_VENUE_MICROSTRUCTURE")
SHORT = {"coinbase_l2": "coinbase", "kraken_book": "kraken", "binance_usdm_book": "binance",
         "bybit_linear_book": "bybit", "okx_swap_book": "okx", "kalshi_ws": "kalshi"}
WINDOWS = ("1s", "5s", "15s", "30s", "60s")
WINDOW_MS = {"1s": 1000, "5s": 5000, "15s": 15_000, "30s": 30_000, "60s": 60_000}
CHANGE_H = ("5s", "15s", "60s")
NET_H = ("5s", "60s")
ACCEL_H = ("5s", "15s")
DEPTH_LEVELS = (1, 5, 10, 20)
LIQ_BPS = (10, 25)
TRADE_COUNT_W = ("1s", "5s", "15s", "60s")
IMB_W = ("5s", "60s")
CANCEL_W = ("5s", "60s")
K_DEPTH = (1, 5, 10)
K_LIQ_C = (2, 5)
K_CHG_W = ("5s", "60s")
K_OFI_W = ("5s", "15s", "60s")
K_H = ("15s", "60s")


@dataclass(frozen=True)
class MicroFeatureConfig:
    warmup_ms: int = 1000               # after a (re)snapshot, level features wait this long
    source_alive_ms: int = 10_000       # trades: a venue with no trade-feed message for this long is not observed
    retention_ms: int = 1_200_000       # history kept (20 min: 15-min large-trade baseline + margin)
    large_trade_lookback_ms: int = 900_000
    large_trade_min_n: int = 50
    large_trade_pct: float = 0.95
    vpin_calibration_ms: int = 600_000
    vpin_bucket_div: int = 100
    vpin_buckets: int = 50
    adverse_horizon_ms: int = 5000
    adverse_lookback_ms: int = 65_000
    adverse_min_n: int = 10
    sweep_min_levels: int = 2
    sweep_trade_window_ms: int = 1000
    sweep_min_trade_share: float = 0.5
    replenish_window_ms: int = 2000
    replenish_fraction: float = 0.8
    vacuum_depth_ratio: float = 0.5
    vacuum_spread_ratio: float = 2.0
    baseline_ms: int = 300_000          # 5-min per-second medians for the vacuum / ratio features
    autocorr_min_n: int = 20
    partial_windows: bool = False
    partial_min_coverage: float = 0.8

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class MicroFeatureDef:
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
        d.append(MicroFeatureDef(n, fam, v, w, u, t, c))
    for v in EXCHANGE_BOOKS:
        s = VENUES[v]
        p = f"micro.{SHORT[v]}"
        B = s.family
        q = s.price_unit
        c = "coin"
        cv = "OKX sizes are contracts: coin quantities need a verified contract value (else UNAVAILABLE)" if v == "okx_swap_book" else ""
        # ---- BOOK state ----
        add(f"{p}.book_ready", B, v, "-", "0/1", "1 if the local book is READY at T (valid, warmed up, not stale), else 0")
        for n, u, t in (("best_bid", q, "best bid price"), ("best_ask", q, "best ask price"), ("mid", q, "(bid + ask) / 2"),
                        ("spread", q, "ask - bid"), ("spread_bps", "bps", "relative spread (ask - bid) / mid * 1e4"),
                        ("microprice", q, "L1 size-weighted microprice"),
                        ("microprice_minus_mid_bps", "bps", "(microprice - mid) / mid * 1e4")):
            add(f"{p}.{n}", B, v, "-", u, t)
        for lv in DEPTH_LEVELS:
            add(f"{p}.bid_depth_{lv}", B, v, "-", c, f"bid size in the best {lv} levels", cv)
            add(f"{p}.ask_depth_{lv}", B, v, "-", c, f"ask size in the best {lv} levels", cv)
        for lv in DEPTH_LEVELS:
            add(f"{p}.imbalance_{lv}", B, v, "-", "ratio", f"(B - A) / (B + A) over the best {lv} levels (unit-free)")
        add(f"{p}.total_depth_10", B, v, "-", c, "bid + ask size in the best 10 levels", cv)
        add(f"{p}.next_level_gap_bid_bps", B, v, "-", "bps", "distance from the best bid to the 2nd bid level")
        add(f"{p}.next_level_gap_ask_bps", B, v, "-", "bps", "distance from the best ask to the 2nd ask level")
        add(f"{p}.depth_concentration_bid", B, v, "-", "ratio", "best-level bid size / bid depth 10")
        add(f"{p}.depth_concentration_ask", B, v, "-", "ratio", "best-level ask size / ask depth 10")
        add(f"{p}.depth_slope_bid", B, v, "-", f"{c}/bp", "least-squares slope (through 0) of cumulative bid depth vs distance from mid, top 10", cv)
        add(f"{p}.depth_slope_ask", B, v, "-", f"{c}/bp", "least-squares slope (through 0) of cumulative ask depth vs distance from mid, top 10", cv)
        for h in CHANGE_H:
            add(f"{p}.imbalance_10_change.{h}", B, v, h, "ratio", "imbalance_10(T) - imbalance_10(T - h)")
            add(f"{p}.spread_change_bps.{h}", B, v, h, "bps", "spread_bps(T) - spread_bps(T - h)")
            add(f"{p}.microprice_change_bps.{h}", B, v, h, "bps", "microprice velocity: (mp(T) / mp(T - h) - 1) * 1e4")
        for h in ACCEL_H:
            add(f"{p}.microprice_accel_bps.{h}", B, v, h, "bps", "(mp(T) - mp(T-h)) - (mp(T-h) - mp(T-2h)), in bps of mp(T-h)")
        add(f"{p}.latency_median_ms.60s", B, v, "60s", "ms", "median receive_ts - venue event_ts of book events (clock offset included)")
        add(f"{p}.book_updates.60s", B, v, "60s", "count", "book updates received in (T - 60 s, T]")
        add(f"{p}.book_age_ms", B, v, "-", "ms", "T - receive time of the last applied book event")
        # ---- LIQUIDITY ----
        for bp in LIQ_BPS:
            add(f"{p}.bid_liquidity_{bp}bps", "LIQUIDITY", v, "-", s.notional_unit, f"bid notional within {bp} bps of mid", cv)
            add(f"{p}.ask_liquidity_{bp}bps", "LIQUIDITY", v, "-", s.notional_unit, f"ask notional within {bp} bps of mid", cv)
        for w in WINDOWS:
            for side in ("bid", "ask"):
                add(f"{p}.{side}_depth_added.{w}", "LIQUIDITY", v, w, c, f"{side} size added at levels ranked < 10", cv)
                add(f"{p}.{side}_depth_removed.{w}", "LIQUIDITY", v, w, c,
                    f"{side} size removed at levels ranked < 10 (cancellations AND executions - not 'cancellations')", cv)
        for w in NET_H:
            add(f"{p}.net_bid_depth_change_10.{w}", "LIQUIDITY", v, w, c, "bid_depth_10(T) - bid_depth_10(T - w)", cv)
            add(f"{p}.net_ask_depth_change_10.{w}", "LIQUIDITY", v, w, c, "ask_depth_10(T) - ask_depth_10(T - w)", cv)
        add(f"{p}.depth10_vs_5m_median", "LIQUIDITY", v, "5m", "ratio", "total_depth_10 / median of per-second total_depth_10 over (T - 5 min, T]")
        add(f"{p}.spread_vs_5m_median", "LIQUIDITY", v, "5m", "ratio", "spread_bps / median per-second spread_bps over (T - 5 min, T]")
        add(f"{p}.liquidity_vacuum", "LIQUIDITY", v, "5m", "0/1", "1 if depth ratio < 0.5 AND spread ratio > 2 (thresholds in config)")
        # ---- ORDER_FLOW ----
        for w in WINDOWS:
            add(f"{p}.ofi_l1.{w}", "ORDER_FLOW", v, w, c, "Cont-Kukanov-Stoikov L1 order-flow imbalance summed over updates in the window", cv)
            add(f"{p}.mlofi_5.{w}", "ORDER_FLOW", v, w, c, "multi-level OFI: level-m OFI summed over the best 5 levels", cv)
        for w in CANCEL_W:
            add(f"{p}.est_cancel_bid.{w}", "ORDER_FLOW", v, w, c, "ESTIMATE: max(0, bid removed - sell-aggressor volume)", "estimate, never exact")
            add(f"{p}.est_cancel_ask.{w}", "ORDER_FLOW", v, w, c, "ESTIMATE: max(0, ask removed - buy-aggressor volume)", "estimate, never exact")
        add(f"{p}.est_exec_share_of_removed.60s", "ORDER_FLOW", v, "60s", "ratio", "ESTIMATE: traded volume / removed depth (top 10)", "estimate, never exact")
        # ---- TRADE_INTENSITY ----
        for w in TRADE_COUNT_W:
            add(f"{p}.trade_count.{w}", "TRADE_INTENSITY", v, w, "count", "trades received in the window")
        add(f"{p}.trade_rate_ratio_5s_60s", "TRADE_INTENSITY", v, "60s", "ratio", "(count 5 s / 5) / (count 60 s / 60)")
        for w in IMB_W:
            add(f"{p}.buy_sell_imbalance.{w}", "TRADE_INTENSITY", v, w, "ratio", "(buy - sell aggressor volume) / total")
        add(f"{p}.median_interarrival_ms.60s", "TRADE_INTENSITY", v, "60s", "ms", "median gap between consecutive trade receive times")
        add(f"{p}.large_trade_count.60s", "TRADE_INTENSITY", v, "60s", "count", "trades above the causal large-trade threshold")
        add(f"{p}.large_trade_volume_share.60s", "TRADE_INTENSITY", v, "60s", "ratio", "volume share of large trades")
        # ---- SWEEP ----
        add(f"{p}.sweep_count.60s", "SWEEP", v, "60s", "count",
            "sweeps: >= 2 levels cleared in one update AND same-side trades (<= 1 s before) reaching the 2nd cleared level "
            "with >= 50% of the cleared size")
        add(f"{p}.last_sweep_direction", "SWEEP", v, "5m", "+1/-1", "+1 buy sweep (asks cleared), -1 sell sweep; UNDEFINED if none in 5 min")
        add(f"{p}.last_sweep_levels", "SWEEP", v, "5m", "levels", "levels cleared by the last sweep")
        add(f"{p}.last_sweep_notional", "SWEEP", v, "5m", s.notional_unit, "notional of the cleared levels", cv)
        add(f"{p}.last_sweep_age_ms", "SWEEP", v, "5m", "ms", "T - receive time of the last sweep")
        add(f"{p}.last_sweep_duration_ms", "SWEEP", v, "5m", "ms", "receive-time span of the trades evidencing the last sweep")
        add(f"{p}.last_sweep_impact_bps", "SWEEP", v, "5m", "bps", "mid change across the sweeping update (signed)")
        # ---- REPLENISHMENT ----
        add(f"{p}.replenishment_bid_count.60s", "REPLENISHMENT", v, "60s", "count", "REPLENISHMENT_PATTERN at the best bid (not an iceberg claim)")
        add(f"{p}.replenishment_ask_count.60s", "REPLENISHMENT", v, "60s", "count", "REPLENISHMENT_PATTERN at the best ask (not an iceberg claim)")
        add(f"{p}.replenishment_max_run.60s", "REPLENISHMENT", v, "60s", "count", "longest run of replenishments at one price")
        # ---- TOXICITY ----
        add(f"{p}.vpin_style_50", "TOXICITY", v, "buckets", "ratio", "VPIN-STYLE (trade-sign, session-relative buckets; not canonical VPIN)")
        add(f"{p}.trade_sign_autocorr.60s", "TOXICITY", v, "60s", "corr", "lag-1 autocorrelation of trade signs (>= 20 trades)")
        add(f"{p}.adverse_move_bps", "TOXICITY", v, "65s", "bps", "mean signed 5-s mid move after trades received in [T-65 s, T-5 s]")
    # ---- Kalshi (the row's market; YES terms, cents / contracts) ----
    k, v = "micro.kalshi", "kalshi_ws"
    add(f"{k}.book_ready", "KALSHI_BOOK", v, "-", "0/1", "1 if the market's local book is READY at T")
    for n, u, t in (("yes_bid", "YES_CENTS", "best YES bid"), ("yes_ask", "YES_CENTS", "YES ask = 100 - best NO bid"),
                    ("no_bid", "NO_CENTS", "best NO bid"), ("no_ask", "NO_CENTS", "NO ask = 100 - best YES bid"),
                    ("yes_bid_size", "contracts", "size at the YES bid"), ("yes_ask_size", "contracts", "size at the YES ask (= best NO bid size)"),
                    ("no_bid_size", "contracts", "size at the NO bid"), ("no_ask_size", "contracts", "size at the NO ask (= best YES bid size)"),
                    ("spread_cents", "cents", "YES ask - YES bid (= NO spread)"), ("mid_cents", "YES_CENTS", "(YES bid + YES ask) / 2"),
                    ("microprice_cents", "YES_CENTS", "L1 size-weighted microprice in YES cents"),
                    ("microprice_minus_mid_cents", "cents", "microprice - mid")):
        add(f"{k}.{n}", "KALSHI_BOOK", v, "-", u, t)
    for lv in K_DEPTH:
        add(f"{k}.yes_depth_{lv}", "KALSHI_BOOK", v, "-", "contracts", f"YES bid size in the best {lv} levels")
        add(f"{k}.no_depth_{lv}", "KALSHI_BOOK", v, "-", "contracts", f"NO bid size in the best {lv} levels (= YES ask side)")
        add(f"{k}.imbalance_{lv}", "KALSHI_BOOK", v, "-", "ratio", f"(YES bids - NO bids) / total over the best {lv} levels")
    for h in K_H:
        add(f"{k}.imbalance_5_change.{h}", "KALSHI_BOOK", v, h, "ratio", "imbalance_5(T) - imbalance_5(T - h)")
        add(f"{k}.spread_change_cents.{h}", "KALSHI_BOOK", v, h, "cents", "spread(T) - spread(T - h)")
        add(f"{k}.microprice_change_cents.{h}", "KALSHI_BOOK", v, h, "cents", "microprice(T) - microprice(T - h)")
    add(f"{k}.latency_median_ms.60s", "KALSHI_BOOK", v, "60s", "ms", "median receive_ts - ts_ms of book events")
    add(f"{k}.book_updates.60s", "KALSHI_BOOK", v, "60s", "count", "book updates of this market in (T - 60 s, T]")
    add(f"{k}.book_age_ms", "KALSHI_BOOK", v, "-", "ms", "T - receive time of the market's last book event")
    for c in K_LIQ_C:
        add(f"{k}.yes_liquidity_{c}c", "LIQUIDITY", v, "-", "contracts", f"YES bid contracts within {c} cents of mid")
        add(f"{k}.no_liquidity_{c}c", "LIQUIDITY", v, "-", "contracts", f"NO bid contracts within {c} cents of mid")
    for w in K_CHG_W:
        for side in ("yes", "no"):
            add(f"{k}.{side}_depth_added.{w}", "LIQUIDITY", v, w, "contracts", f"{side.upper()} bid size added (levels ranked < 10)")
            add(f"{k}.{side}_depth_removed.{w}", "LIQUIDITY", v, w, "contracts", f"{side.upper()} bid size removed (not 'cancellations')")
    for w in K_OFI_W:
        add(f"{k}.ofi_l1.{w}", "ORDER_FLOW", v, w, "contracts", "L1 OFI in YES terms")
    for w in K_H:
        add(f"{k}.trade_count.{w}", "TRADE_INTENSITY", v, w, "count", "Kalshi trades in the window")
    add(f"{k}.buy_sell_imbalance.60s", "TRADE_INTENSITY", v, "60s", "ratio", "(YES-buy - YES-sell taker contracts) / total")
    add(f"{k}.vwap_cents.60s", "TRADE_INTENSITY", v, "60s", "YES_CENTS", "volume-weighted trade price")
    add(f"{k}.last_trade_age_ms", "TRADE_INTENSITY", v, "-", "ms", "T - receive time of the market's last trade")
    add(f"{k}.large_trade_count.60s", "TRADE_INTENSITY", v, "60s", "count", "trades above the causal size threshold")
    add(f"{k}.sweep_count.60s", "SWEEP", v, "60s", "count", "Kalshi sweeps (>= 2 levels cleared + same-side trades)")
    add(f"{k}.replenishment_yes_count.60s", "REPLENISHMENT", v, "60s", "count", "REPLENISHMENT_PATTERN at the YES bid")
    add(f"{k}.replenishment_no_count.60s", "REPLENISHMENT", v, "60s", "count", "REPLENISHMENT_PATTERN at the NO bid (YES ask)")
    add(f"{k}.trade_sign_autocorr.60s", "TOXICITY", v, "60s", "corr", "lag-1 autocorrelation of YES-terms trade signs")
    add(f"{k}.adverse_move_cents", "TOXICITY", v, "65s", "cents", "mean signed 5-s YES-mid move after trades in [T-65 s, T-5 s]")
    # ---- CROSS_VENUE_MICROSTRUCTURE ----
    X = "CROSS_VENUE_MICROSTRUCTURE"
    add("micro.x.spot_mid_dispersion_bps", X, "*", "-", "bps", "|Coinbase mid - Kraken mid| / average (both USD)")
    add("micro.x.perp_mid_dispersion_bps", X, "*", "-", "bps", "(max - min) / median of perp mids (all USDT)")
    add("micro.x.imbalance_10_median_spot", X, "*", "-", "ratio", "median imbalance_10 of READY spot books")
    add("micro.x.imbalance_10_median_perp", X, "*", "-", "ratio", "median imbalance_10 of READY perp books")
    add("micro.x.imbalance_10_sign_agreement", X, "*", "-", "ratio", "share of READY exchange books whose imbalance_10 sign equals the median's")
    for w in NET_H:
        add(f"micro.x.ofi_l1_total.{w}", X, "*", w, "coin", "sum of ofi_l1 over exchange books READY for the window")
    add("micro.x.books_ready", X, "*", "-", "count", "number of exchange books READY at T")
    return d


FEATURES = _defs()
FEATURE_NAMES = tuple(f.name for f in FEATURES)
BY_NAME = {f.name: f for f in FEATURES}
assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES), "duplicate micro feature names"
assert all(f.family in FAMILIES for f in FEATURES)


def names_by_family():
    out = {f: [] for f in FAMILIES}
    for f in FEATURES:
        out[f.family].append(f.name)
    return out


def counts_by_family():
    return {k: len(v) for k, v in names_by_family().items()}
