"""
Venue registry. Adding a venue = one VenueSpec here + one adapter module in sources/ + a synthetic
generator branch; the collector, feature engine, aggregation and dataset are venue-agnostic.

Why these venues
    binance_usdm   Binance USDⓈ-M perpetuals: the largest perp venue by volume for BTC/ETH/SOL/XRP; public
                   websocket with aggregate trades (maker flag), book ticker, partial depth, 1-s mark/index/
                   funding stream and liquidation snapshots; REST open interest / funding history.
    bybit_linear   Bybit USDT perpetuals: second-tier-largest; public v5 websocket with taker-side trades,
                   a ticker stream carrying mark / index / funding / interval / open interest (snapshot +
                   delta), order book and ALL liquidations (not sampled).
    okx_swap       OKX USDT swaps: large, well-documented v5 API with taker-side trades, mark / index /
                   funding (incl. a published NEXT-period forecast when the contract uses it) / open-interest
                   channels and liquidation orders. Sizes are in CONTRACTS - it is the venue that exercises
                   contract-value normalization.
    kalshi_perp    Kalshi's own perpetuals (the market the existing perp research studies): public REST only;
                   bid/ask/last, settlement mark, reference (CF-based) index per contract and a funding
                   estimate. No trades with sides, OI, liquidations or depth are published; per-contract units
                   and funding units are NOT verified, so only ratios (returns, mark/index premium) are used.
    coinbase_usdt  Coinbase USDT-USD ticker: converts USDT-quoted perp prices to USD before comparing with the
                   USD spot reference / CF RTI (USDT/USD deviations are several bps - basis-sized).

Semantics below are from the venues' public API documentation (see docs/PERP_HIGH_RESOLUTION_DATA.md).
"""
from dataclasses import dataclass, field
from typing import Dict, Optional

ASSETS = ("BTC", "ETH", "SOL", "XRP")


@dataclass(frozen=True)
class VenueSpec:
    name: str
    symbols: Dict[str, str]                 # asset -> contract / instrument id
    quote_ccy: str                          # price currency of the contract
    size_unit: str                          # "coin" | "contracts" | "none"
    contract_values: Dict[str, float] = field(default_factory=dict)   # asset -> base coin per contract (static)
    contract_values_verified: bool = False  # static table verified? (else INSTRUMENT metadata must confirm)
    trade_side_semantics: str = "UNAVAILABLE"     # TAKER_SIDE_FIELD | INVERTED_MAKER_SIDE | UNAVAILABLE
    liquidation_side_semantics: str = "UNAVAILABLE"
    liquidation_sampling: str = "none"      # "all" | "sampled" | "none"
    funding_interval_source: str = "unknown"
    default_funding_interval_ms: Optional[int] = None
    publishes_predicted_funding: bool = False
    oi_unit: str = "none"
    book_depth_max: int = 0
    index_symbols: Dict[str, str] = field(default_factory=dict)
    notes: str = ""


VENUES = {
    "binance_usdm": VenueSpec(
        name="binance_usdm", symbols={a: f"{a}USDT" for a in ASSETS}, quote_ccy="USDT", size_unit="coin",
        trade_side_semantics="INVERTED_MAKER_SIDE",          # aggTrade m = "is the buyer the market maker"
        liquidation_side_semantics="FORCED_ORDER_SIDE",      # forceOrder o.S = side of the liquidation order
        liquidation_sampling="sampled",                      # only the LARGEST liquidation per symbol per 1000 ms
        funding_interval_source="fundingInfo_or_default_8h", default_funding_interval_ms=8 * 3_600_000,
        publishes_predicted_funding=False, oi_unit="coin", book_depth_max=20,
        notes="markPrice@1s: p=mark, i=index, r=funding rate for the upcoming settlement at T (estimate)"),
    "bybit_linear": VenueSpec(
        name="bybit_linear", symbols={a: f"{a}USDT" for a in ASSETS}, quote_ccy="USDT", size_unit="coin",
        trade_side_semantics="TAKER_SIDE_FIELD",             # publicTrade S = side of taker
        liquidation_side_semantics="POSITION_SIDE",          # allLiquidation S: "Buy" = a LONG position liquidated
        liquidation_sampling="all",
        funding_interval_source="ticker_fundingIntervalHour", default_funding_interval_ms=None,
        publishes_predicted_funding=False, oi_unit="coin", book_depth_max=50,
        notes="tickers stream = snapshot then deltas (only changed fields); merged per symbol"),
    "okx_swap": VenueSpec(
        name="okx_swap", symbols={a: f"{a}-USDT-SWAP" for a in ASSETS}, quote_ccy="USDT", size_unit="contracts",
        contract_values={"BTC": 0.01, "ETH": 0.1, "SOL": 1.0, "XRP": 100.0}, contract_values_verified=False,
        trade_side_semantics="TAKER_SIDE_FIELD",             # trades side = taker side
        liquidation_side_semantics="ORDER_SIDE_AND_POS_SIDE",  # side = forced order side, posSide = position side
        liquidation_sampling="sampled",
        funding_interval_source="nextFundingTime_minus_fundingTime", publishes_predicted_funding=True,
        oi_unit="contracts", book_depth_max=5,
        index_symbols={a: f"{a}-USDT" for a in ASSETS},
        notes="contract values must be confirmed from /api/v5/public/instruments (ctVal) before sizes are used"),
    "kalshi_perp": VenueSpec(
        name="kalshi_perp", symbols={}, quote_ccy="USD_PER_CONTRACT", size_unit="none",
        funding_interval_source="unverified", publishes_predicted_funding=False, oi_unit="none", book_depth_max=0,
        notes="tickers discovered from /margin/markets; prices per contract (units unverified)"),
}
PERP_VENUES = ("binance_usdm", "bybit_linear", "okx_swap", "kalshi_perp")
SIZE_VENUES = ("binance_usdm", "bybit_linear", "okx_swap")


def spec(name):
    return VENUES[name]
