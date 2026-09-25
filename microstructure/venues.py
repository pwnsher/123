"""
Book venues for Step 5: how each venue's price-level book is delivered, how its continuity can be checked,
and what that implies for trust. RESEARCH ONLY; every transport is a public (or, for Kalshi, read-only
authenticated) websocket subscription plus GET snapshots.

sequence_policy (applied by microstructure.reconstruction, the single place books are rebuilt)
    chain_contiguous   every message on a CHAIN (Coinbase: one websocket connection; Kalshi: one subscription id)
                       carries a sequence number that increases by exactly 1. The adapter reports, per book event,
                       the previous sequence number seen on the chain (or the last one before a discontinuity), so
                       a message lost between two book events - even one for a different product or market on the
                       same chain - is detected at the next book event of the chain. A break invalidates EVERY book
                       of the chain (NEEDS_RESNAPSHOT); recovery = resubscribe (new snapshot).
    binance_diff       REST snapshot (lastUpdateId) + diff stream (U first id, u last id, pu previous u).
                       Deltas received before the snapshot are buffered; deltas with u < lastUpdateId are dropped;
                       the first applied delta must satisfy U <= lastUpdateId <= u; afterwards pu must equal the
                       previous u, else NEEDS_RESNAPSHOT (a new REST snapshot).
    prev_chain         each update carries prevSeqId which must equal the previous seqId of the same book (OKX).
    checksum           no sequence numbers; the venue publishes a CRC32 of the top 10 levels after every update
                       (Kraken). Mismatch -> INVALID -> resubscribe. Needs the pair's price / qty precision (from
                       the instrument channel); without it the book is kept but flagged CHECKSUM_UNVERIFIED.
    monotonic_only     update ids increase but contiguity is not documented (Bybit): a restart snapshot (u = 1)
                       resets the book, a non-increasing id invalidates it, and a LOST delta is NOT detectable
                       from the stream alone (documented limitation; flag SEQUENCE_UNVERIFIABLE).

book_classification (the brief's vocabulary): TRUE_INCREMENTAL_BOOK for all six Step-5 books. None of them is
an ORDER-LEVEL (L3) feed: every size is an aggregate per price level, so queue position is never available.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class BookVenue:
    name: str
    kind: str                      # spot | perp | binary
    family: str                    # SPOT_BOOK | PERP_BOOK | KALSHI_BOOK
    sequence_policy: str
    book_classification: str
    level_type: str                # PRICE_LEVEL (aggregate size per price) - never ORDER_LEVEL here
    price_unit: str
    qty_unit: str
    notional_unit: str
    url: str
    channel: str
    depth_options: Tuple[int, ...]
    default_depth: Optional[int]   # None = the venue's full diff stream (Binance / Coinbase)
    update_interval_ms: Optional[int]   # nominal push interval (None = event driven)
    resnapshot: str                # how a fresh snapshot is obtained
    symbols: Dict[str, str] = field(default_factory=dict)
    stale_after_ms: int = 5000     # no message on the book's connection for this long -> STALE
    documentation: str = ""
    limitations: str = ""


VENUES = {
    "coinbase_l2": BookVenue(
        "coinbase_l2", "spot", "SPOT_BOOK", "chain_contiguous", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "USD", "coin", "USD",
        "wss://advanced-trade-ws.coinbase.com", "level2 (+ heartbeats)", (), None, None, "resubscribe (reconnect)",
        {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}, 5000,
        "Coinbase Advanced Trade market-data websocket, public level2 channel: snapshot then updates with ABSOLUTE "
        "new_quantity per price level (0 removes the level); every message carries a per-connection sequence_num.",
        "Coinbase trades for the same products come from Step 3 (Coinbase Exchange 'matches'); the two endpoints share "
        "the matching engine but not a sequence space."),
    "kraken_book": BookVenue(
        "kraken_book", "spot", "SPOT_BOOK", "checksum", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "USD", "coin", "USD",
        "wss://ws.kraken.com/v2", "book (+ instrument for precisions)", (10, 25, 100, 500, 1000), 25, None,
        "resubscribe (reconnect)", {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD"}, 5000,
        "Kraken v2 book channel: snapshot then updates (qty is the new ABSOLUTE level size, 0 deletes); the local book "
        "is truncated to the subscribed depth after every update; CRC32 checksum of the top 10 levels per update.",
        "no sequence numbers: continuity is established by the checksum only (requires instrument precisions)."),
    "binance_usdm_book": BookVenue(
        "binance_usdm_book", "perp", "PERP_BOOK", "binance_diff", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "USDT", "coin", "USDT",
        "wss://fstream.binance.com/stream", "<symbol>@depth@100ms + GET /fapi/v1/depth", (5, 10, 20, 50, 100, 500, 1000), 1000,
        100, "GET /fapi/v1/depth?limit=1000 (lastUpdateId)",
        {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}, 5000,
        "Binance USD-M diff book depth stream (U, u, pu; absolute quantities, 0 removes) aligned to a REST snapshot per "
        "the documented local-order-book procedure.",
        "REST snapshot weight grows with depth (limit=1000); a snapshot older than the buffered stream forces a new one."),
    "bybit_linear_book": BookVenue(
        "bybit_linear_book", "perp", "PERP_BOOK", "monotonic_only", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "USDT", "coin", "USDT",
        "wss://stream.bybit.com/v5/public/linear", "orderbook.<depth>.<symbol>", (1, 50, 200, 500), 200, 100,
        "resubscribe (reconnect); u=1 snapshot from the venue also resets",
        {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}, 5000,
        "Bybit v5 orderbook channel: snapshot then deltas (absolute sizes, 0 deletes); u = update id, u=1 = venue restart "
        "snapshot.",
        "delta contiguity is not documented: a lost delta cannot be detected from the stream (SEQUENCE_UNVERIFIABLE)."),
    "okx_swap_book": BookVenue(
        "okx_swap_book", "perp", "PERP_BOOK", "prev_chain", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "USDT", "contracts", "USDT",
        "wss://ws.okx.com:8443/ws/v5/public", "books (400 levels)", (400,), 400, 100, "resubscribe (reconnect)",
        {"BTC": "BTC-USDT-SWAP", "ETH": "ETH-USDT-SWAP", "SOL": "SOL-USDT-SWAP", "XRP": "XRP-USDT-SWAP"}, 5000,
        "OKX v5 books channel: snapshot then incremental updates; seqId / prevSeqId chain per book; absolute sizes in "
        "CONTRACTS (0 deletes).",
        "sizes are contracts: coin / notional depth only after a VERIFIED contract value (Step-4 INSTRUMENT events); "
        "the OKX book checksum is deprecated (always 0), so continuity rests on the seqId chain."),
    "kalshi_ws": BookVenue(
        "kalshi_ws", "binary", "KALSHI_BOOK", "chain_contiguous", "TRUE_INCREMENTAL_BOOK", "PRICE_LEVEL", "YES_CENTS",
        "contracts", "USD", "wss://api.elections.kalshi.com/trade-api/ws/v2", "orderbook_delta + trade", (), None, None,
        "resubscribe (a new subscription id)", {}, 120_000,
        "Kalshi websocket orderbook_snapshot / orderbook_delta (delta_fp is a RELATIVE size change at a price) and "
        "public trade channel; seq increases by 1 per message per subscription. Bids only per side; stored in YES terms "
        "(YES asks = 100 - NO bids).",
        "the connection is authenticated (read-only API key, as in Step 3's CF-via-Kalshi feed); a quiet market sends no "
        "messages, so staleness is judged per connection with a long threshold."),
}

BOOK_VENUES = tuple(VENUES)
SPOT_BOOKS = tuple(n for n, v in VENUES.items() if v.kind == "spot")
PERP_BOOKS = tuple(n for n, v in VENUES.items() if v.kind == "perp")
EXCHANGE_BOOKS = SPOT_BOOKS + PERP_BOOKS

# where each book venue's TRADES come from (Steps 3 / 4 already capture spot and perp trades)
TRADE_SOURCE = {"coinbase_l2": "coinbase", "kraken_book": "kraken", "binance_usdm_book": "binance_usdm",
                "bybit_linear_book": "bybit_linear", "okx_swap_book": "okx_swap", "kalshi_ws": "kalshi_ws"}
# Step-4 venue whose INSTRUMENT events carry the contract value for a contracts-sized book
CONTRACT_VALUE_SOURCE = {"okx_swap_book": "okx_swap"}


def venue(name):
    return VENUES[name]
