# Real-capture regression fixtures

`first_real_session_20260930T004307Z-51664fe9.json` is **derived from first real session 20260930T004307Z-51664fe9**
(about 1.15 minutes; correctly classified DEGRADED). It holds only the minimal public messages needed to reproduce
the Step 6.4 real-feed defects deterministically:

| Key | What it reproduces |
|---|---|
| `cross_source_inversions` | 1–5 ms cross-venue scheduling inversions in the shared collector (must never invalidate / reconnect a book) |
| `clock_processing_order` | older `(wall, mono)` pairs processed after newer ones (must never be a `WALL_BACKWARDS` anomaly) |
| `kalshi_orderbook_fp` | the current Kalshi REST orderbook schema (`orderbook_fp`), incl. an empty YES side |
| `kalshi_zero_quote_market` | a market object with a `0.0000` / size `0.00` YES bid and its `1.0000` NO-ask mirror |
| `kalshi_market_rules` | the verbatim BTC / ETH / SOL / XRP market objects incl. `rules_primary` / `rules_secondary` |
| `http_access_denied` | Binance Futures HTTP 451 and Bybit HTTP 403 endpoints (host + path only) |
| `kalshi_trade_pages` | two consecutive, overlapping 100-trade polls of one market |

Sanitization: public market data only — no credentials, auth headers, account or connection ids, local paths or
session directories. URLs are reduced to host + path where only the endpoint matters, and book deltas are trimmed to
at most two changes. Every retained value is verbatim from the capture. The source store's SHA-256 values are
recorded for traceability. The 28 MB raw session itself is **not** in the repository.

These are **debugging evidence for code regressions only**. They are never research evidence and never model inputs.
