# Gem Radar

Paste a token contract into `/gem <contract>` and get a structured **GEM / WATCH / AVOID**
verdict with a 0–100 Gem Score, a separate 0–100 confidence score, red flags, and the
source, timestamp and status of every number.

Gem Radar is an **analytical risk-ranking tool only**. It never connects a wallet, asks for
keys or seed phrases, signs, swaps, buys, sells or places orders.

```
AVOID — 18/100 — Confidence 94/100
CRITICAL FLAGS
- Active mint authority allows additional supply creation.
...
Not financial advice. Heuristic risk analysis only.
```

## How it is built

| Layer | Where | What |
| --- | --- | --- |
| Claude Code mod | `hooks/register.tsx`, `hooks/lib.ts`, `types/index.d.ts` | Registers the slash commands, draws the side panel from session state, makes the DEEP-scan model call |
| Deterministic engine | `src/gem_radar/` (Python 3.10+, standard library only) | Address checks, chain detection, provider adapters, cache, normalization, disagreement, scoring, red flags, confidence, history, watchlist |
| Tests | `tests/test_gem_radar.py` (unittest), `tests/mod.test.ts` (`claude plugin test`) | 45 engine tests and 6 mod tests, all offline |

Collection and scoring are plain code. The model only interprets: it gets the structured
evidence pack and is told to reason from that alone.

```
src/gem_radar/
  core/      enums, types (metric registry), config, errors
  chains/    detect (validation + family), evm (EIP-55 / Keccak), solana (base58)
  data/      http (timeouts, retries, 429, pacing), cache, freshness, provenance,
             aggregator, providers/ (dexscreener, goplus, honeypot_is, rugcheck, rpc, local_files)
  scoring/   normalization, gem_score, red_flags, confidence
  analysis/  fast_scan (chain resolution + evaluation), deep_scan (triggers + evidence pack), disagreement
  commands/  gem, recheck, radar, watch
  storage/   db (SQLite), history, watchlist
  ui/        formatters (text report), panel (panel data)
```

## Supported chains and providers

| Chain | Providers |
| --- | --- |
| Solana | DexScreener, RugCheck, Solana RPC (mint/freeze authority, Token-2022 extensions); GoPlus Solana (deep) |
| Ethereum, BSC, Base | DexScreener, GoPlus, EVM RPC (bytecode); Honeypot.is buy/sell simulation (deep) |
| Arbitrum, Polygon, Optimism, Avalanche | DexScreener, GoPlus, EVM RPC |
| TRON, TON, Sui/Aptos | Detected and reported as **UNSUPPORTED**. Nothing is analysed. |

All providers are public endpoints. The public tiers need no key. Every provider is an
adapter, so scoring never depends on one API's format. `./radar-data/` gives a local
JSON/CSV fallback.

**Chain detection.** A Solana address is base58 that decodes to 32 bytes. An EVM `0x`
address is valid on every EVM chain, so Gem Radar never guesses one. It takes the chain
from DexScreener pairs, or failing that from which chain has bytecode at the address. If
more than one chain matches, the report is `STATUS: AMBIGUOUS` with the candidates, and
you re-run with `--chain <name>`.

## Installation

You need Claude Code 2.1.292 or newer (function-hook mods) and `python3` 3.10+ on `PATH`
(or set `GEM_RADAR_PYTHON`). There is nothing to pip-install.

Install from the marketplace once, at a terminal prompt:

```
/plugin install gem-radar --marketplace pwnsher/123
```

Answer `y` to add the marketplace, then pick the **user** scope. That keeps it in every
future session.

During development, load the plugin from its folder instead:

```
claude --plugin-dir /path/to/123/gem-radar
```

Use `/reload-plugins` after updating an installed copy.

The standalone CLI works without Claude Code. In that mode there's no model
interpretation, and DEEP scans say so.

```
gem-radar/bin/gem-radar scan <contract> [--chain base] [--refresh] [--deep auto|force|off]
```

## Commands

| Command | What it does |
| --- | --- |
| `/gem <contract> [--chain c] [--deep auto\|force\|off]` | Fresh scan. Reuses a provider answer only if it's under 120 s old. |
| `/gem-recheck <contract>` | Bypasses the cache (pacing and rate limits still apply) and shows movement, e.g. `74 → 61 (-13)`, and what changed |
| `/radar` | Opens the side panel: **Scan · Watchlist · History** |
| `/gem-watch [<contract>]` | Adds to the watchlist. With no argument, lists it. |
| `/gem-unwatch <contract>` | Removes from the watchlist |
| `/gem-history <contract>` | Stored scans and what changed between them |

The panel is **display only**. Switching tabs reads session state. It makes no model call,
no provider request and no score change, and the mod tests check this. A scan runs only
when you submit a contract in the Scan tab or run `/gem`. `/radar` reads the local
database with the network switched off.

## Configuration

Copy `config.example.json` to `~/.gem-radar/config.json`, or
point `GEM_RADAR_CONFIG` at the file. You can override any part: verdict bands, critical
cap, thresholds (`dev_critical_pct` defaults to 15), freshness windows, cache TTL,
conflict policy, deep-scan triggers, HTTP timeout/retries, per-provider pacing, provider
reliability, and `providers_disabled`.

Credentials and endpoints come only from environment variables, listed by name in
`.env.example`: `SOLANA_RPC_URL`, `GEM_RADAR_RPC_<CHAIN>`, `HONEYPOT_IS_API_KEY`. Their
values are never logged, stored or printed. `status` lists only which names are set.

## Scoring methodology (Gem Score, 0–100)

| Category | Max | Checks |
| --- | --- | --- |
| Liquidity | 25 | Depth (USD, 15) · LP burned/locked % (10) |
| Contract | 25 | Not a honeypot (6) · taxes or transfer fee (5) · mint authority revoked (5) · EVM: no pause/blacklist (4), verified source (2), not a proxy (1), no owner backdoors (2) · Solana: freeze revoked (4), no dangerous Token-2022 extensions (3), immutable metadata (2) |
| Holders | 20 | Top-10 % (8) · top holder % (4) · dev/deployer % (5) · holder count (3). Pools and burn addresses are excluded. |
| Volume | 15 | 24h volume (5) · volume/liquidity sanity (4; above 15× counts as possible wash) · buy/sell balance (3) · trade count (3) |
| Age/Social | 15 | First pool age (9) · website listed (2) · social links listed (4). These are presence counts from DexScreener, not follower or engagement numbers. |

Each category is its own function. For every check the report shows the raw inputs,
normalized inputs, points, max, missing inputs and warnings.

**UNKNOWN stays UNKNOWN.** Points come only from verified evidence. An unknown input
earns none of its points, is listed under *MISSING / UNKNOWN DATA*, lowers confidence and
never fires a red flag. The report gives the range the score could reach once those
inputs are known. If fewer than 40% of the scoring inputs could be verified and no
critical flag fired, the verdict is **UNRATED** rather than AVOID, because missing data
is not evidence of risk.

**Verdict bands** (configurable): 80–100 GEM · 40–79 WATCH · 0–39 AVOID.

### Example

| | Points |
| --- | --- |
| Liquidity: $620k (13) + LP 97% burned (10) | 23/25 |
| Contract: no honeypot (6), 0%/3% tax (3), mint revoked (5), no pause/blacklist (4), verified (2), not a proxy (1), no backdoors (2) | 23/25 |
| Holders: top10 28% (6), top1 6% (3), dev 0.4% (5), 15k holders (3) | 17/20 |
| Volume: $750k (4), vol/liq 1.2× (4), 900/700 buys/sells (3), 1,600 trades (2) | 13/15 |
| Age/Social: 200-day pool (7), website (2), 2 socials (4) | 13/15 |
| **Total** | **89 → GEM** |

The same token with an **active mint authority** scores `AVOID — 20/100`. The cap is shown
first, under CRITICAL FLAGS.

## Critical red-flag overrides

Any CRITICAL flag sets `final = min(calculated, 20)` and forces **AVOID**. The flag is
printed before any strength.

CRITICAL flags:
- Honeypot evidence; cannot sell all; sell or buy tax ≥ 50%.
- Active mint authority. On EVM, a mint function with an active owner.
- Active freeze authority (Solana).
- Owner can change balances.
- Transfers pausable by an active owner. Blacklist with an active owner is HIGH, or CRITICAL if `blacklist_is_critical` is set.
- Token-2022 permanent delegate; non-transferable token.
- Flagged as rugged.
- Dev/deployer holdings above 15% (configurable).
- Removable liquidity (under 50% locked/burned) while the top 10 hold more than 50%.

HIGH/MEDIUM flags (hidden owner, reclaimable ownership, modifiable tax, transfer hook,
failed sell simulation, proxy, high concentration, suspicious volume, zero sells) appear
under RISKS and don't cap the score.

## Confidence methodology (0–100, separate from the score)

| Part | Max |
| --- | --- |
| Completeness: share of scoring points whose inputs were verified | 40 |
| Freshness: LIVE 1.0 · CACHED 0.8 · CONFLICT 0.6 · STALE 0.1 | 20 |
| Independent sources that returned data (1 → 5, 2 → 10, 3+ → 15) | 15 |
| Mean configured source reliability | 10 |
| Agreement: −8 per conflict on an important metric, −4 per other | 15 |

Completeness below 50% caps confidence at 50, and below 25% caps it at 25. A high score
built on thin data can't show high confidence.

## Data status, cache and disagreement

- **LIVE**: fetched from a provider during this scan.
- **CACHED**: a cached or local value inside its freshness window (market 5 min, liquidity 15 min, holders 1 h, contract 24 h, age 7 d, social 24 h).
- **STALE**: outside its window. It's shown but never scored.
- **UNKNOWN**: no reading.
- **CONFLICT**: independent sources disagree beyond tolerance, e.g. liquidity more than 25% apart, holder % more than 3–5 points apart, any boolean mismatch.

A conflict keeps every reading and is never averaged. The default policy
(`conservative`) scores the riskier reading. The `exclude` policy doesn't score that
metric. Either way confidence drops, and a conflict on an important metric triggers a
DEEP scan.

`/gem` reuses a provider answer under 120 s old. If a provider fails, the last cached
answer is used at its true age and marked CACHED or STALE. Local `radar-data` values are
never LIVE, and a network reading always supersedes them.

## Two-stage analysis

**FAST** (default) uses the essential providers and runs deterministic scoring, flags,
confidence and disagreement checks.

**DEEP** runs when the score is between 40 and 80, confidence is below 60, important
sources disagree, or there are suspicious signals (volume far above liquidity, zero sells,
failed sell simulation, high concentration, unverified mint owner). It adds corroborating
providers (Honeypot.is, GoPlus Solana), re-scores, and then the mod asks a model to
interpret the evidence pack.

The model is chosen at run time. If the session's own model is Opus- or Fable-class it is
used first, then the `opus` alias (Claude Code resolves it to the newest Opus the account
may use), then the session model. No model name is hard-coded. The report records `Mode`,
`Model` (the one that actually answered, or `none (…why)`) and `Reason for escalation`.
Output that calls a token safe, guaranteed or risk-free is withheld.

## Watchlist and history

Every scan is appended to `~/.gem-radar/radar.db` (SQLite, WAL). Each row stores the
contract, chain, time, calculated and final score, confidence, verdict, components, red
flags, normalized evidence with provenance, providers, missing values, conflicts, model and
mode. No secrets are stored.

Adding a contract to the watchlist does **not** start background monitoring. Use
`/gem-recheck` to refresh it. The watchlist and `/gem-history` show movement
(`74 → 61 (-13)`) with what changed: components, flags added or removed, metrics and
source status.

## Local data fallback (`./radar-data/`)

JSON (a list or `{"records": [...]}`) or CSV, with one record per metric:

`source, captured_at (ISO-8601 with zone, or unix), chain, contract, metric, value[, unit]`

Use `unit=fraction` for 0–1 percentages. Invalid records (missing fields, unknown metric,
out-of-range value, zone-less time) are rejected and counted, never repaired. A source
containing "mock" or "test" labels the verdict TEST/MOCK. See `radar-data.example/`.

## Troubleshooting

- **Every provider BLOCKED.** The network refused those hosts. Allow `api.dexscreener.com`, `api.gopluslabs.io`, `api.honeypot.is`, `api.rugcheck.xyz`, your Solana RPC and `*.publicnode.com` (or your own RPCs). Run `gem-radar/bin/gem-radar probe` to check.
- **`STATUS: AMBIGUOUS` / `UNKNOWN` chain.** Re-run with `--chain <name>`.
- **`UNRATED`.** Too little verified data. Check provider status in the report.
- **`Interpretation: UNAVAILABLE`.** No model answered. The deterministic verdict still stands.
- **Commands missing.** Check `claude plugin list`, run `/reload-plugins`, and make sure `python3` works or set `GEM_RADAR_PYTHON`.

## Limitations

- Heuristic only: a high score is not a safety guarantee, and a low one is not proof of fraud.
- Holder concentration comes from providers' top-N lists. Pools and burn addresses are excluded where identifiable. CEX, bridge and locker wallets may not be.
- `dev_pct` on Solana is known only when the creator wallet appears among the top holders.
- LP lock data covers v2-style pools (GoPlus) and RugCheck markets. Concentrated-liquidity positions often read UNKNOWN.
- Unique traders, self-trading detection, wallet clustering and real social engagement aren't measured: there's no reliable free source.
- Provider schemas can change. Adapters reject shapes they don't understand (`PARSE_ERROR`) rather than guess.

## Privacy and security

- Read-only: no wallet, key, signature or trade code exists in this project.
- Inputs are sanitized (an allow-listed character set plus format and checksum validation). Provider responses are parsed as JSON data only and never executed. Only HTTPS is used.
- Credentials come only from environment variables. URL keys are redacted from every message, and none are written to history or cache. The tests check this.
- Data stays local in `~/.gem-radar/`. The only outbound requests go to the providers above, plus the model call for DEEP interpretation through your Claude Code session.

## Development

```
cd gem-radar
python3 -m unittest discover -s tests -v   # engine tests (offline)
claude plugin test .                        # mod tests
claude plugin validate .
ruff check src tests && mypy src/gem_radar
```

*Not financial advice. Heuristic risk analysis only.*
