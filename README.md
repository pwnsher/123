# Kalshi 15m crypto callout watcher — local-first baseline

Decision-support and **paper** trading for Kalshi's 15-minute BTC/ETH/SOL/XRP up/down markets.
It **places no orders**. Discord is optional and legacy. Everything runs locally.

* How it works: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
* Frozen strategy behaviour, thresholds, test baseline: [`docs/BASELINE.md`](docs/BASELINE.md)
* Settlement research layer (Step 2): [`docs/SETTLEMENT_ENGINE.md`](docs/SETTLEMENT_ENGINE.md)
* High-resolution market data + research features (Step 3): [`docs/HIGH_RESOLUTION_DATA.md`](docs/HIGH_RESOLUTION_DATA.md)
* Expanded perpetual-futures telemetry (Step 4): [`docs/PERP_HIGH_RESOLUTION_DATA.md`](docs/PERP_HIGH_RESOLUTION_DATA.md)
* Market microstructure — local order books, order flow, toxicity (Step 5): [`docs/MICROSTRUCTURE.md`](docs/MICROSTRUCTURE.md)
* Future phases (not implemented): [`docs/ROADMAP.md`](docs/ROADMAP.md)
* Perp research steps, runbook: `SETUP.txt`, `PRODUCTION_RUNBOOK.txt`

## Requirements

CPython 3.10–3.13 (checked on 3.10.20, 3.11.15, 3.12.3, 3.13.12) and `pip install requests`.
Optional: `cryptography` (only for `kalshi_api_learn.py` / `make_keys.py`), `discord.py` (only for
the legacy `kalshi_bot.py`), `node` (only for stage 15's browser-behaviour checks; skipped if missing).
No Docker. No credentials are needed for anything below.

## Commands (Windows PowerShell; replace `py` with `python3` on Linux/macOS)

```powershell
# one-time
py -m pip install requests

# tests: every stage suite once, each in its own process (1-21)
py run_all_tests.py
py test_stage16.py            # one stage alone (re-runs the earlier stages once each)

# integrity checks (offline, no network)
py run_local.py --check                                     # config + both fingerprints
py -m kalshi_core.baseline --verify                         # config/strategy_baseline.json vs the code
py -m regression.generate --check                           # 47 behavioural fixtures vs the code
py strategy_fingerprint.py --verify step5_baseline_manifest.json   # legacy fingerprint (Step 6 gate)

# run the watcher + local dashboard (no Discord)  ->  http://127.0.0.1:8000
py run_local.py
py run_local.py --port 8010 --log-level DEBUG
py run_local.py --no-web --journal decisions.jsonl          # watcher only, journal every decision
$env:KALSHI_LOG_FORMAT = "json"; py run_local.py            # JSON logs
py kalshi_dashboard.py                                      # legacy launcher (same threads, print-only)

# evaluation / research (see SETUP.txt for each step)
py kalshi_backtest.py --days 14                             # needs network (Coinbase candles)
py label_binary_outcomes.py
py analyze_perp_quality.py
py analyze_perp_predictive.py
py analyze_perp_shadow.py
py check_perp_deployment.py

# settlement research (Step 2; offline, observation only - see docs/SETTLEMENT_ENGINE.md)
py -m settlement.fingerprint --verify                              # settlement code/policies vs config/settlement_baseline.json
py scripts/settlement_import.py --kind kalshi-markets-json --input settled.json   # captured data -> settlement_data/
py scripts/settlement_import.py --kind kalshi-ws-jsonl --input capture.jsonl --inspect
py scripts/verify_settlement_resolution.py                         # reconstructed vs official Kalshi outcome
py scripts/compare_settlement_overlap.py                           # live vs historical CF observations
py scripts/build_settlement_checkpoints.py --out analysis_output/settlement_checkpoints.csv
py scripts/settlement_validation_report.py                         # analysis_output/settlement_validation.json + .md
py scripts/bench_settlement.py                                     # latency / throughput

# high-resolution market data (Step 3; read-only research capture - see docs/HIGH_RESOLUTION_DATA.md)
py collect_market_data.py --dry-run                                # plan + offline checks, no network
py collect_market_data.py --assets BTC,ETH,SOL,XRP --duration 3600 # CF needs KALSHI_API_KEY_ID + key (+ cryptography)
#   research status page: http://127.0.0.1:8766/  (separate from the production dashboard)
py scripts/replay_market_data.py market_data_sessions\<session> --digest
py scripts/build_market_features.py market_data_sessions\<session> --assets BTC --out analysis_output\features_btc
py scripts/bench_market_data.py
py scripts/mutation_test_market_data.py
py -m market_data.fingerprint --verify

# perpetual-futures research telemetry (Step 4; read-only; never feeds the existing perp veto)
py collect_research_data.py --dry-run                              # Step 3 + Step 4 plan, no network
py collect_research_data.py --assets BTC,ETH,SOL,XRP --cf --coinbase --secondary --kalshi --perps --duration 3600
py scripts/replay_perp_data.py market_data_sessions\<session> --digest --renormalize
py scripts/build_research_dataset.py market_data_sessions\<session> --assets BTC --out analysis_output\research_btc
py scripts/bench_perp_data.py
py scripts/mutation_test_perp_data.py
py -m perp_data.fingerprint --verify

# market microstructure research (Step 5; read-only; never feeds production or the existing perp veto)
py collect_research_data.py --dry-run --all-research               # Steps 3 + 4 + 5 plan, no network
py collect_research_data.py --assets BTC,ETH,SOL,XRP --all-research --compression-level 9
py scripts/replay_microstructure.py market_data_sessions\<session> --digest --renormalize
py scripts/build_micro_dataset.py market_data_sessions\<session> --assets BTC --out analysis_output\joint_btc
py scripts/lead_lag_analysis.py market_data_sessions\<session> --asset BTC --out analysis_output\lead_lag_btc.json
py scripts/prune_sessions.py market_data_sessions --keep-days 14
py scripts/bench_microstructure.py
py scripts/mutation_test_microstructure.py
py -m microstructure.fingerprint --verify

# legacy optional Discord adapter
py -m pip install -U discord.py
$env:DISCORD_BOT_TOKEN = "<your token>"
py kalshi_bot.py
```

Stop with **Ctrl+C**. `run_local.py` logs `event=shutdown` and closes the web server.

## Configuration

* Strategy values live in `kalshi_dashboard.py` and are **frozen**: they are fingerprinted and
  covered by regression fixtures. Do not edit them casually; see `docs/BASELINE.md`.
* Infrastructure and secrets come from environment variables. Names are listed in `.env.example`.
  An optional `.env` next to `run_local.py` is read without overriding the shell.
  `PERP_LIVE_VETO_ENABLED` is never taken from `.env`.
* `.env`, `*.pem` and runtime data files are git-ignored. Never commit keys or tokens.
