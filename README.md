# Kalshi 15m crypto callout watcher — local-first baseline

Decision-support and **paper** trading for Kalshi's 15-minute BTC/ETH/SOL/XRP up/down markets.
It **places no orders**. Discord is optional and legacy. Everything runs locally.

* How it works: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
* Frozen strategy behaviour, thresholds, test baseline: [`docs/BASELINE.md`](docs/BASELINE.md)
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

# tests: every stage suite once, each in its own process (1-17)
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
