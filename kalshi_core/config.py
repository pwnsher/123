"""
Configuration classification (Step 1 cleanup).

A. STRATEGY-CRITICAL configuration is NOT duplicated here. It stays exactly where it is (module
   constants in kalshi_dashboard.py) and is pinned by strategy_fingerprint.py and by
   config/strategy_baseline.json (kalshi_core.baseline). strategy_config_snapshot() only READS it.
B. INFRASTRUCTURE configuration: environment variables, listed below with their defaults.
C. SECRETS: environment variables only, never committed, never logged. describe_secrets()
   reports presence (set/unset) and never a value.

No variable in this module can change a strategy value, and none can enable live trading.
"""
import os
from dataclasses import dataclass, asdict

# ---- C. secrets (names only) ----
SECRET_ENV_VARS = (
    "DISCORD_BOT_TOKEN",          # legacy optional Discord adapter (kalshi_bot.py) only
    "KALSHI_API_KEY_ID",          # kalshi_api_learn.py only (read-only prod / demo DRY_RUN learning tool)
)
SENSITIVE_PATH_ENV_VARS = (
    "KALSHI_PRIVATE_KEY_PATH",    # path to the RSA private key used by kalshi_api_learn.py
)

# ---- B. infrastructure variables READ BY THE LEGACY CODE (unchanged; listed for reference) ----
LEGACY_INFRA_ENV_VARS = {
    "KALSHI_PERP_TELEMETRY": "1 (0 disables the passive perp recorder)",
    "KALSHI_PERP_API_BASE": "https://external-api.kalshi.com/trade-api/v2",
    "KALSHI_PERP_TICKER_BTC": "(auto-discovered)", "KALSHI_PERP_TICKER_ETH": "(auto-discovered)",
    "KALSHI_PERP_TICKER_SOL": "(auto-discovered)", "KALSHI_PERP_TICKER_XRP": "(auto-discovered)",
    "KALSHI_PERP_SHADOW": "1 (0 disables the Step 4 shadow journal)",
    "PERP_LIVE_VETO_ENABLED": "0 (suppress-only veto; also needs a valid manual promotion)",
    "PERP_LIVE_VETO_KILL_FILE": "DISABLE_PERP_LIVE_VETO",
    "KALSHI_ENV": "demo (kalshi_api_learn.py only)",
}

# Variables that ACTIVATE a safety-relevant behaviour must be set deliberately in the shell,
# never picked up silently from a .env file (PRODUCTION_RUNBOOK: "this shell only").
NEVER_FROM_DOTENV = ("PERP_LIVE_VETO_ENABLED",)


@dataclass(frozen=True)
class InfraConfig:
    """Infrastructure settings of the local runner (run_local.py). Never strategy values."""
    log_level: str = "INFO"
    log_format: str = "text"            # "text" (key=value) or "json"
    log_file: str = ""                  # empty = stderr only
    decision_journal: str = ""          # empty = off; else JSON-lines file of SignalDecisions
    dashboard_port: int = 8000          # legacy default kalshi_dashboard.PORT
    open_web: bool = True

    @classmethod
    def from_env(cls, environ=None):
        e = os.environ if environ is None else environ
        fmt = (e.get("KALSHI_LOG_FORMAT") or "text").strip().lower()
        try:
            port = int(e.get("KALSHI_DASHBOARD_PORT") or 8000)
        except ValueError:
            port = 8000
        return cls(log_level=(e.get("KALSHI_LOG_LEVEL") or "INFO").strip().upper(),
                   log_format=fmt if fmt in ("text", "json") else "text",
                   log_file=(e.get("KALSHI_LOG_FILE") or "").strip(),
                   decision_journal=(e.get("KALSHI_DECISION_JOURNAL") or "").strip(),
                   dashboard_port=port if 0 < port < 65536 else 8000)

    def to_dict(self):
        return asdict(self)


def describe_secrets(environ=None):
    """{name: "set" | "unset"} — presence only, never the value."""
    e = os.environ if environ is None else environ
    return {n: ("set" if e.get(n) else "unset") for n in SECRET_ENV_VARS + SENSITIVE_PATH_ENV_VARS}


def secret_values(environ=None):
    """Current secret values, for the log redaction filter only (never logged or returned elsewhere)."""
    e = os.environ if environ is None else environ
    return [e[n] for n in SECRET_ENV_VARS if e.get(n) and len(e[n]) >= 6]


def load_dotenv(path=".env", environ=None):
    """Minimal .env loader (no dependency). KEY=VALUE lines; '#' comments; optional quotes.
    Never overrides a variable that is already set, never loads NEVER_FROM_DOTENV.
    Returns (loaded_names, refused_names). Values are never returned or printed."""
    e = os.environ if environ is None else environ
    loaded, refused = [], []
    if not os.path.isfile(path):
        return loaded, refused
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if not key.replace("_", "").isalnum():
                continue
            if key in NEVER_FROM_DOTENV:
                refused.append(key)
                continue
            if key in e:
                continue
            e[key] = val
            loaded.append(key)
    return loaded, refused


def strategy_config_snapshot(dashboard_module):
    """READ the live values of the fingerprinted strategy constants from the imported legacy
    module (includes runtime-mutable web controls). Used for logging only."""
    import strategy_fingerprint as sf
    out = {}
    for name in sf.STRATEGY_CONSTANTS + ("ACTIVE_COINS", "DIRECTION", "POLL_SECONDS"):
        v = getattr(dashboard_module, name, None)
        out[name] = dict(v) if isinstance(v, dict) else v
    return out


DISCORD_TOKEN_PLACEHOLDER = "PASTE_YOUR_BOT_TOKEN_HERE"


def discord_token_usable(token):
    """True only for a non-empty token that is not the shipped placeholder (legacy adapter only)."""
    t = (token or "").strip()
    return bool(t) and t != DISCORD_TOKEN_PLACEHOLDER
