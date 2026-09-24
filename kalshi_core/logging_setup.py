"""
Structured, consistent logging for the local runner.

    log_event(log, "signal_generated", coin="BTC", side="UP", conf=91.2)
    text:  2026-09-24T12:00:00Z INFO kalshi.signal event=signal_generated coin=BTC side=UP conf=91.2
    json:  {"ts": "...", "level": "INFO", "logger": "kalshi.signal", "event": "signal_generated", ...}

Every handler carries a RedactingFilter: known secret values (from the environment), PEM
blocks and Discord-token-shaped strings are replaced by "***" before anything is written.
Per-tick events are DEBUG; INFO is reserved for state changes (see run_local.py).
"""
import datetime as dt
import json
import logging
import re

from kalshi_core.config import secret_values

ROOT = "kalshi"
_PEM = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S)
_DISCORD_TOKEN = re.compile(r"\b[MNO][A-Za-z\d_-]{23,27}\.[A-Za-z\d_-]{6}\.[A-Za-z\d_-]{27,}\b")


def redact(text, secrets=()):
    s = str(text)
    for v in secrets:
        if v:
            s = s.replace(v, "***")
    s = _PEM.sub("***PRIVATE KEY REDACTED***", s)
    return _DISCORD_TOKEN.sub("***", s)


class RedactingFilter(logging.Filter):
    def __init__(self, secrets=None):
        super().__init__()
        self.secrets = list(secrets) if secrets is not None else secret_values()

    def filter(self, record):
        record.msg = redact(record.getMessage(), self.secrets)
        record.args = ()
        f = getattr(record, "fields", None)
        if isinstance(f, dict):
            record.fields = {k: (redact(v, self.secrets) if isinstance(v, str) else v) for k, v in f.items()}
        return True


def _ts(record):
    return dt.datetime.fromtimestamp(record.created, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _kv(v):
    if v is None:
        return "null"
    if isinstance(v, float):
        return repr(round(v, 6))
    s = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return json.dumps(s) if (not s or any(c in s for c in ' ="\n')) else s


class KeyValueFormatter(logging.Formatter):
    def format(self, record):
        parts = [_ts(record), record.levelname, record.name]
        ev = getattr(record, "event", None)
        if ev:
            parts.append(f"event={ev}")
        for k, v in (getattr(record, "fields", None) or {}).items():
            parts.append(f"{k}={_kv(v)}")
        msg = record.getMessage()
        if msg and msg != ev:
            parts.append(f"msg={_kv(msg)}")
        out = " ".join(parts)
        if record.exc_info:
            out += "\n" + self.formatException(record.exc_info)
        return out


class JsonFormatter(logging.Formatter):
    def format(self, record):
        d = {"ts": _ts(record), "level": record.levelname, "logger": record.name}
        ev = getattr(record, "event", None)
        if ev:
            d["event"] = ev
        d.update(getattr(record, "fields", None) or {})
        msg = record.getMessage()
        if msg and msg != ev:
            d["msg"] = msg
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, sort_keys=True, default=str)


def configure(level="INFO", fmt="text", log_file="", stream=None, secrets=None):
    """Configure the "kalshi" logger tree. Idempotent (replaces earlier handlers)."""
    import sys
    lg = logging.getLogger(ROOT)
    lg.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    lg.propagate = False
    for h in list(lg.handlers):
        lg.removeHandler(h)
    handlers = [logging.StreamHandler(stream or sys.stderr)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    formatter = JsonFormatter() if fmt == "json" else KeyValueFormatter()
    flt = RedactingFilter(secrets)
    for h in handlers:
        h.setFormatter(formatter)
        h.addFilter(flt)
        lg.addHandler(h)
    return lg


def get_logger(name):
    return logging.getLogger(f"{ROOT}.{name}")


def log_event(logger, event, level=logging.INFO, exc_info=None, **fields):
    logger.log(level, event, extra={"event": event, "fields": fields}, exc_info=exc_info)
