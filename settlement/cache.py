"""
Settlement research store: append-only JSON Lines with per-record checksums. Offline replay.

Line format (one JSON object per line, UTF-8, "\n" terminated):
    {"store": "kalshi_settlement_store", "v": 1, "kind": <kind>, "data": {...}, "sha": <16 hex>}
kinds: header, session (capture provenance), observation, market, resolution, published_average,
issue. `sha` = first 16 hex of SHA-256 over the canonical JSON of [kind, data].

Writes append whole lines, flushed and fsync'ed; a crash can at worst leave a truncated LAST line,
which the loader reports as CORRUPT_RECORD and skips (never guesses). Derived whole-file outputs use
write_json_atomic (temp file + rename). Timestamps are UTC epoch ms / ISO-8601 with Z. No secrets
are ever written: records are built only from parsed market data (secret-looking keys are refused).
Loading is deterministic: records are returned in a total order, independent of file order.
"""
import datetime as dt
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field

from settlement import RECORD_SCHEMA_VERSION
from settlement.cf_live import PublishedAverage
from settlement.types import OfficialResolution, ParseIssue, SettlementMarket, SettlementObservation

STORE = "kalshi_settlement_store"
STORE_VERSION = 1
KINDS = {"header", "session", "observation", "market", "resolution", "published_average", "issue"}
_SECRET_WORDS = ("secret", "token", "password", "private_key", "api_key", "authorization", "signature")


class StoreError(Exception):
    pass


def _sha(kind, data):
    return hashlib.sha256(json.dumps([kind, data], sort_keys=True, separators=(",", ":"), allow_nan=False)
                          .encode()).hexdigest()[:16]


def _check_no_secrets(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if any(w in str(k).lower() for w in _SECRET_WORDS):
                raise StoreError(f"refusing to store a secret-looking field {path}{k}")
            _check_no_secrets(v, f"{path}{k}.")
    elif isinstance(obj, list):
        for v in obj:
            _check_no_secrets(v, path)


def utc_now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def encode(kind, data):
    if kind not in KINDS:
        raise StoreError(f"unknown record kind {kind!r}")
    _check_no_secrets(data)
    return json.dumps({"store": STORE, "v": STORE_VERSION, "kind": kind, "data": data, "sha": _sha(kind, data)},
                      sort_keys=True, separators=(",", ":"), allow_nan=False)


class SettlementStore:
    def __init__(self, path):
        self.path = path

    def append(self, records, session=None, fsync=True):
        """records: iterable of (kind, dataclass-or-dict). A session record (provenance) precedes them."""
        lines = []
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            lines.append(encode("header", {"store_version": STORE_VERSION, "record_schema_version": RECORD_SCHEMA_VERSION,
                                           "created_utc": utc_now_iso()}))
        if session is not None:
            lines.append(encode("session", dict(session, written_utc=session.get("written_utc") or utc_now_iso())))
        for kind, rec in records:
            lines.append(encode(kind, rec.to_dict() if hasattr(rec, "to_dict") else dict(rec)))
        d = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(d, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="\n") as f:
            if f.tell() > 0 and not _ends_with_newline(self.path):
                f.write("\n")                                 # never glue onto a truncated line
            f.write("".join(line + "\n" for line in lines))
            f.flush()
            if fsync:
                os.fsync(f.fileno())
        return len(lines)


def _ends_with_newline(path):
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        if f.tell() == 0:
            return True
        f.seek(-1, os.SEEK_END)
        return f.read(1) == b"\n"


@dataclass
class LoadedStore:
    observations: list = field(default_factory=list)
    markets: dict = field(default_factory=dict)
    resolutions: dict = field(default_factory=dict)
    published: list = field(default_factory=list)
    issues: list = field(default_factory=list)
    sessions: list = field(default_factory=list)
    corrupt: list = field(default_factory=list)          # [(file, line, reason)]
    conflicts: list = field(default_factory=list)        # metadata that disagreed between records

    def summary(self):
        return {"observations": len(self.observations), "markets": len(self.markets),
                "resolutions": len(self.resolutions), "published_averages": len(self.published),
                "issues": len(self.issues), "sessions": len(self.sessions), "corrupt_records": len(self.corrupt),
                "metadata_conflicts": len(self.conflicts)}


def load(paths):
    """Load one or more store files. Corrupt/unknown lines are reported, never interpreted."""
    if isinstance(paths, str):
        paths = [paths]
    out = LoadedStore()
    seen_obs = set()
    for path in sorted(paths):
        with open(path, encoding="utf-8", newline="") as f:
            for n, line in enumerate(f, start=1):
                raw = line.rstrip("\n")
                if not raw.strip():
                    continue
                try:
                    rec = json.loads(raw)
                except ValueError as e:
                    out.corrupt.append((path, n, f"invalid JSON: {e}"[:120]))
                    continue
                if not isinstance(rec, dict) or rec.get("store") != STORE:
                    out.corrupt.append((path, n, "not a settlement-store record"))
                    continue
                if rec.get("v") != STORE_VERSION:
                    out.corrupt.append((path, n, f"unsupported store version {rec.get('v')!r}"))
                    continue
                kind, data = rec.get("kind"), rec.get("data")
                if kind not in KINDS or not isinstance(data, dict):
                    out.corrupt.append((path, n, f"unknown kind {kind!r}"))
                    continue
                if rec.get("sha") != _sha(kind, data):
                    out.corrupt.append((path, n, "checksum mismatch"))
                    continue
                try:
                    _add(out, kind, data, seen_obs)
                except (TypeError, ValueError) as e:
                    out.corrupt.append((path, n, f"invalid {kind} record: {e}"[:120]))
    out.observations.sort(key=lambda o: o.sort_key())
    out.published.sort(key=lambda p: (p.index_id, p.event_ts_ms, p.kind, p.receive_ts_ms or 0))
    out.issues.sort(key=lambda i: (i.source, i.kind, i.event_ts_ms or 0, i.location, i.detail))
    return out


def _add(out, kind, data, seen_obs):
    if kind == "observation":
        o = SettlementObservation.from_dict(data)
        if o.record_schema_version != RECORD_SCHEMA_VERSION:
            raise ValueError(f"record schema {o.record_schema_version}")
        k = (o.index_id, o.source, o.event_ts_ms, o.value, o.receive_ts_ms, o.amend_ts_ms, o.seq)
        if k not in seen_obs:                        # identical record written twice: keep once
            seen_obs.add(k)
            out.observations.append(o)
    elif kind == "market":
        m = SettlementMarket.from_dict(data)
        prev = out.markets.get(m.ticker)
        if prev is not None and prev != m:
            out.conflicts.append({"ticker": m.ticker, "kind": "market", "kept": prev.to_dict(), "other": m.to_dict()})
        else:
            out.markets[m.ticker] = m
    elif kind == "resolution":
        r = OfficialResolution.from_dict(data)
        prev = out.resolutions.get(r.ticker)
        if prev is not None and prev != r:
            out.conflicts.append({"ticker": r.ticker, "kind": "resolution", "kept": prev.to_dict(), "other": r.to_dict()})
        else:
            out.resolutions[r.ticker] = r
    elif kind == "published_average":
        out.published.append(PublishedAverage.from_dict(data))
    elif kind == "issue":
        out.issues.append(ParseIssue.from_dict(data))
    elif kind == "session":
        out.sessions.append(data)


def write_json_atomic(path, obj):
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(obj, f, indent=1, sort_keys=True, allow_nan=False, default=str)
            f.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
