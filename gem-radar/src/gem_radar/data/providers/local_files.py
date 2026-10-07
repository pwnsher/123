"""Local fallback: JSON / CSV records in ./radar-data (or $GEM_RADAR_DATA_DIR).

Record fields (JSON object or CSV columns):
  source       who produced the value (required; "mock"/"test" in it marks TEST/MOCK data)
  captured_at  ISO-8601 UTC or unix seconds (required)
  chain        e.g. solana, ethereum, base (required)
  contract     token address (required)
  metric       a Gem Radar metric name, e.g. liquidity_usd (required)
  value        the value (required)
  unit         for percent metrics: "pct" (default) or "fraction"

A JSON file is a list of records or {"records": [...]}. Records that fail
validation are rejected and counted, never repaired. Local values are never
LIVE: CACHED inside their freshness window, STALE outside it.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ...core.config import data_dir
from ...core.enums import ProviderStatus
from ...core.types import METRICS, ProviderReport, Reading
from ...scoring import normalization as n
from ..freshness import classify
from .base import FetchContext, Provider

REQUIRED = ("source", "captured_at", "chain", "contract", "metric", "value")


def parse_time(x: Any) -> Optional[float]:
    v = n.num(x)
    if v is not None:
        return v / 1000.0 if v > 1e12 else v
    if isinstance(x, str) and x.strip():
        try:
            dt = datetime.fromisoformat(x.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            return None  # ambiguous local time: reject rather than assume
        return dt.timestamp()
    return None


def parse_value(metric: str, value: Any, unit: str) -> Optional[Any]:
    kind = METRICS[metric].kind
    if kind == "bool":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        return {"true": True, "1": True, "yes": True,
                "false": False, "0": False, "no": False}.get(s)
    if kind == "pct":
        if unit == "fraction":
            return n.pct_from_fraction(value)
        if metric == "price_change_24h_pct":
            return n.signed_pct(value)
        return n.pct_from_percent(value)
    if kind == "usd":
        return n.usd(value)
    if kind == "count":
        return n.count(value)
    if kind == "ts":
        return parse_time(value)
    return n.num(value)


def _same_contract(a: str, b: str, chain: str) -> bool:
    return a.lower() == b.lower() if chain != "solana" else a == b


class LocalFiles(Provider):
    name = "local"
    tier = "fast"

    def __init__(self, directory: Optional[Path] = None):
        self.directory = directory

    def _records(self, path: Path) -> list[dict]:
        if path.suffix.lower() == ".json":
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            recs = data.get("records") if isinstance(data, dict) else data
            return [r for r in recs if isinstance(r, dict)] if isinstance(recs, list) else []
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        d = self.directory or data_dir()
        if not d.is_dir():
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND, message=f"no {d.name}/ folder")
        readings: list[Reading] = []
        rejected: list[str] = []
        files = sorted(p for p in d.iterdir() if p.suffix.lower() in (".json", ".csv"))
        for path in files:
            try:
                records = self._records(path)
            except (OSError, ValueError, csv.Error) as e:
                rejected.append(f"{path.name}: unreadable ({type(e).__name__})")
                continue
            for i, r in enumerate(records):
                if n.chain_id(str(r.get("chain", ""))) != ctx.chain or not _same_contract(
                        str(r.get("contract", "")), ctx.address, ctx.chain):
                    continue  # another token's record: not ours, not an error
                missing = [k for k in REQUIRED if r.get(k) in (None, "")]
                if missing:
                    rejected.append(f"{path.name}#{i}: missing {','.join(missing)}")
                    continue
                metric = str(r["metric"]).strip()
                if metric not in METRICS:
                    rejected.append(f"{path.name}#{i}: unknown metric {metric!r}")
                    continue
                ts = parse_time(r["captured_at"])
                if ts is None:
                    rejected.append(f"{path.name}#{i}: bad captured_at")
                    continue
                unit = str(r.get("unit") or "pct").strip().lower()
                value = parse_value(metric, r["value"], unit)
                if value is None:
                    rejected.append(f"{path.name}#{i}: invalid value for {metric}")
                    continue
                src = str(r["source"]).strip()[:80]
                is_mock = any(t in src.lower() for t in ("mock", "test"))
                readings.append(Reading(
                    metric, value, f"local:{path.name}:{src}", ts,
                    classify(metric, ts, ctx.now, ctx.cfg, live=False), r["value"],
                    "local fallback record", is_mock))
        status = ProviderStatus.AVAILABLE if readings else ProviderStatus.NOT_FOUND
        msg = f"{len(readings)} record(s) from {len(files)} file(s)"
        if rejected:
            msg += f"; {len(rejected)} rejected"
        return ProviderReport(self.name, status, None, msg, readings, {"rejected": rejected[:50]})


class StaticProvider(Provider):
    """TEST/MOCK provider: serves fixed readings. Every reading is labeled mock."""

    is_mock = True

    def __init__(self, name: str, values: dict[str, Any], *, tier: str = "fast",
                 chains: Optional[set[str]] = None, fail: Optional[ProviderStatus] = None,
                 extra: Optional[dict] = None, fetched_at: Optional[float] = None,
                 reliability: float = 0.8):
        self.name = name if name.startswith("mock") else f"mock:{name}"
        self.values = values
        self.tier = tier
        self.chains = chains
        self.fail = fail
        self.extra = extra or {}
        self.fetched_at = fetched_at
        self._rel = reliability
        self.calls = 0

    def reliability(self, cfg: dict) -> float:
        return self._rel

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        from ...core.errors import ProviderError
        self.calls += 1
        if self.fail is not None:
            raise ProviderError(self.name, self.fail, "simulated failure (TEST/MOCK)")
        b = self.builder(ctx)
        if self.fetched_at is not None:
            b.fetched_at = self.fetched_at
        for k, v in self.values.items():
            b.add(k, v, note="TEST/MOCK")
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at, "TEST/MOCK",
                              b.readings, dict(self.extra))
