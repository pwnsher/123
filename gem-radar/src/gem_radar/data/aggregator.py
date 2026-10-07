"""Runs providers (with cache and graceful degradation) and resolves metrics."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

from ..analysis.disagreement import resolve
from ..core.enums import ProviderStatus
from ..core.types import METRICS, Metric, ProviderReport, Reading
from .cache import ProviderCache
from .providers.base import FetchContext, Provider


@dataclass
class Evidence:
    chain: str
    address: str
    collected_at: float
    metrics: dict[str, Metric]
    reports: list[ProviderReport]
    extras: dict[str, dict] = field(default_factory=dict)

    @property
    def is_mock(self) -> bool:
        return any(r.is_mock for m in self.metrics.values() for r in m.readings)

    def value(self, name: str):
        m = self.metrics.get(name)
        return m.value if m is not None and m.usable else None

    def readings(self) -> list[Reading]:
        return [r for rep in self.reports for r in rep.readings]


def _cached(p: Provider, ctx: FetchContext, cache: Optional[ProviderCache], use_cache: bool,
            cfg: dict) -> Optional[ProviderReport]:
    if cache is None or not use_cache or p.is_mock or p.name == "local" or not p.supports(ctx.chain):
        return None
    return cache.get(p.name, ctx.chain, ctx.address, now=ctx.now, cfg=cfg,
                     max_age=float(cfg["cache_ttl_seconds"]))


def _settle(p: Provider, rep: ProviderReport, ctx: FetchContext, cache: Optional[ProviderCache],
            cfg: dict) -> ProviderReport:
    """Main thread only (SQLite): store a fresh answer, or fall back to the cache."""
    cacheable = cache is not None and not p.is_mock and p.name != "local"
    if rep.status == ProviderStatus.AVAILABLE:
        if cacheable:
            cache.put(ctx.chain, ctx.address, rep)  # type: ignore[union-attr]
        return rep
    # Live fetch failed: fall back to the last cached answer at any age (CACHED/STALE).
    if cacheable and rep.status not in (ProviderStatus.UNSUPPORTED, ProviderStatus.NOT_FOUND):
        old = cache.get(p.name, ctx.chain, ctx.address, now=ctx.now, cfg=cfg)  # type: ignore[union-attr]
        if old is not None:
            old.status = rep.status
            old.message = f"{rep.message}; using cached data"
            return old
    return rep


def _run_all(ps: list[Provider], ctx: FetchContext, cache: Optional[ProviderCache],
             use_cache: bool, cfg: dict) -> list[ProviderReport]:
    hits = {id(p): _cached(p, ctx, cache, use_cache, cfg) for p in ps}
    todo = [p for p in ps if hits[id(p)] is None]
    live: dict[int, ProviderReport] = {}
    if len(todo) == 1:
        live[id(todo[0])] = todo[0].run(ctx)
    elif todo:
        with ThreadPoolExecutor(max_workers=min(8, len(todo))) as ex:
            for p, rep in zip(todo, ex.map(lambda q: q.run(ctx), todo), strict=True):
                live[id(p)] = rep
    out = []
    for p in ps:
        hit = hits[id(p)]
        out.append(hit if hit is not None else _settle(p, live[id(p)], ctx, cache, cfg))
    return out


def collect(chain: str, address: str, cfg: dict, providers: list[Provider], *,
            stage: str = "fast", cache: Optional[ProviderCache] = None, use_cache: bool = True,
            ctx: Optional[FetchContext] = None, previous: Optional[list[ProviderReport]] = None
            ) -> Evidence:
    """stage "fast": fast-tier providers. stage "deep": deep-tier providers, merged
    with the reports already collected (`previous`)."""
    ctx = ctx or FetchContext(chain, address, cfg)
    ctx.now = time.time()
    tiers = {"fast": ("fast",), "deep": ("deep",), "all": ("fast", "deep")}[stage]
    chosen = [p for p in providers if p.tier in tiers]
    reports: list[ProviderReport] = list(previous or [])
    # DEX data first: other adapters use its pool addresses to exclude pools from holders.
    first = [p for p in chosen if p.name == "dexscreener"]
    rest = [p for p in chosen if p.name != "dexscreener"]
    reports.extend(_run_all(first, ctx, cache, use_cache, cfg))
    for rep in reports:
        if rep.name == "dexscreener" and rep.extra.get("pairs"):
            ctx.shared.setdefault("pool_addresses", set()).update(
                x["pair"] if chain == "solana" else x["pair"].lower()
                for x in rep.extra["pairs"] if x.get("pair"))
    reports.extend(_run_all(rest, ctx, cache, use_cache, cfg))

    rel = {p.name.split(":")[0]: p.reliability(cfg) for p in providers}
    rel = {**{k: float(v) for k, v in cfg["provider_reliability"].items()}, **rel}
    by_metric: dict[str, list[Reading]] = {k: [] for k in METRICS}
    for rep in reports:
        for r in rep.readings:
            if r.metric in by_metric:
                by_metric[r.metric].append(r)
    metrics = {k: resolve(k, v, rel, cfg.get("conflict_policy", "conservative"))
               for k, v in by_metric.items()}
    extras = {rep.name: rep.extra for rep in reports if rep.extra}
    return Evidence(chain, address, ctx.now, metrics, reports, extras)
