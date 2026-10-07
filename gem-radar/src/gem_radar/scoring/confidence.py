"""Confidence (0-100): how much the data supports the score — separate from it.

  completeness  40  share of scoring points whose inputs were available
  freshness     20  LIVE 1.0, CACHED 0.8, CONFLICT 0.6, STALE 0.1 over gathered inputs
  sources       15  independent providers that returned data (1 -> 5, 2 -> 10, 3+ -> 15)
  reliability   10  mean configured reliability of the providers used
  agreement     15  minus 8 per conflict on an important metric, 4 per other (min 0)

Caps: completeness < 50% caps confidence at 50; under 25% caps at 25.
Unsupported/unknown inputs reduce completeness; TEST/MOCK data is labeled but
not otherwise discounted here (the report says so).
"""
from __future__ import annotations

from ..core.enums import ProviderStatus, Status
from ..core.types import METRICS, CategoryScore, Metric, ProviderReport


def compute(components: dict[str, CategoryScore], metrics: dict[str, Metric],
            reports: list[ProviderReport], cfg: dict) -> dict:
    total_max = sum(c.max_points for c in components.values())
    known_max = sum(a["max"] for c in components.values() for a in c.awarded
                    if not str(a["detail"]).startswith("UNKNOWN"))
    completeness = known_max / total_max if total_max else 0.0

    inputs = {k for c in components.values() for k in c.raw_inputs}
    weights = {Status.LIVE: 1.0, Status.CACHED: 0.8, Status.CONFLICT: 0.6, Status.STALE: 0.1}
    gathered = [metrics[k] for k in inputs if k in metrics and metrics[k].status != Status.UNKNOWN]
    freshness = (sum(weights.get(m.status, 0) for m in gathered) / len(gathered)) if gathered else 0.0

    # A report that carries readings contributed data (live, cached, or a cached fallback).
    used = [r for r in reports if r.readings and r.status != ProviderStatus.UNSUPPORTED]
    names = sorted({r.name.split(":")[0] if not r.name.startswith("mock") else r.name for r in used})
    n = len(names)
    sources_pts = 0 if n == 0 else (5 if n == 1 else (10 if n == 2 else 15))
    rel_cfg = cfg["provider_reliability"]
    rel = (sum(float(rel_cfg.get(x.split(":")[0], 0.5)) for x in names) / n) if n else 0.0

    cross = [m for m in metrics.values()
             if len({r.source for r in m.readings if r.status != Status.STALE}) >= 2]
    conflicts = [m for m in metrics.values() if m.status == Status.CONFLICT]
    penalty = sum(8 if METRICS[m.name].important else 4 for m in conflicts)
    agreement_pts = max(0.0, 15.0 - penalty) if cross else 0.0

    score = 40 * completeness + 20 * freshness + sources_pts + 10 * rel + agreement_pts
    cap = None
    if completeness < 0.25:
        cap = 25
    elif completeness < 0.5:
        cap = 50
    if cap is not None:
        score = min(score, cap)
    return {
        "score": round(max(0.0, min(100.0, score))),
        "completeness": round(completeness, 3),
        "freshness": round(freshness, 3),
        "source_count": n,
        "sources": names,
        "reliability": round(rel, 3),
        "cross_checked_metrics": len(cross),
        "conflicts": len(conflicts),
        "cap": cap,
    }
