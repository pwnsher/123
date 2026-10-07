"""Source disagreement: compare independent readings of one metric.

Contradictory values are never averaged. Under the default "conservative"
policy the riskier reading is scored (and the metric marked CONFLICT); under
"exclude" a conflicted metric is not scored at all.
"""
from __future__ import annotations

from typing import Any, Optional

from ..core.enums import Status
from ..core.types import METRICS, Metric, MetricDef, Reading


def differs(d: MetricDef, a: Any, b: Any) -> bool:
    if d.kind in ("bool", "text"):
        return a != b
    fa, fb = float(a), float(b)
    diff = abs(fa - fb)
    tol = max(d.abs_tol, d.rel_tol * max(abs(fa), abs(fb)))
    return diff > tol


def disagreement_detail(d: MetricDef, readings: list[Reading]) -> dict:
    out: dict[str, Any] = {
        "readings": [{"source": r.source, "value": r.value, "status": r.status.value,
                      "fetched_at": r.fetched_at} for r in readings],
    }
    if d.kind not in ("bool", "text"):
        vals = [float(r.value) for r in readings]
        lo, hi = min(vals), max(vals)
        out["spread"] = round(hi - lo, 6)
        out["relative"] = round((hi - lo) / hi, 4) if hi else None
        out["tolerance"] = {"abs": d.abs_tol, "rel": d.rel_tol}
    return out


def conservative(d: MetricDef, readings: list[Reading]) -> Optional[Any]:
    vals = [r.value for r in readings]
    if d.better == "higher":
        return min(vals)
    if d.better == "lower":
        return max(vals)
    if d.better == "false":          # a risk flag: any True reading is the conservative one
        return any(bool(v) for v in vals)
    return None                       # no safe side (informational): not scored


def resolve(name: str, readings: list[Reading], reliability: dict[str, float],
            policy: str = "conservative") -> Metric:
    d = METRICS[name]
    if not readings:
        return Metric(name, Status.UNKNOWN)
    usable = [r for r in readings if r.status in (Status.LIVE, Status.CACHED)]
    if not usable:
        return Metric(name, Status.STALE, None, readings,
                      note="only stale readings: shown, not scored")
    # Local-file readings only fill gaps: a network reading supersedes them.
    network = [r for r in usable if not r.source.startswith("local:")]
    if network:
        usable = network
    by_source: dict[str, Reading] = {}
    for r in usable:
        by_source.setdefault(r.source, r)
    distinct = list(by_source.values())
    conflict = any(differs(d, a.value, b.value)
                   for i, a in enumerate(distinct) for b in distinct[i + 1:])
    if conflict:
        value = conservative(d, distinct) if policy == "conservative" else None
        return Metric(name, Status.CONFLICT, value, readings, disagreement_detail(d, distinct),
                      note=("scored with the more conservative reading" if value is not None
                            else "not scored while sources disagree"))
    best = max(distinct, key=lambda r: (reliability.get(r.source.split(":")[0], 0.5),
                                        r.status == Status.LIVE))
    status = Status.LIVE if best.status == Status.LIVE else Status.CACHED
    return Metric(name, status, best.value, readings)
