"""FAST scan: chain resolution, essential metrics, deterministic evaluation."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

from ..chains.detect import SUPPORTED_CHAINS, Detection
from ..chains.evm import EVM_CHAINS
from ..core.enums import ProviderStatus, Severity, Status, Verdict
from ..core.errors import ProviderError
from ..core.types import METRICS
from ..data.aggregator import Evidence
from ..data.providers.base import FetchContext, Provider
from ..data.providers.dexscreener import DexScreener
from ..data.providers.rpc import EvmRPC
from ..scoring import confidence as conf
from ..scoring import gem_score, red_flags


@dataclass
class ChainResolution:
    status: str                 # RESOLVED | AMBIGUOUS | UNKNOWN | UNSUPPORTED
    chain: Optional[str] = None
    method: str = ""
    candidates: list[str] = field(default_factory=list)
    reason: str = ""
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return self.__dict__.copy()


def resolve_chain(det: Detection, requested: Optional[str], cfg: dict,
                  providers: list[Provider], ctx_shared: dict) -> ChainResolution:
    notes = list(det.notes)
    if not det.supported:
        return ChainResolution("UNSUPPORTED", det.candidates[0] if len(det.candidates) == 1
                               else None, "address format", det.candidates, det.reason, notes)
    if requested:
        req = requested.strip().lower()
        if req not in det.candidates:
            return ChainResolution("UNSUPPORTED" if req not in SUPPORTED_CHAINS else "UNKNOWN",
                                   req, "user", det.candidates,
                                   f"a {det.family} address cannot be on {req}"
                                   if req in SUPPORTED_CHAINS else
                                   f"{req} is not a supported chain ({', '.join(SUPPORTED_CHAINS)})",
                                   notes)
        return ChainResolution("RESOLVED", req, "user-specified --chain", [req], "", notes)
    if det.family == "solana":
        return ChainResolution("RESOLVED", "solana", "address format (base58, 32 bytes)",
                               ["solana"], "", notes)

    # EVM: one address, many chains. Resolve from evidence, never by guessing.
    dex = next((p for p in providers if isinstance(p, DexScreener)), None)
    if dex is not None:
        ctx = FetchContext("?", det.address, cfg, shared=ctx_shared)
        try:
            seen = dex.chains_seen(ctx)
        except ProviderError as e:
            notes.append(f"DexScreener unavailable for chain detection ({e.status.value})")
            seen = None
        if seen:
            supported = [c for c in seen if c in EVM_CHAINS]
            unsupported = [c for c in seen if c not in EVM_CHAINS]
            if len(supported) == 1:
                if unsupported:
                    notes.append(f"also listed on unsupported chain(s): {', '.join(unsupported)}")
                return ChainResolution("RESOLVED", supported[0], "DexScreener pairs",
                                       supported, "", notes)
            if len(supported) > 1:
                return ChainResolution("AMBIGUOUS", None, "DexScreener pairs", supported,
                                       "token address has DEX pairs on several chains; "
                                       "re-run with --chain <name>", notes)
            return ChainResolution("UNSUPPORTED", unsupported[0], "DexScreener pairs", unsupported,
                                   f"only listed on unsupported chain(s): {', '.join(unsupported)}",
                                   notes)
    rpc = next((p for p in providers if isinstance(p, EvmRPC)), None)
    if rpc is not None:
        def probe(chain: str) -> Optional[bool]:
            try:
                return rpc.has_code(FetchContext(chain, det.address, cfg))
            except ProviderError:
                return None
        chains = list(EVM_CHAINS)
        with ThreadPoolExecutor(max_workers=len(chains)) as ex:
            results = dict(zip(chains, ex.map(probe, chains), strict=True))
        with_code = [c for c, v in results.items() if v]
        if len(with_code) == 1:
            return ChainResolution("RESOLVED", with_code[0], "bytecode present on one chain",
                                   with_code, "", notes)
        if len(with_code) > 1:
            return ChainResolution("AMBIGUOUS", None, "bytecode present", with_code,
                                   "contract exists on several chains; re-run with --chain <name>",
                                   notes)
        if all(v is None for v in results.values()):
            notes.append("EVM RPC endpoints unreachable for chain detection")
    return ChainResolution("UNKNOWN", None, "", list(EVM_CHAINS),
                           "could not determine which EVM chain this address is on; "
                           "re-run with --chain <name>", notes)


def verdict_for(score: float, cfg: dict) -> Verdict:
    b = cfg["verdict_bands"]
    if score >= b["gem_min"]:
        return Verdict.GEM
    if score >= b["watch_min"]:
        return Verdict.WATCH
    return Verdict.AVOID


def evaluate(ev: Evidence, cfg: dict) -> dict:
    """Deterministic scoring of collected evidence."""
    components = gem_score.score_all(ev.metrics, cfg, ev.chain, ev.collected_at)
    flags = red_flags.detect(ev.metrics, cfg, ev.chain, ev.extras)
    calculated = round(sum(c.points for c in components.values()), 1)
    critical = [f for f in flags if f.severity == Severity.CRITICAL]
    final = min(calculated, float(cfg["critical_cap"])) if critical else calculated
    confidence = conf.compute(components, ev.metrics, ev.reports, cfg)
    missing = sorted({m for c in components.values() for m in c.missing})
    unscored = round(sum(a["max"] for c in components.values() for a in c.awarded
                         if str(a["detail"]).startswith("UNKNOWN")), 1)
    if critical:
        verdict = Verdict.AVOID
    elif confidence["completeness"] < float(cfg["min_completeness_for_verdict"]):
        verdict = Verdict.UNRATED
    else:
        verdict = verdict_for(final, cfg)
    conflicts = [m.to_dict() for m in ev.metrics.values() if m.status == Status.CONFLICT]
    return {
        "calculated_score": calculated,
        "final_score": final,
        "cap_applied": bool(critical) and calculated > final,
        "cap_reason": critical[0].message if critical else None,
        "verdict": verdict.value,
        "unscored_points": unscored,
        "score_range": [final, min(100.0, final + unscored) if not critical else final],
        "components": {k: c.to_dict() for k, c in components.items()},
        "red_flags": [f.to_dict() for f in sorted(flags, key=lambda f: list(Severity).index(
            f.severity))],
        "confidence": confidence,
        "missing": missing,
        "missing_detail": {m: _missing_reason(ev, m) for m in missing},
        "conflicts": conflicts,
        "strengths": strengths(components),
        "risks": risks(components, flags, ev),
    }


def _missing_reason(ev: Evidence, name: str) -> str:
    m = ev.metrics.get(name)
    if m is not None and m.status == Status.STALE:
        return "STALE (only out-of-window readings)"
    if m is not None and m.status == Status.CONFLICT:
        return "CONFLICT (not scored under the exclude policy)"
    return "UNKNOWN (no provider returned it)"


def strengths(components: dict) -> list[str]:
    good = [(a["points"] / a["max"], a["points"], f"{a['check']}: {a['detail']}")
            for c in components.values() for a in c.awarded
            if a["max"] and a["points"] >= 0.7 * a["max"]]
    return [t for _, _, t in sorted(good, key=lambda x: (-x[0], -x[1]))][:5]


def risks(components: dict, flags: list, ev: Evidence) -> list[str]:
    out = [f"[{f.severity.value}] {f.message}" for f in flags if f.severity != Severity.CRITICAL]
    for m in ev.metrics.values():
        if m.status == Status.CONFLICT and m.disagreement:
            vals = ", ".join(f"{r['source']}={r['value']}" for r in m.disagreement["readings"])
            out.append(f"Sources disagree on {METRICS[m.name].label}: {vals}")
    weak = [f"{a['check']}: {a['detail']}" for c in components.values() for a in c.awarded
            if a["max"] and a["points"] == 0 and not str(a["detail"]).startswith("UNKNOWN")]
    return (out + weak)[:6]


def provider_summary(reports: list) -> list[dict]:
    seen: dict[str, dict] = {}
    for r in reports:
        if r.status == ProviderStatus.UNSUPPORTED:
            continue
        seen[r.name] = r.to_dict()
    return list(seen.values())
