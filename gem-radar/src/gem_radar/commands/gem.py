"""/gem <contract>: validate, resolve chain, FAST scan, optional DEEP stage, persist."""
from __future__ import annotations

import sqlite3
import time
from typing import Optional

from ..analysis import deep_scan, fast_scan
from ..chains.detect import detect
from ..core.config import load_config
from ..core.enums import AnalysisMode
from ..core.errors import GemRadarError
from ..data.aggregator import collect
from ..data.cache import ProviderCache
from ..data.providers import default_providers
from ..data.providers.base import FetchContext, Provider
from ..storage import db, history

NO_MODEL = "none (standalone CLI has no model access; the Claude Code mod runs interpretation)"


def scan(raw: str, *, chain: Optional[str] = None, refresh: bool = False, deep: str = "auto",
         providers: Optional[list[Provider]] = None, cfg: Optional[dict] = None,
         conn: Optional[sqlite3.Connection] = None, persist: bool = True,
         interpreter: bool = False) -> dict:
    """`interpreter=True` when a caller (the Claude Code mod) will run the model
    interpretation of a DEEP scan and store it with `finalize`."""
    cfg = cfg or load_config()
    started = time.time()
    try:
        det = detect(raw)
    except GemRadarError as e:
        return {"kind": "rejected", "input": str(raw)[:200], "error": e.to_dict()}
    providers = providers if providers is not None else default_providers(cfg)
    shared: dict = {}
    res = fast_scan.resolve_chain(det, chain, cfg, providers, shared)
    if res.status != "RESOLVED" or not res.chain:
        return {"kind": "chain", "input": det.input, "address": det.address,
                "family": det.family, "chain_resolution": res.to_dict()}

    conn = conn or db.connect()
    cache = ProviderCache(conn)
    ctx = FetchContext(res.chain, det.address, cfg, shared=shared)
    ev = collect(res.chain, det.address, cfg, providers, stage="fast", cache=cache,
                 use_cache=not refresh, ctx=ctx)
    result = fast_scan.evaluate(ev, cfg)
    reasons = deep_scan.escalation_reasons(result, ev, cfg)
    mode = AnalysisMode.FAST
    if deep == "force" and not reasons:
        reasons = ["deep scan requested"]
    if reasons and deep != "off":
        mode = AnalysisMode.DEEP
        ev = collect(res.chain, det.address, cfg, providers, stage="deep", cache=cache,
                     use_cache=not refresh, ctx=ctx, previous=ev.reports)
        result = fast_scan.evaluate(ev, cfg)
    pack = deep_scan.evidence_pack(result, ev) if mode == AnalysisMode.DEEP else None
    result.update({
        "kind": "verdict",
        "contract": det.address,
        "input": det.input,
        "chain": res.chain,
        "chain_resolution": res.to_dict(),
        "ts": ev.collected_at,
        "duration_s": round(time.time() - started, 2),
        "metrics": {k: m.to_dict() for k, m in ev.metrics.items()},
        "providers": fast_scan.provider_summary(ev.reports),
        "provider_extras": {k: v for k, v in ev.extras.items()
                            if k in ("dexscreener", "rugcheck", "solana_rpc", "local")},
        "is_mock": ev.is_mock,
        "analysis": {
            "mode": mode.value,
            "model": NO_MODEL if mode == AnalysisMode.DEEP else "none (FAST scan: deterministic)",
            "reasons": reasons,
            "escalation_suppressed": bool(reasons) and deep == "off",
            "interpretation": None,
            "llm": None if pack is None else {
                "status": "PENDING" if interpreter else "NOT_RUN",
                "note": "" if interpreter else
                "model interpretation runs only inside the Claude Code mod (/gem)",
                "system": deep_scan.SYSTEM_PROMPT,
                "prompt": deep_scan.build_prompt(pack, reasons),
            },
        },
    })
    result["scan_id"] = history.save(conn, result) if persist else None
    return result
