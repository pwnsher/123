"""DEEP scan: escalation triggers, corroborating providers, and the evidence
pack handed to an LLM for interpretation.

The model never adds facts: it receives only the structured evidence below,
with instructions to reason from it alone. The model call itself happens in
the Claude Code mod (which can detect the session's available models); the
standalone CLI records MODEL as unavailable rather than inventing one.
"""
from __future__ import annotations

import json

from ..core.enums import Status
from ..core.types import METRICS
from ..data.aggregator import Evidence

SUSPICIOUS = {"SUSPICIOUS_VOLUME", "NO_SELLS", "SELL_UNVERIFIED", "HIGH_CONCENTRATION",
              "MINT_FUNCTION_UNVERIFIED_OWNER"}


def escalation_reasons(result: dict, ev: Evidence, cfg: dict) -> list[str]:
    d = cfg["deep_scan"]
    reasons = []
    score = result["final_score"]
    if d["score_low"] <= score <= d["score_high"]:
        reasons.append(f"score {score:g} is in the {d['score_low']}-{d['score_high']} band")
    c = result["confidence"]["score"]
    if c < d["confidence_below"]:
        reasons.append(f"confidence {c} is below {d['confidence_below']}")
    if d.get("on_conflict"):
        important = [m.name for m in ev.metrics.values()
                     if m.status == Status.CONFLICT and METRICS[m.name].important]
        if important:
            reasons.append("sources disagree on " + ", ".join(METRICS[n].label for n in important))
    if d.get("on_suspicious_activity"):
        sus = [f["code"] for f in result["red_flags"] if f["code"] in SUSPICIOUS]
        if sus:
            reasons.append("suspicious signals: " + ", ".join(sus))
    return reasons


SYSTEM_PROMPT = (
    "You are the interpretation step of Gem Radar, a heuristic memecoin risk tool. "
    "You receive a JSON evidence pack produced by deterministic code. Rules: use ONLY facts in "
    "the pack; never invent prices, holders, liquidity, dates, social metrics or events; treat "
    "UNKNOWN as unknown; do not change the score, verdict or flags; never call a token safe, "
    "guaranteed, risk-free or certain to rise; give no buy/sell instructions. Write at most 8 "
    "short lines: what the conflicts or suspicious signals most plausibly mean, which missing "
    "data matters most, and what a careful analyst would verify next."
)


def evidence_pack(result: dict, ev: Evidence) -> dict:
    metrics = {}
    for m in ev.metrics.values():
        if m.status == Status.UNKNOWN:
            continue
        metrics[m.name] = {
            "status": m.status.value,
            "value": m.value,
            "readings": [{"source": r.source, "value": r.value, "status": r.status.value}
                         for r in m.readings],
        }
    return {
        "token": {"chain": ev.chain, "contract": ev.address},
        "verdict": result["verdict"],
        "final_score": result["final_score"],
        "calculated_score": result["calculated_score"],
        "confidence": result["confidence"],
        "components": {k: {"points": v["points"], "max": v["max_points"]}
                       for k, v in result["components"].items()},
        "red_flags": [{"severity": f["severity"], "code": f["code"], "message": f["message"]}
                      for f in result["red_flags"]],
        "metrics": metrics,
        "unknown": result["missing"],
        "conflicts": [{"metric": c["name"], "detail": c["disagreement"]} for c in result["conflicts"]],
        "provider_status": {r.name: r.status.value for r in ev.reports},
        "provider_notes": {k: v for k, v in ev.extras.items() if k in ("rugcheck",)},
        "is_test_mock_data": ev.is_mock,
    }


def build_prompt(pack: dict, reasons: list[str]) -> str:
    return ("Escalation reasons: " + "; ".join(reasons) + "\n\nEvidence pack (JSON):\n"
            + json.dumps(pack, indent=1, default=str))
