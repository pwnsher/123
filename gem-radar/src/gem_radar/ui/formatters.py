"""Plain-text rendering of verdicts, chain problems, history and watchlist."""
from __future__ import annotations

from collections import Counter

from ..core.types import METRICS
from ..data.provenance import iso

DISCLAIMER = "Not financial advice. Heuristic risk analysis only."
ORDER = [("liquidity", "Liquidity"), ("contract", "Contract"), ("holders", "Holders"),
         ("volume", "Volume"), ("age_social", "Age/Social")]


def _fmt_value(v) -> str:
    if isinstance(v, float):
        return f"{v:,.6g}" if abs(v) < 1e6 else f"{v:,.0f}"
    return str(v)


def render(r: dict) -> str:
    kind = r.get("kind")
    if kind == "rejected":
        return render_rejected(r)
    if kind == "chain":
        return render_chain(r)
    return render_verdict(r)


def render_rejected(r: dict) -> str:
    e = r["error"]
    return (f"REJECTED — {e['code']}\nINPUT: {r['input']}\nREASON: {e['message']}\n"
            f"No analysis was performed.\n\n{DISCLAIMER}")


def render_chain(r: dict) -> str:
    c = r["chain_resolution"]
    lines = [f"CHAIN: {c.get('chain') or 'undetermined'} (address family: {r['family']})",
             f"STATUS: {c['status']}"]
    if c.get("candidates"):
        lines.append(f"CANDIDATES: {', '.join(c['candidates'])}")
    if c.get("method"):
        lines.append(f"METHOD: {c['method']}")
    lines.append(f"REASON: {c.get('reason') or '—'}")
    lines += [f"NOTE: {n}" for n in c.get("notes", [])]
    if c["status"] in ("AMBIGUOUS", "UNKNOWN"):
        lines.append(f"NEXT: /gem {r['input']} --chain <one of the candidates>")
    lines += ["No analysis was performed (nothing is fabricated for an unresolved chain).", "",
              DISCLAIMER]
    return "\n".join(lines)


def render_verdict(r: dict) -> str:
    c = r["confidence"]
    out = [f"{r['verdict']} — {r['final_score']:g}/100 — Confidence {c['score']}/100"]
    sym = (r.get("provider_extras", {}).get("dexscreener") or {}).get("symbol")
    out.append(f"TOKEN: {sym + ' · ' if sym else ''}{r['chain']} · {r['contract']}")
    if r.get("movement"):
        out.append(f"MOVEMENT since previous scan: {r['movement']['line']}")
        out += [f"  · {c}" for c in r["movement"]["changes"]]
    if r["verdict"] == "UNRATED":
        out.append(f"UNRATED: only {c['completeness']:.0%} of scoring inputs could be verified; "
                   "missing data is not treated as good or bad. The score above counts verified "
                   "evidence only.")
    if r.get("is_mock"):
        out.append("!! TEST/MOCK DATA: this verdict uses mock/test readings, not market data.")
    crit = [f for f in r["red_flags"] if f["severity"] == "CRITICAL"]
    if crit:
        out += ["", "CRITICAL FLAGS"]
        out += [f"- {f['message']}" for f in crit]
        if r["cap_applied"]:
            out.append(f"- Score capped at {r['final_score']:g} (calculated "
                       f"{r['calculated_score']:g}) because of the critical flag(s) above.")
        else:
            out.append(f"- Critical flag forces AVOID (calculated score {r['calculated_score']:g}).")
    out += ["", "STRENGTHS"]
    out += [f"{i}. {s}" for i, s in enumerate(r["strengths"], 1)] or ["- none verified"]
    out += ["", "RISKS"]
    out += [f"{i}. {s}" for i, s in enumerate(r["risks"], 1)] or ["- none detected in "
                                                                    "available data"]
    out += ["", "SCORE BREAKDOWN"]
    for key, label in ORDER:
        comp = r["components"][key]
        out.append(f"{label} {'.' * (17 - len(label))} {comp['points']:g}/{comp['max_points']:g}")
    out.append(f"Total ............. {r['calculated_score']:g}/100"
               + (f" → capped {r['final_score']:g}" if r["cap_applied"] else ""))
    if r.get("unscored_points") and not r["cap_applied"] and r["verdict"] != "AVOID":
        lo, hi = r["score_range"]
        out.append(f"Unscored (UNKNOWN inputs) {r['unscored_points']:g} pts — score could range "
                   f"{lo:g}–{hi:g} once they are known")

    statuses = Counter(m["status"] for m in r["metrics"].values() if m["readings"])
    out += ["", "DATA QUALITY",
            f"- confidence: {c['score']}/100 (completeness {c['completeness']:.0%}, "
            f"reliability {c['reliability']:.2f}" + (f", capped at {c['cap']}" if c["cap"] else "")
            + ")",
            "- freshness: " + (", ".join(f"{k} {v}" for k, v in sorted(statuses.items()))
                               or "no data"),
            f"- source count: {c['source_count']} ({', '.join(c['sources']) or 'none'})"]
    if r["conflicts"]:
        out.append(f"- conflicts: {len(r['conflicts'])}")
        for cf in r["conflicts"]:
            vals = "; ".join(f"{x['source']} = {_fmt_value(x['value'])}"
                             for x in cf["disagreement"]["readings"])
            rel = cf["disagreement"].get("relative")
            out.append(f"  · {METRICS[cf['name']].label}: {vals}"
                       + (f" (spread {rel:.0%})" if rel is not None else "")
                       + f" — {cf['note']}")
    else:
        out.append("- conflicts: none")

    out += ["", "MISSING / UNKNOWN DATA"]
    out += [f"- {METRICS[m].label} — {r['missing_detail'].get(m, 'UNKNOWN')}"
            for m in r["missing"]] or ["- none"]

    out += ["", "SOURCES"]
    lines = []
    for name, m in r["metrics"].items():
        for rd in m["readings"]:
            lines.append(f"- {METRICS[name].label} = {_fmt_value(rd['value'])} — {rd['source']} — "
                         f"{iso(rd['fetched_at'])} — "
                         f"{'CONFLICT' if m['status'] == 'CONFLICT' else rd['status']}"
                         + (" — TEST/MOCK" if rd.get("is_mock") else ""))
    out += lines or ["- no readings"]
    out += ["", "PROVIDERS"]
    out += [f"- {p['name']}: {p['status']}" + (f" — {p['message']}" if p["message"] else "")
            for p in r["providers"]]

    a = r["analysis"]
    out += ["", "ANALYSIS", f"Mode: {a['mode']}", f"Model: {a['model']}",
            "Reason for escalation: " + ("; ".join(a["reasons"]) if a["reasons"] else "none")
            + (" (escalation suppressed by --deep off)" if a.get("escalation_suppressed") else "")]
    if a.get("interpretation"):
        out += ["Interpretation (model, from the evidence above only):", a["interpretation"]]
    elif a["mode"] == "DEEP" and a.get("llm"):
        out.append(f"Interpretation: {a['llm'].get('status', 'PENDING')}"
                   + (f" — {a['llm']['note']}" if a["llm"].get("note") else ""))
    if r.get("scan_id"):
        out.append(f"Scan id: {r['scan_id']} · {iso(r['ts'])} · {r.get('duration_s', 0)}s")
    out += ["", DISCLAIMER]
    return "\n".join(out)


def render_history(contract: str, scans: list[dict], diffs: list[tuple[dict, dict, list[str]]]
                   ) -> str:
    if not scans:
        return f"No stored scans for {contract}.\n\n{DISCLAIMER}"
    out = [f"HISTORY — {contract} ({len(scans)} scan(s), newest first)"]
    for s in scans:
        out.append(f"- #{s['id']} {iso(s['ts'])} {s['chain']}: {s['verdict']} {s['final_score']:g}"
                   f"/100 conf {s['confidence']:g} [{s['analysis_mode']}]"
                   + (" TEST/MOCK" if s["is_mock"] else ""))
    if diffs:
        out += ["", "CHANGES"]
        for older, newer, lines in diffs:
            out.append(f"#{older['id']} → #{newer['id']}: {older['final_score']:g} → "
                       f"{newer['final_score']:g} ({newer['final_score'] - older['final_score']:+g})")
            out += [f"  · {line}" for line in lines]
    out += ["", DISCLAIMER]
    return "\n".join(out)


def render_watchlist(entries: list[dict]) -> str:
    if not entries:
        return "Watchlist is empty. Add with /gem-watch <contract>."
    out = ["WATCHLIST (no background monitoring: refresh with /gem-recheck <contract>)"]
    for e in entries:
        if e["current_score"] is None:
            out.append(f"- {e['chain']} {e['contract']}: not scanned yet")
            continue
        mv = e.get("movement") or f"{e['current_score']:g}"
        out.append(f"- {e['chain']} {e['contract']}: {e['verdict']} {mv} · conf "
                   f"{e['confidence']:g} · {iso(e['scanned_at'])}")
        if e.get("changes") and e.get("previous_score") is not None:
            out += [f"    · {c}" for c in e["changes"][:5]]
    return "\n".join(out)
