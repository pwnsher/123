"""Scan history (SQLite, append-only) and scan-to-scan diffs."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from ..analysis.disagreement import differs
from ..core.types import METRICS


def save(conn: sqlite3.Connection, r: dict) -> int:
    cur = conn.execute(
        "INSERT INTO scans(contract, chain, ts, calculated_score, final_score, confidence, verdict,"
        " components_json, red_flags_json, evidence_json, providers_json, missing_json,"
        " conflicts_json, model, analysis_mode, escalation_json, interpretation, is_mock)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (r["contract"], r["chain"], r["ts"], r["calculated_score"], r["final_score"],
         r["confidence"]["score"], r["verdict"], json.dumps(r["components"]),
         json.dumps(r["red_flags"]), json.dumps(r["metrics"], default=str),
         json.dumps(r["providers"]), json.dumps(r["missing"]), json.dumps(r["conflicts"]),
         r["analysis"]["model"], r["analysis"]["mode"], json.dumps(r["analysis"]["reasons"]),
         r["analysis"].get("interpretation"), int(bool(r.get("is_mock")))))
    conn.commit()
    return int(cur.lastrowid or 0)


def annotate(conn: sqlite3.Connection, scan_id: int, *, model: str, interpretation: str) -> None:
    conn.execute("UPDATE scans SET model=?, interpretation=? WHERE id=?",
                 (model, interpretation, scan_id))
    conn.commit()


def _row(row: sqlite3.Row) -> dict:
    d = dict(row)
    for k in ("components", "red_flags", "evidence", "providers", "missing", "conflicts",
              "escalation"):
        raw = d.pop(f"{k}_json", None)
        d[k] = json.loads(raw) if raw else None
    d["is_mock"] = bool(d.get("is_mock"))
    return d


def get(conn: sqlite3.Connection, scan_id: int) -> Optional[dict]:
    row = conn.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
    return _row(row) if row else None


def for_contract(conn: sqlite3.Connection, contract: str, chain: Optional[str] = None,
                 limit: int = 20) -> list[dict]:
    q = "SELECT * FROM scans WHERE lower(contract)=lower(?)"
    args: list[Any] = [contract]
    if chain:
        q += " AND chain=?"
        args.append(chain)
    q += " ORDER BY ts DESC, id DESC LIMIT ?"
    args.append(limit)
    return [_row(r) for r in conn.execute(q, args).fetchall()]


def recent(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute("SELECT * FROM scans ORDER BY ts DESC, id DESC LIMIT ?", (limit,))
    return [_row(r) for r in rows.fetchall()]


def diff(prev: dict, cur: dict) -> dict:
    """What changed between two stored scans (prev older)."""
    out: dict[str, Any] = {
        "score": {"from": prev["final_score"], "to": cur["final_score"],
                  "delta": round(cur["final_score"] - prev["final_score"], 1)},
        "verdict": None if prev["verdict"] == cur["verdict"] else
        {"from": prev["verdict"], "to": cur["verdict"]},
        "confidence": {"from": prev["confidence"], "to": cur["confidence"]},
    }
    comps = []
    for k, c in (cur["components"] or {}).items():
        p = (prev["components"] or {}).get(k)
        if p and p["points"] != c["points"]:
            comps.append({"component": c["label"], "from": p["points"], "to": c["points"]})
    out["components"] = comps
    pf = {f["code"] for f in prev["red_flags"] or []}
    cf = {f["code"] for f in cur["red_flags"] or []}
    out["flags_added"] = sorted(cf - pf)
    out["flags_removed"] = sorted(pf - cf)
    changed = []
    pe, ce = prev["evidence"] or {}, cur["evidence"] or {}
    for name, cm in ce.items():
        pm = pe.get(name)
        if name not in METRICS or pm is None:
            continue
        a, b = pm.get("value"), cm.get("value")
        if pm.get("status") != cm.get("status") or (
                a is not None and b is not None and differs(METRICS[name], a, b)) or (
                (a is None) != (b is None)):
            changed.append({"metric": name, "from": a, "to": b,
                            "status": [pm.get("status"), cm.get("status")]})
    out["metrics_changed"] = changed
    ps = {p["name"]: p["status"] for p in prev["providers"] or []}
    cs = {p["name"]: p["status"] for p in cur["providers"] or []}
    out["source_changes"] = [{"provider": k, "from": ps.get(k), "to": cs.get(k)}
                             for k in sorted(set(ps) | set(cs)) if ps.get(k) != cs.get(k)]
    return out


def movement_line(d: dict) -> str:
    s = d["score"]
    sign = "+" if s["delta"] >= 0 else ""
    return f"{s['from']:g} → {s['to']:g} ({sign}{s['delta']:g})"


def explain(d: dict) -> list[str]:
    lines = []
    if d["verdict"]:
        lines.append(f"verdict {d['verdict']['from']} → {d['verdict']['to']}")
    for c in d["components"]:
        lines.append(f"{c['component']}: {c['from']:g} → {c['to']:g}")
    if d["flags_added"]:
        lines.append("flags added: " + ", ".join(d["flags_added"]))
    if d["flags_removed"]:
        lines.append("flags removed: " + ", ".join(d["flags_removed"]))
    for m in d["metrics_changed"][:8]:
        lines.append(f"{METRICS[m['metric']].label}: {m['from']} → {m['to']} "
                     f"({m['status'][0]} → {m['status'][1]})")
    for s in d["source_changes"]:
        lines.append(f"source {s['provider']}: {s['from']} → {s['to']}")
    return lines or ["no material changes"]
