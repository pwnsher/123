"""
LEAKAGE guards for the research matrix (run before any model sees data; a failure stops the run).

    name_scan(columns)                 no label field (settlement.checkpoints.LABEL_FIELDS), no official result /
                                       expiration value, no Step-5 POST-EVENT price-impact label (micro_label.* /
                                       *fwd_*) may ever be a feature column
    value_scan(matrix, rows, names)    TRAIN rows only: a feature that reproduces the outcome (|2 AUC - 1| >= 0.999 over
                                       >= min_markets markets) or equals the settlement value is LEAKAGE_SUSPECT
    availability_scan(rows)            no row's checkpoint is at / after its market close; no row carries a label source
                                       outside the documented set
"""
import re

from feature_eval.labels import LABEL_SOURCES

FORBIDDEN_PATTERNS = (r"^micro_label\.", r"^label\.", r"^y_", r"fwd_", r"forward_", r"official", r"expiration_value",
                      r"^result$", r"reconstructed_outcome", r"final_settlement", r"post_event")


class LeakageError(AssertionError):
    pass


def name_scan(columns):
    from settlement.checkpoints import LABEL_FIELDS
    bad = sorted(c for c in columns if c in LABEL_FIELDS or any(re.search(p, c) for p in FORBIDDEN_PATTERNS))
    return {"ok": not bad, "forbidden_columns": bad}


def _auc(vals, ys):
    pos = [v for v, y in zip(vals, ys) if y == 1]
    neg = [v for v, y in zip(vals, ys) if y == 0]
    if not pos or not neg:
        return None
    allv = sorted((v, y) for v, y in zip(vals, ys))
    rank, i, rs = 1, 0, 0.0
    while i < len(allv):
        j = i
        while j < len(allv) and allv[j][0] == allv[i][0]:
            j += 1
        avg = (rank + rank + (j - i) - 1) / 2
        rs += avg * sum(1 for k in range(i, j) if allv[k][1] == 1)
        rank += j - i
        i = j
    return (rs - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))


def value_scan(rows, names, col_index, min_markets=30, threshold=0.999):
    suspects = []
    for n in names:
        j = col_index[n]
        pairs = [(r["x"][j], r["y"], r["market_ticker"]) for r in rows
                 if r["st"][j] == "READY" and r["x"][j] is not None and r.get("y") is not None]
        if len({m for _, _, m in pairs}) < min_markets:
            continue
        a = _auc([v for v, _, _ in pairs], [y for _, y, _ in pairs])
        if a is not None and abs(2 * a - 1) >= threshold:
            suspects.append({"feature": n, "auc": a})
    return {"ok": not suspects, "leakage_suspects": suspects, "threshold_abs_gini": threshold,
            "note": "a feature that separates the outcome (almost) perfectly on TRAIN rows is treated as leakage"}


def availability_scan(rows):
    bad = [r["market_ticker"] for r in rows if r["checkpoint_ts_ms"] >= r["close_ts_ms"]]
    lab = sorted({r.get("label_source") for r in rows} - set(LABEL_SOURCES) - {None})
    return {"ok": not bad and not lab, "checkpoints_at_or_after_close": bad[:10], "unknown_label_sources": lab}


def guard(columns, rows, candidate_names, col_index, train_rows):
    out = {"name_scan": name_scan(columns), "availability_scan": availability_scan(rows),
           "value_scan": value_scan(train_rows, candidate_names, col_index)}
    failed = {k: v for k, v in out.items() if not v["ok"]}
    out["ok"] = not failed
    if failed:
        raise LeakageError(f"leakage guard failed: {failed}")
    return out
