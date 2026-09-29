#!/usr/bin/env python3
"""
Mutation tests for the Step-6 evaluation framework's methodological rules (offline, no network).

    py scripts/mutation_test_step6.py [--out analysis_output/step6_mutation_results.json] [--only S1,S7]

Each mutation breaks ONE rule (S1-S14 of the Step-6 brief, S15-S20 of Step 6.1, S21-S27 of Step 6.2) in a TEMPORARY copy of the repository and runs the relevant
Stage-22 tests there; CAUGHT = those tests fail. Control: the unmutated copy must PASS the same tests. The working tree
is never modified.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip", ".mypy_cache", ".ruff_cache")
SPL = "feature_eval/splits.py"
ABL = "feature_eval/ablation.py"

MUTATIONS = [
    ("S1", "random train / test shuffle (close-time groups shuffled before the chronological cut)",
     [(SPL, "    groups = _close_groups(table)\n    n = len(groups)\n",
       "    groups = _close_groups(table)\n    __import__(\"random\").Random(0).shuffle(groups)\n    n = len(groups)\n", 1)],
     ["splits"]),
    ("S2", "scaler fit on all data (training + test rows standardize the features)",
     [(ABL, "scaler_X=Xtr_, scaler_w=wcc)", "scaler_X=Xtr_ + Xte, scaler_w=wcc + [1.0] * len(Xte))", 2)],
     ["fold"]),
    ("S3", "feature selector reads future folds (screening on training + test rows)",
     [(ABL, "sel, scores = select_features(train_rows, kept,", "sel, scores = select_features(train_rows + test_rows, kept,", 1)],
     ["fold"]),
    ("S4", "holdout influences hyperparameters / model choice (FINAL_HOLDOUT markets enter the walk-forward test blocks)",
     [(SPL, 'dev_groups = _close_groups([e for e in usable if e["market"] in dev])',
       'dev_groups = _close_groups([e for e in table if e["market"] in dev or e["market"] in holdout])', 1)],
     ["splits"]),
    ("S5", "row-level bootstrap instead of market clusters",
     [("feature_eval/bootstrap.py", "    g = groups(markets)\n    keys = resampling_units(markets)\n",
       "    g = {i: [i] for i in range(len(markets))}\n    keys = list(g)\n", 1)],
     ["bootstrap"]),
    ("S6", "the settlement result enters the features (the label is copied into a feature column)",
     [("feature_eval/dataset.py", '            y, src = market_label(labels.get(tk, {}), gate.get("status"))\n',
       '            y, src = market_label(labels.get(tk, {}), gate.get("status"))\n'
       '            x[0], s[0] = (float(y) if y is not None else None), "READY"\n', 1)],
     ["dataset"]),
    ("S7", "a post-event price-impact label enters the features undetected (name and value leakage scans disabled)",
     [("feature_eval/leakage.py", 'FORBIDDEN_PATTERNS = (r"^micro_label\\.", r"^label\\.", r"^y_", r"fwd_", r"forward_",',
       'FORBIDDEN_PATTERNS = (r"^label\\.", r"^y_", r"forward_",', 1),
      ("feature_eval/leakage.py", "if a is not None and abs(2 * a - 1) >= threshold:", "if False:", 1)],
     ["leakage"]),
    ("S8", "synthetic results treated as real (the synthetic flag is ignored when writing the real outputs)",
     [("feature_eval/report.py", "    return bool(isinstance(section, dict) and (section.get(\"synthetic_only\") or",
       "    return False and bool(isinstance(section, dict) and (section.get(\"synthetic_only\") or", 1)],
     ["outputs"]),
    ("S9", "missing values silently replaced with zero (raw 0 instead of the training mean + indicator)",
     [("feature_eval/models.py", "out.append(0.0)                              # standardized TRAIN MEAN",
       "out.append((0.0 - self.mean[j]) / self.std[j])  # raw zero", 1)],
     ["missing"]),
    ("S10", "dataset fingerprint ignores a session (only the first session is hashed)",
     [("feature_eval/dataset.py", "for s in session_meta),", "for s in session_meta[:1]),", 1)],
     ["dsfp"]),
    ("S11", "holdout reused after tuning (the single-use holdout check is bypassed)",
     [("feature_eval/ledger.py", "prior = self.holdout_accesses(dataset_fingerprint)", "prior = []", 1)],
     ["ledger"]),
    ("S12", "feature manifest changed without a version / fingerprint change (records comparison skipped)",
     [("feature_eval/universe.py", 'if stored.get("records_sha256") != cur["records_sha256"]:', "if False:", 1)],
     ["universe"]),
    ("S13", "Coinbase sequence failure ignored (a contradicting real feed is not reported)",
     [("feature_eval/coinbase_seq.py",
       "if (p is not None and p >= cfg.contradiction_share) or (c is not None and c >= cfg.contradiction_share):",
       "if False:", 1)],
     ["coinbase"]),
    ("S14", "unverified settlement labels treated as gold",
     [("feature_eval/labels.py", 'GOLD_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED")',
       'GOLD_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED", "SETTLEMENT_UNVERIFIED")', 1)],
     ["labels"]),
    ("S15", "ambiguous tied settlement conventions become VERIFIED (preferred policy in the passing set is enough)",
     [("feature_eval/labels.py", "    if passing == [cfg.label_window_policy] and not tied:",
       "    if cfg.label_window_policy in passing:", 1)],
     ["convention"]),
    ("S16", "ridge lambda inner split operates on ROWS (a market is split across inner train / validation)",
     [("feature_eval/models.py",
       '        a = [i for i, r in enumerate(meta or []) if r["market_ticker"] in set(tr_m)]\n'
       '        b = [i for i, r in enumerate(meta or []) if r["market_ticker"] in set(va_m)]\n',
       "        _cut = int(len(X) * (1 - self.inner_fraction))\n"
       "        a, b = (list(range(_cut)), list(range(_cut, len(X)))) if meta is not None else ([], [])\n", 1)],
     ["ridge"]),
    ("S17", "ridge inner split omits the purge / embargo (and its internal overlap guard, so the TEST must catch it)",
     [(SPL, "    tr, purged = _purge(tr, va, cfg)\n    trm, vam", "    purged = []\n    trm, vam", 1),
      (SPL, "        if max(e[\"close_ts_ms\"] for e in tr) + cfg.label_horizon_ms > start:", "        if False:", 1)],
     ["ridge"]),
    ("S18", "a rejected CF source still contributes to settlement verification / reconstructed labels",
     [("feature_eval/dataset.py", '            elif key in rejected_sources:\n                rep["index_values_rejected_source"] += 1',
       '            elif False:\n                rep["index_values_rejected_source"] += 1', 1)],
     ["rejected"]),
    ("S19", "equality under an AT_LEAST crypto contract is not YES (strict comparison restored)",
     [("settlement/rules.py", "        ok = v >= k\n", "        ok = v > k\n", 1)],
     ["rules"]),
    ("S20", "settlement-value precision uses a universal 0.01 tolerance (validates a wrong 4-dp XRP value)",
     [("settlement/resolution.py", '        out["expiration_value_exact_after_rounding"] = exact(settlement_value) == ev\n',
       '        out["expiration_value_exact_after_rounding"] = abs(exact(settlement_value) - ev) <= Fraction(1, 100)\n', 1),
      ("settlement/resolution.py", '        out["expiration_value_within_half_unit"] = abs(exact(unrounded) - ev) <= half\n',
       '        out["expiration_value_within_half_unit"] = abs(exact(unrounded) - ev) <= Fraction(1, 100)\n', 1)],
     ["precision"]),
    ("S21", "a current rule is applied to a pre-observation historical market and becomes a gold reconstructed label",
     [("settlement/market_rules.py", "    if market.close_ts_ms is not None and market.close_ts_ms >= static.observed_ts_ms:",
       "    if market.close_ts_ms is not None:", 1)],
     ["histrule"]),
    ("S22", "captured rules_primary contradicts the static rule but the static rule silently wins",
     [("settlement/market_rules.py", "            info.update(status=RULE_CONFLICT, reasons=conflicts)\n            return None, info",
       "            info.update(status=RULE_VERIFIED_FOR_MARKET, gold_eligible=True)\n            return static, info", 1)],
     ["rulecapture"]),
    ("S23", "the dataset fingerprint ignores the per-market rule-text hash",
     [("feature_eval/dataset.py", '    body["market_rules"] = sorted([tk, p.get("rule_text_sha256"), p.get("rule_status"), '
                                  'p.get("rule_fingerprint")]',
       '    body["market_rules"] = sorted([tk, None, p.get("rule_status"), p.get("rule_fingerprint")]', 1)],
     ["rulefp"]),
    ("S24", "the current (July-2026) schedule rounds the fee itself to full cents instead of fee + cost to a centicent",
     [("feature_eval/economics.py", "        fee = _ceil_to(raw + cost, CENTICENT) - cost", "        fee = _ceil_to(raw, CENT)", 1)],
     ["fees62"]),
    ("S25", "the Kalshi fee_multiplier_override from the captured metadata is ignored",
     [("feature_eval/economics.py", '    om = _num(meta.get("fee_multiplier_override"))', "    om = None", 1)],
     ["fees62"]),
    ("S26", "a historical trade uses a fee schedule whose effective interval does not cover it",
     [("feature_eval/economics.py", "x.covers_series(ticker) and x.covers_time(ts_ms)]",
       "x.covers_series(ticker) and ts_ms is not None]", 1)],
     ["fees62"]),
    ("S27", "settlement sample values are forced through a lossy float round-trip before the official rounding",
     [("settlement/engine.py", "        key = exact(obs.value_text or obs.value)", "        key = exact(obs.value)", 1)],
     ["cfexact"]),
]


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1800):
    tmp = tempfile.mkdtemp(prefix="mut-step6-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage22.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:70] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "step6_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    res = {"note": "Each mutation breaks one Step-6 rule in a temporary copy; CAUGHT = the Stage-22 tests fail.",
           "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    res["controls"]["unmutated"] = run_variant([], all_tests)
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        r = {"id": mid, "mutation": desc, "tests": tests, "as_is": run_variant(edits, tests)}
        res["mutations"].append(r)
        print(f"{mid} {desc}\n   {'CAUGHT' if r['as_is']['caught'] else 'NOT CAUGHT'}  ({r['as_is']['failed_test']})",
              flush=True)
    c = res["controls"]
    ok = not c["unmutated"]["caught"] and all(m["as_is"]["caught"] for m in res["mutations"])
    res["all_caught_and_controls_pass"] = ok
    print(f"control: unmutated {'PASS' if not c['unmutated']['caught'] else 'FAIL ' + str(c['unmutated']['failed_test'])}")
    print("RESULT: " + ("every mutation caught; control passes" if ok else "NOT every mutation caught / the control failed"))
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(res, f, indent=1)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
