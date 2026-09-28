#!/usr/bin/env python3
"""
Step-6 evaluation-framework benchmark on SYNTHETIC research matrices only (throughput of the code, never a
predictive result).

    py scripts/bench_step6.py [--out analysis_output/step6_performance.json] [--slots 100,400] [--workers 1,2]

Measures: synthetic matrix generation, structural pruning, one logistic / ridge / boosted fit, the full DEVELOPMENT
ablation (27 stage-1 fits x folds + LOFO + stage 3) with 1 and N worker processes (identical results checked), and
calibration + economics. The hardware / Python version is recorded with the numbers.
"""
import argparse
import json
import os
import platform
import sys
import time

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from feature_eval.ablation import AblationConfig, Study, fit_fold, usable_rows  # noqa: E402
from feature_eval.gates import ComplexityGates                                  # noqa: E402
from feature_eval.pipeline import run                                           # noqa: E402
from feature_eval.pruning import structural_prune                               # noqa: E402
from feature_eval.splits import market_table, partition, walk_forward           # noqa: E402
from feature_eval.synthetic import make_matrix                                  # noqa: E402


def _t(fn):
    t0 = time.perf_counter()
    r = fn()
    return r, round(time.perf_counter() - t0, 3)


def bench(slots, workers):
    cfg = AblationConfig(bootstrap_reps=300, permutation_reps=1000,
                         complexity=ComplexityGates(min_events_per_parameter=5, min_train_markets_per_parameter=8))
    (m, recs), t_gen = _t(lambda: make_matrix(n_slots=slots, checkpoints_s=(300, 60)))
    rows = usable_rows(m.rows)
    table = market_table(rows)
    parts = partition(table)
    folds = walk_forward(table, parts)
    st = Study(m, recs, parts, folds, cfg)
    tr, te = st.fold_rows(folds[-1])
    fams = sorted(st.fam_names)
    _, t_prune = _t(lambda: [structural_prune(tr, st.fam_names[f], st.col_index, st.role_of, cfg.prune) for f in fams])
    fits = {}
    for mname in cfg.models:
        _, fits[mname] = _t(lambda mname=mname: fit_fold(tr, te, fams, st.fam_names, st.col_index, st.role_of, mname, cfg,
                                                         st.holdout))
    out = {"slots": slots, "markets": len(table), "rows": len(m.rows), "generate_s": t_gen,
           "structural_prune_all_families_s": t_prune, "single_fold_fit_s": fits, "full_pipeline_s": {}}
    ref = None
    for w in workers:
        res, t = _t(lambda w=w: run(m, recs, cfg=cfg, workers=w))
        out["full_pipeline_s"][str(w)] = t
        key = json.dumps({k: v for k, v in res["family_ablation"].items() if k != "environment"}, sort_keys=True, default=str)
        if ref is None:
            ref = key
        out.setdefault("identical_across_workers", True)
        out["identical_across_workers"] = out["identical_across_workers"] and key == ref
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "analysis_output", "step6_performance.json"))
    ap.add_argument("--slots", default="100,400")
    ap.add_argument("--workers", default="1,2")
    a = ap.parse_args(argv)
    res = {"synthetic": True,
           "note": "SYNTHETIC research matrices - code throughput only; never evidence about any feature or model",
           "hardware": {"python": sys.version.split()[0], "implementation": platform.python_implementation(),
                        "platform": platform.platform(), "machine": platform.machine(),
                        "cpu_count": os.cpu_count()},
           "runs": []}
    for s in (int(x) for x in a.slots.split(",")):
        r = bench(s, [int(x) for x in a.workers.split(",")])
        res["runs"].append(r)
        print(json.dumps(r, sort_keys=True), flush=True)
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(res, f, indent=1, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
