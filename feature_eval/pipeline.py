"""
The Step-6 research pipeline over one ResearchMatrix (DEVELOPMENT data only; the FINAL_HOLDOUT is untouched unless
evaluate_final_holdout() is called explicitly, which is recorded permanently in the research ledger).

run(matrix, records, ...) -> {"data": ..., "family_ablation": ..., "calibration": ..., "confidence_buckets": ...,
                              "economic_metrics": ...}
With too little real data every section says INSUFFICIENT_DATA and names the failed requirements; with synthetic data
every section carries synthetic_only = true and the overall class SYNTHETIC_ONLY.
"""
from concurrent.futures import ProcessPoolExecutor

from feature_eval import RESULT_CLASSES
from feature_eval import metrics as mt
from feature_eval.ablation import (LAYER_HYPOTHESES, AblationConfig, Study, apply_bh, classify, feature_value,
                                   fit_fold, paired, usable_rows)
from feature_eval.calibration import CalibrationGates, fit_calibrator
from feature_eval.economics import FeeModel, evaluate as economics
from feature_eval.experiment import environment, experiment_fingerprint
from feature_eval.gates import SampleGates, direction_of, sample_gate
from feature_eval.leakage import guard
from feature_eval.models import market_weights
from feature_eval.pruning import redundancy_report, structural_prune
from feature_eval.splits import SplitConfig, assert_chronological, calibration_split, market_table, partition, walk_forward

_W = {}


def _worker_init(matrix, records, parts, folds, cfg, coinbase_allowed):
    _W["study"] = Study(matrix, records, parts, folds, cfg, coinbase_allowed)


def _worker_job(job):
    hid, fams, model = job
    res, _r, _p, _b = _W["study"].evaluate(hid, fams, model)
    return res


def _spec(study, fams, model, split_cfg, fee, cal_gates, strategy):
    cfg = study.cfg
    return {"model": model, "families": list(fams), "missing_strategy": strategy,
            "hyperparameters": {"max_selected_features": cfg.max_selected_features, "boosted_rounds": cfg.boosted_rounds,
                                "ridge_grid": [0.1, 1.0, 10.0, 100.0], "logistic_l2": 1e-4,
                                "ridge_inner_split": {"unit": "market close group", "fraction": cfg.ridge_inner_fraction,
                                                      "lookback_ms": cfg.ridge_inner_lookback_ms,
                                                      "label_horizon_ms": cfg.ridge_inner_label_horizon_ms}},
            "calibration": {"primary": "none", "gates": cal_gates.to_dict()}, "split_config": split_cfg.to_dict(),
            "prune": cfg.prune.to_dict(), "complexity": cfg.complexity.to_dict(),
            "fee_model_fingerprint": fee.fingerprint(), "seed": cfg.seed,
            "bootstrap": {"unit": "market", "reps": cfg.bootstrap_reps, "permutation_reps": cfg.permutation_reps},
            "decision": {"q_threshold": cfg.q_threshold, "practical_logloss_improvement": cfg.practical_logloss_improvement}}


def _strip(res):
    for k in ("comparison_vs_A2", "comparison_vs_A", "legacy_A2_vs_A"):
        c = res.get(k)
        if isinstance(c, dict):
            for s in ("model", "baseline"):
                if isinstance(c.get(s), dict):
                    c[s].pop("classification", None)
    for a in res.get("folds", []):
        a.pop("selection_markets", None)
        ri = a.get("ridge_inner")
        if isinstance(ri, dict):
            for k in ("train_markets", "validation_markets", "purged_markets", "used_train_markets",
                      "used_validation_markets"):
                ri.pop(k, None)
    return res


def _insufficient(reason, extra=None):
    d = {"result_class": "INSUFFICIENT_DATA", "reason": reason}
    d.update(extra or {})
    return d


def run(matrix, records, split_cfg=None, cfg=None, sample_cfg=None, fee=None, cal_gates=None, coinbase_allowed=False,
        workers=1, stages=("ablation", "calibration", "economics"), ledger=None):
    split_cfg = split_cfg or SplitConfig()
    cfg = cfg or AblationConfig()
    fee = fee or FeeModel()
    cal_gates = cal_gates or CalibrationGates()
    synthetic = matrix.synthetic_only
    rows = usable_rows(matrix.rows)
    table = market_table(rows)
    parts = partition(table, split_cfg)
    folds = walk_forward(table, parts, split_cfg)
    assert_chronological(folds, table)
    train_set = set(parts["TRAIN"])
    tr_rows = [r for r in rows if r["market_ticker"] in train_set]
    cand = sorted(r["name"] for r in records if r["role"] == "MODEL_CANDIDATE" and r["name"] in matrix.col_index)
    leak = guard(matrix.columns, matrix.rows, cand, matrix.col_index, tr_rows)
    study = Study(matrix, records, parts, folds, cfg, coinbase_allowed)
    gate = sample_gate(matrix.rows, sample_cfg or SampleGates(), synthetic, tuple(matrix.meta.get("assets", ())),
                       matrix.meta.get("checkpoint_grid_s"), study.regime_of,
                       ("VOL_LOW", "VOL_MID", "VOL_HIGH") if study.regime_of else None)
    base = {"synthetic_only": synthetic, "dataset_fingerprint": matrix.fingerprint, "sample_gate": gate,
            "splits": {k: (len(v) if isinstance(v, list) else v) for k, v in parts.items()},
            "split_boundaries_close_ts_ms": parts["boundaries_close_ts_ms"],
            "walk_forward": [{k: (len(v) if isinstance(v, list) else v) for k, v in f.items()} for f in folds],
            "purge_ms": split_cfg.purge_ms, "leakage_guard": leak, "environment": environment(),
            "coinbase_sequence_features": {"allowed": coinbase_allowed, "gated_out": len(study.coinbase_gated)},
            "regime": study.regime_info, "never": "APPROVED_FOR_PRODUCTION"}
    out = {"data": base}
    runnable = synthetic or gate["status"] == "SUFFICIENT_FOR_RESEARCH"
    if not runnable or not folds:
        why = gate["failed_requirements"] if not runnable else "no walk-forward fold could be formed"
        for k in ("family_ablation", "calibration", "confidence_buckets", "economic_metrics"):
            out[k] = dict(base, **_insufficient(why))
        return out
    if ledger is not None:
        ledger.append("DEVELOPMENT_EVALUATION", dataset_fingerprint=matrix.fingerprint, synthetic_only=synthetic,
                      stages=list(stages), hypotheses=[h for h, _ in LAYER_HYPOTHESES])
    # ---------------- stage 1: layers
    jobs = []
    for hid, layers in LAYER_HYPOTHESES:
        fams = study.families_of_layers(layers)
        for m in cfg.models:
            jobs.append((hid, tuple(fams), m))
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                                 initargs=(matrix, records, parts, folds, cfg, coinbase_allowed)) as ex:
            s1 = list(ex.map(_worker_job, jobs))                      # map keeps job order: deterministic
    else:
        s1 = [study.evaluate(h, list(f), m)[0] for h, f, m in jobs]
    for r in s1:
        r["layers"] = dict(LAYER_HYPOTHESES)[r["hypothesis"]]
        fp, _ = experiment_fingerprint(matrix.fingerprint, _spec(study, r["families"], r["model"], split_cfg, fee,
                                                                  cal_gates, cfg.missing_strategy))
        r["experiment_fingerprint"] = fp
        for a in r["folds"]:
            a.pop("selection_markets", None)
    apply_bh(s1, cfg, synthetic, study.degraded)
    ok = [r for r in s1 if r["comparison_vs_A2"].get("status") == "OK"]
    best = min(ok, key=lambda r: (r["comparison_vs_A2"]["model"]["log_loss"], r["hypothesis"], r["model"])) if ok else None
    # ---------------- stage 2: LOFO on the best DEV configuration
    s2 = []
    best_pred = None
    if best:
        _res, brow, bpred, ba2 = study.evaluate(best["hypothesis"], best["families"], best["model"])
        best_pred = (brow, bpred, ba2)
        live = [f for f in best["families"] if study.fam_names.get(f)]
        if len(live) >= 2:
            for f in live:
                red = [g for g in live if g != f]
                _r2, rrow, rpred, _ = study.evaluate(f"LOFO-{f}", red, best["model"])
                cmpd = paired(rrow, bpred, rpred, cfg, study.fold_of, study.regime_of)   # full vs reduced
                s2.append({"hypothesis": f"LOFO-{f}", "left_out_family": f, "model": best["model"],
                           "comparison_vs_A2": cmpd,
                           "interpretation": "delta < 0: the FULL configuration beats the one without this family"})
        apply_bh(s2, cfg, synthetic, study.degraded)
    # ---------------- stage 3: families inside promising layers
    s3 = []
    for hid, layers in LAYER_HYPOTHESES[:4]:
        prim = next((r for r in s1 if r["hypothesis"] == hid and r["model"] == cfg.primary_model), None)
        if prim is None or prim["result_class"] != "PROMISING_RESEARCH_ONLY":
            continue
        for f in study.families_of_layers(layers):
            res, _, _, _ = study.evaluate(f"{hid}-{f}", [f], cfg.primary_model)
            s3.append(res)
    apply_bh(s3, cfg, synthetic, study.degraded)
    fam_class = family_classes(study, s1, s2, s3, cfg)
    prune_full = pruning_report(study, tr_rows)
    fa = dict(base)
    fa.update({"config": cfg.to_dict(), "stage1_layer_hypotheses": [_strip(r) for r in s1],
               "best_development_configuration": ({"hypothesis": best["hypothesis"], "model": best["model"],
                                                   "families": best["families"],
                                                   "experiment_fingerprint": best["experiment_fingerprint"],
                                                   "selected_by": "lowest DEVELOPMENT out-of-fold log loss (holdout never used)"}
                                                  if best else None),
               "stage2_leave_one_family_out": [_strip(r) for r in s2], "stage3_family_hypotheses": [_strip(r) for r in s3],
               "family_classification": fam_class, "feature_classification": feature_classes(study, fam_class, prune_full),
               "structural_pruning_train_partition": prune_full,
               "complexity_reports": [{"hypothesis": r["hypothesis"], "model": r["model"], "fold": a.get("fold"),
                                       **(a.get("complexity") or {})} for r in s1 for a in r["folds"] if a.get("complexity")],
               "result_class": overall_class(s1, synthetic, study.degraded)})
    if best:
        fa["missing_data"] = missing_data(study, best, cfg)
        fa["asset_specific"] = asset_specific(study, best, cfg)
        fa["slices_best_configuration"] = slices(best_pred, cfg)
        fa["session_normalized"] = session_normalized(study, best, best_pred, cfg)
        fa["redundancy_train_only"] = redundancy_report(tr_rows, sorted(n for f in best["families"]
                                                                       for n in study.fam_names.get(f, []))[:120],
                                                        matrix.col_index)
    out["family_ablation"] = fa
    # ---------------- calibration / buckets / economics (DEV out-of-fold, best configuration and legacy)
    if "calibration" not in stages:
        for k in ("calibration", "confidence_buckets", "economic_metrics"):
            out[k] = dict(base, result_class="NOT_RUN", reason="stage not requested in this invocation")
        return out
    cal, preds = calibration_study(study, best, cal_gates, cfg)
    out["calibration"] = dict(base, **cal)
    out["confidence_buckets"] = dict(base, **buckets(preds, fee))
    out["economic_metrics"] = dict(base, **economics_section(preds, fee))
    return out


def overall_class(s1, synthetic, degraded):
    if synthetic:
        return "SYNTHETIC_ONLY"
    classes = [r["result_class"] for r in s1]
    for c in ("PROMISING_RESEARCH_ONLY", "UNSTABLE", "DEGRADED", "NO_INCREMENTAL_VALUE",
              "INSUFFICIENT_DATA_FOR_COMPLEXITY", "INSUFFICIENT_DATA"):
        if c in classes:
            assert c in RESULT_CLASSES
            return c
    return "INSUFFICIENT_DATA"


def family_classes(study, s1, s2, s3, cfg):
    out = {}
    lofo = {r["left_out_family"]: r for r in s2}
    fam3 = {r["families"][0]: r for r in s3}
    layer_res = {}
    for r in s1:
        if r["model"] == cfg.primary_model and len(r["layers"]) == 1:
            layer_res[r["layers"][0]] = r["result_class"]
    for f in sorted(set(study.fam_layer) | {r["family"] for r in study.rec.values()}):
        names = study.fam_names.get(f, [])
        if not names:
            out[f] = {"class": "UNAVAILABLE", "why": "no model-candidate input (or gated: Coinbase sequence status)"}
            continue
        lay = study.fam_layer.get(f)
        lc = layer_res.get(lay)
        if f in fam3:
            c = fam3[f]["result_class"]
            cls = {"PROMISING_RESEARCH_ONLY": "KEEP_CANDIDATE", "UNSTABLE": "UNSTABLE", "DEGRADED": "UNSTABLE",
                   "INSUFFICIENT_DATA_FOR_COMPLEXITY": "INSUFFICIENT_DATA", "INSUFFICIENT_DATA": "INSUFFICIENT_DATA",
                   "UNAVAILABLE": "UNAVAILABLE"}.get(c, "NO_INCREMENTAL_VALUE")
            if cls == "KEEP_CANDIDATE" and f in lofo and lofo[f]["result_class"] == "NO_INCREMENTAL_VALUE":
                cls = "REDUNDANT"
            out[f] = {"class": cls, "why": f"stage-3 {c}; layer {lay} {lc}"}
        elif lc in ("INSUFFICIENT_DATA", "INSUFFICIENT_DATA_FOR_COMPLEXITY", None):
            out[f] = {"class": "INSUFFICIENT_DATA", "why": f"layer {lay}: {lc}"}
        elif lc == "UNSTABLE":
            out[f] = {"class": "UNSTABLE", "why": f"layer {lay} unstable"}
        elif lc == "UNAVAILABLE":
            out[f] = {"class": "UNAVAILABLE", "why": f"layer {lay} unavailable"}
        else:
            out[f] = {"class": "NO_INCREMENTAL_VALUE", "why": f"layer {lay}: {lc} (families tested only inside promising layers)"}
        out[f]["layer"] = lay
        out[f]["model_candidate_features"] = len(names)
        if study.synthetic:
            out[f]["synthetic_only"] = True
    return out


def pruning_report(study, tr_rows):
    fams = {}
    for f in sorted(study.fam_names):
        fams[f] = structural_prune(tr_rows, study.fam_names[f], study.col_index, study.role_of, study.cfg.prune)
    kept = sorted(n for p in fams.values() for n in p["kept"])
    removed = {n: why for p in fams.values() for n, why in p["removed"].items()}
    nonmodel = {n: f"NOT_MODEL_CANDIDATE ({r['role']})" for n, r in study.rec.items() if r["role"] != "MODEL_CANDIDATE"}
    gated = {n: "GATED_COINBASE_SEQUENCE_UNVERIFIED" for n in study.coinbase_gated}
    import hashlib
    import json
    body = {"kept_features": kept, "structurally_removed_features": dict(sorted({**removed, **nonmodel, **gated}.items())),
            "family_fingerprints": {f: p["fingerprint"] for f, p in fams.items()}}
    body["fingerprint"] = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    body["counts"] = {"kept": len(kept), "removed": len(body["structurally_removed_features"])}
    body["scope"] = "TRAIN partition rows only; label independent; removed fields stay in the raw data and research rows"
    return body


def feature_classes(study, fam_class, prune_full):
    out = {}
    removed = prune_full["structurally_removed_features"]
    for n, r in sorted(study.rec.items()):
        why = removed.get(n)
        if why:
            if why.startswith(("EXACT_DUPLICATE", "HIGHLY_CORRELATED")) or "ALIAS" in why:
                out[n] = "REDUNDANT"
            else:
                out[n] = "UNAVAILABLE"
        else:
            out[n] = fam_class.get(r["family"], {}).get("class", "INSUFFICIENT_DATA")
    counts = {}
    for c in out.values():
        counts[c] = counts.get(c, 0) + 1
    return {"counts": counts, "by_feature": out,
            "note": "classification only - no source code or raw field is deleted on the basis of these results"}


def missing_data(study, best, cfg):
    out = {}
    for strat, model in (("indicator", best["model"] if best["model"] != "D_boosted_stumps" else "C_ridge_logistic"),
                         ("complete_case", best["model"] if best["model"] != "D_boosted_stumps" else "C_ridge_logistic"),
                         ("native", "D_boosted_stumps")):
        res, rows, pred, a2 = study.evaluate(best["hypothesis"], best["families"], model,
                                             strategy=None if strat == "native" else strat)
        c = res["comparison_vs_A2"]
        out[strat] = {"model": model, "status": c.get("status"), "rows_evaluated": c.get("n_rows"),
                      "rows_available": len(rows), "log_loss": (c.get("model") or {}).get("log_loss"),
                      "baseline_log_loss": (c.get("baseline") or {}).get("log_loss"),
                      "log_loss_improvement": c.get("log_loss_improvement")}
    loss = {}
    for f in best["families"]:
        idx = [study.col_index[n] for n in study.fam_names.get(f, [])]
        if not idx:
            continue
        allm = sum(1 for r in study.rows if all(feature_value(r, j) is None for j in idx))
        anym = sum(1 for r in study.rows if any(feature_value(r, j) is None for j in idx))
        loss[f] = {"rows": len(study.rows), "rows_all_missing": allm, "rows_any_missing": anym,
                   "complete_case_sample_loss_share": anym / len(study.rows) if study.rows else None}
    return {"strategies": out, "sample_loss_per_family": loss,
            "rule": "missing is never replaced by zero: train mean + indicator, complete case, or native tree routing"}


def asset_specific(study, best, cfg):
    assets = sorted({r["asset"] for r in study.rows})
    res_p, rows, pred, a2 = study.evaluate(best["hypothesis"], best["families"], best["model"])
    _r, rows2, pred2, _ = study.evaluate(best["hypothesis"], best["families"], best["model"],
                                         extra_fn=lambda r: [1.0 if r["asset"] == a else 0.0 for a in assets[1:]])
    spec_pred = {}
    for a in assets:
        for f in study.folds:
            tr, te = study.fold_rows(f)
            tr = [r for r in tr if r["asset"] == a]
            te = [r for r in te if r["asset"] == a]
            if not tr or not te or len({r["y"] for r in tr}) < 2:
                continue
            p, _b, info = fit_fold(tr, te, best["families"], study.fam_names, study.col_index, study.role_of,
                                   best["model"], cfg, study.holdout, None, None)
            for r, v in zip(te, p or [None] * len(te)):
                spec_pred[(r["market_ticker"], r["checkpoint_s"])] = v
    p3 = [spec_pred.get((r["market_ticker"], r["checkpoint_s"])) for r in rows]
    out = {"pooled": _metric(rows, pred), "pooled_plus_asset_indicators": _metric(rows2, pred2),
           "asset_specific": _metric(rows, p3), "per_asset": {}}
    for a in assets:
        ix = [i for i, r in enumerate(rows) if r["asset"] == a]
        out["per_asset"][a] = {"pooled": _metric([rows[i] for i in ix], [pred[i] for i in ix]),
                               "asset_specific": _metric([rows[i] for i in ix], [p3[i] for i in ix]),
                               "legacy_A2": _metric([rows[i] for i in ix], [a2[i] for i in ix])}
    out["note"] = "DEVELOPMENT folds only; descriptive comparison, no asset-specific model is promoted"
    return out


def _metric(rows, pred):
    ix = [i for i, p in enumerate(pred) if p is not None]
    if not ix:
        return {"status": "NOT_EVALUATED", "n_rows": 0}
    p = [pred[i] for i in ix]
    y = [rows[i]["y"] for i in ix]
    w = market_weights([rows[i]["market_ticker"] for i in ix])
    return {"n_rows": len(ix), "n_markets": len({rows[i]["market_ticker"] for i in ix}), "log_loss": mt.log_loss(p, y, w),
            "brier": mt.brier(p, y, w)}


def slices(best_pred, cfg):
    rows, pred, a2 = best_pred
    leg = [r["legacy_p_up"] for r in rows]
    out = {"by_checkpoint": {}, "by_direction": {}}
    for cp in sorted({r["checkpoint_s"] for r in rows}, reverse=True):
        ix = [i for i, r in enumerate(rows) if r["checkpoint_s"] == cp]
        out["by_checkpoint"][str(cp)] = {k: _metric([rows[i] for i in ix], [v[i] for i in ix])
                                         for k, v in (("best", pred), ("legacy_A2", a2), ("legacy_A", leg))}
    for d in ("UP", "DOWN"):
        ix = [i for i, r in enumerate(rows) if direction_of(r) == d]
        out["by_direction"][d] = {k: _metric([rows[i] for i in ix], [v[i] for i in ix])
                                  for k, v in (("best", pred), ("legacy_A2", a2), ("legacy_A", leg))}
    out["time_to_close_note"] = "measurement only on the frozen checkpoint grid - no entry window is optimized"
    return out


def session_normalized(study, best, best_pred, cfg):
    sn = sorted(n for f in best["families"] for n in study.fam_names.get(f, [])
                if "SESSION_NORMALIZED" in study.rec[n].get("tags", []))
    rows, pred, a2 = best_pred
    per_session = {}
    for s in sorted({r["session_id"] for r in rows if r.get("session_id")}):
        ix = [i for i, r in enumerate(rows) if r.get("session_id") == s]
        per_session[s] = {"best": _metric([rows[i] for i in ix], [pred[i] for i in ix]),
                          "legacy_A2": _metric([rows[i] for i in ix], [a2[i] for i in ix])}
    out = {"session_normalized_features_in_scope": sn, "per_session_generalization": per_session}
    if sn:
        saved = {f: list(v) for f, v in study.fam_names.items()}
        try:
            for f in study.fam_names:
                study.fam_names[f] = [n for n in study.fam_names[f] if n not in set(sn)]
            study.prune_cache = {}
            _r, rrow, rpred, _ = study.evaluate("SESSION_NORMALIZED_REMOVED", best["families"], best["model"])
        finally:
            study.fam_names = saved
            study.prune_cache = {}
        out["with_vs_without"] = paired(rrow, pred, rpred, cfg)
    out["note"] = ("SESSION_NORMALIZED features rank within a capture session; their value must generalize across "
                   "sessions (per-session results above) and is shown with / without them")
    return out


def calibration_study(study, best, gates, cfg):
    """Per fold: model fit on the EARLIER part of the fold's training markets, calibrator on the LATER part, evaluated on
    the test block. Legacy (A) is calibrated the same way."""
    from feature_eval.splits import SplitConfig
    methods = ("none", "platt", "isotonic")
    acc = {("best", m): [] for m in methods}
    acc.update({("legacy", m): [] for m in methods})
    folds_info = []
    rows_all = []
    for f in study.folds:
        tr, te = study.fold_rows(f)
        fit_m, cal_m, purged = calibration_split(f["train"], market_table(tr), SplitConfig(
            calibration_fraction=0.25, max_causal_lookback_ms=study.parts["config"]["max_causal_lookback_ms"],
            label_horizon_ms=study.parts["config"]["label_horizon_ms"]))
        fr = [r for m in fit_m for r in study.by_market.get(m, [])]
        cr = [r for m in cal_m for r in study.by_market.get(m, [])]
        if not fr or not cr or not te or len({r["y"] for r in fr}) < 2:
            folds_info.append({"fold": f["fold"], "status": "INSUFFICIENT_DATA"})
            continue
        assert max(r["close_ts_ms"] for r in fr) < min(r["close_ts_ms"] for r in cr), "calibration data must be later"
        cand = {"best": None, "legacy": None}
        if best:
            both = cr + te
            p, _b, info = fit_fold(fr, both, best["families"], study.fam_names, study.col_index, study.role_of,
                                   best["model"], study.cfg, study.holdout, None, None)
            if p is not None:
                cand["best"] = (p[:len(cr)], p[len(cr):])
        cand["legacy"] = ([r["legacy_p_up"] for r in cr], [r["legacy_p_up"] for r in te])
        n_cal = len({r["market_ticker"] for r in cr})
        fi = {"fold": f["fold"], "fit_markets": len(fit_m), "calibration_markets": n_cal, "purged": len(purged),
              "methods": {}}
        for who, v in cand.items():
            if v is None:
                continue
            pc, pt = v
            idx = [i for i, p in enumerate(pc) if p is not None]
            for m in methods:
                c, meta = fit_calibrator(m, [pc[i] for i in idx], [cr[i]["y"] for i in idx],
                                         market_weights([cr[i]["market_ticker"] for i in idx]), n_cal, gates)
                fi["methods"][f"{who}:{m}"] = meta
                if meta["status"] != "OK":
                    continue
                ptt = [p if p is None else (c.apply([p])[0] if c else p) for p in pt]
                acc[(who, m)].append((te, ptt))
        rows_all += te
        folds_info.append(fi)
    res = {"methods": {}, "folds": folds_info, "gates": gates.to_dict(),
           "rule": "calibration observations are strictly later than the model-fit observations and earlier than the "
                   "evaluated block; methods below their sample gate are DISABLED, never run"}
    preds = {}
    for (who, m), chunks in acc.items():
        rows = [r for te, _ in chunks for r in te]
        pr = [p for _, pt in chunks for p in pt]
        ix = [i for i, p in enumerate(pr) if p is not None]
        key = f"{who}:{m}"
        if not ix:
            res["methods"][key] = {"status": "DISABLED_OR_NOT_EVALUATED"}
            continue
        p = [pr[i] for i in ix]
        rr = [rows[i] for i in ix]
        y = [r["y"] for r in rr]
        w = market_weights([r["market_ticker"] for r in rr])
        s = mt.summary(p, y, w)
        s.pop("classification", None)
        s["reliability_bins"] = mt.reliability_bins(p, y, w)
        s["folds_used"] = len(chunks)
        res["methods"][key] = s
        preds[key] = (rr, p)
    # predeclared economics / bucket input: Platt when its gate passed in every fold, else uncalibrated
    for who in ("best", "legacy"):
        ok = all(fi.get("methods", {}).get(f"{who}:platt", {}).get("status") == "OK" for fi in folds_info if "methods" in fi)
        k = f"{who}:platt" if ok and f"{who}:platt" in preds else f"{who}:none"
        if k in preds:
            preds[who] = preds[k] + (k,)
    res["economics_input_rule"] = "Platt-calibrated when its gate passed in every fold, else uncalibrated (predeclared)"
    return res, preds


def buckets(preds, fee):
    from feature_eval.economics import row_economics
    out = {}
    for who in ("best", "legacy"):
        if who not in preds:
            out[who] = {"status": "NOT_EVALUATED"}
            continue
        rows, p, key = preds[who]
        extra = []
        for r, pi in zip(rows, p):
            e = row_economics(pi, r.get("execution"), fee, r["market_ticker"], (1,), ts_ms=r.get("checkpoint_ts_ms"),
                              fee_context=r.get("fee_context"))
            if e["status"] != "OK":
                extra.append(None)
                continue
            s = e["sides"][e["favoured_side"]]["1"]
            extra.append({"price": s.get("price"), "net_edge": s.get("net_executable_edge")} if s["status"] == "EXECUTABLE"
                         else None)
        w = market_weights([r["market_ticker"] for r in rows])
        out[who] = {"prediction_source": key, "buckets": mt.confidence_buckets(p, [r["y"] for r in rows], w,
                                                                               [r["market_ticker"] for r in rows], extra)}
    out["note"] = "out-of-fold DEVELOPMENT predictions; counts are reported as 'wins / n', never as an accuracy claim"
    return out


def economics_section(preds, fee):
    out = {}
    for who in ("best", "legacy"):
        if who not in preds:
            out[who] = {"status": "NOT_EVALUATED"}
            continue
        rows, p, key = preds[who]
        later = {}
        by_m = {}
        for r in rows:
            by_m.setdefault(r["market_ticker"], []).append(r)
        for m, rs in by_m.items():
            rs.sort(key=lambda r: -r["checkpoint_s"])
            for i, r in enumerate(rs):
                path = []
                for r2 in rs[i + 1:]:
                    ex = r2.get("execution") or {}
                    yb = ex.get("yes_bid")
                    ya = ex.get("yes_ask")
                    path.append((r2["checkpoint_s"], yb, (100.0 - ya) if ya is not None else None))
                later[(m, r["checkpoint_s"])] = path
        pr = [{"market": r["market_ticker"], "p_yes": pi, "y": r["y"], "execution": r.get("execution"),
               "checkpoint_s": r["checkpoint_s"], "ticker": r["market_ticker"], "ts_ms": r.get("checkpoint_ts_ms"),
               "fee_context": r.get("fee_context")}
              for r, pi in zip(rows, p)]
        e = economics(pr, fee, later_bids=later)
        e["prediction_source"] = key
        out[who] = e
    out["primary"] = "TAKER (immediate execution against the captured book); maker not evaluated"
    out["labels"] = "SIMULATED"
    return out


def evaluate_final_holdout(matrix, records, best, ledger, split_cfg=None, cfg=None, coinbase_allowed=False, reason=""):
    """The ONE permitted evaluation of the frozen best DEV configuration on the FINAL_HOLDOUT; refused a second time."""
    split_cfg = split_cfg or SplitConfig()
    cfg = cfg or AblationConfig()
    rows = usable_rows(matrix.rows)
    table = market_table(rows)
    parts = partition(table, split_cfg)
    exp_fp = best["experiment_fingerprint"]
    ledger.open_holdout(matrix.fingerprint, exp_fp, reason or "final holdout evaluation of the frozen DEV configuration")
    hold = set(parts["FINAL_HOLDOUT"])
    dev = set(parts["TRAIN"]) | set(parts["DEVELOPMENT"])
    tr = [r for r in rows if r["market_ticker"] in dev]
    te = [r for r in rows if r["market_ticker"] in hold]
    study = Study(matrix, records, parts, [], cfg, coinbase_allowed)
    p, a2, info = fit_fold(tr, te, best["families"], study.fam_names, study.col_index, study.role_of, best["model"], cfg,
                           set(), None, None)
    info.pop("_model", None)
    info.pop("selection_markets", None)
    cmp = paired(te, p or [None] * len(te), a2, cfg) if p else {"status": info.get("status")}
    return {"experiment_fingerprint": exp_fp, "comparison_vs_A2": cmp, "fit": info,
            "result_class": classify(cmp, cfg, cmp.get("p_value_sign_flip"), matrix.synthetic_only, False)
            if cmp.get("status") == "OK" else cmp.get("status"), "synthetic_only": matrix.synthetic_only}

