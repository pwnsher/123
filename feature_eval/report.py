"""
Step-6 output files. REAL results go to analysis_output/step6_*.json; SYNTHETIC results can only go to the separate
self-test file (analysis_output/step6_synthetic_selftest.json) - write_real_outputs refuses a synthetic result.
With no (or too little) real data every real file says INSUFFICIENT_DATA and lists the unmet requirements.
"""
import json
import os
import time

from feature_eval import APP_VERSION, RESEARCH_ONLY, STEP6_SCHEMA_VERSION

OUTPUT_FILES = {
    "data_quality": "step6_data_quality.json",
    "family_ablation": "step6_family_ablation.json",
    "calibration": "step6_calibration.json",
    "confidence_buckets": "step6_confidence_buckets.json",
    "economic_metrics": "step6_economic_metrics.json",
    "research_ledger": "step6_research_ledger.json",
}
SELFTEST_FILE = "step6_synthetic_selftest.json"
LEDGER_FILE = "step6_research_ledger.jsonl"


class SyntheticInRealOutput(RuntimeError):
    pass


def _header(kind, synthetic):
    return {"schema_version": STEP6_SCHEMA_VERSION, "app_version": APP_VERSION, "kind": kind,
            "research_only": RESEARCH_ONLY, "synthetic_only": bool(synthetic),
            "production_effect": "NONE - evidence only; nothing here changes a production call, threshold, window, "
                                 "stop, size, the perp veto or any order path",
            "never": "APPROVED_FOR_PRODUCTION"}


def _dump(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, default=str, allow_nan=False)
        f.write("\n")
    os.replace(tmp, path)


def _is_synthetic(section):
    return bool(isinstance(section, dict) and (section.get("synthetic_only") or
                                               (section.get("data") or {}).get("synthetic_only")))


def write_real_outputs(out_dir, data_quality, results=None, ledger_summary=None, insufficient_reason=None,
                       failed_requirements=None):
    """results: pipeline.run(...) output for REAL data, or None when no dataset could be built."""
    if _is_synthetic(data_quality) or (results and any(_is_synthetic(v) for v in results.values())):
        raise SyntheticInRealOutput("synthetic results cannot be written to the real Step-6 outputs")
    written = {}
    dq = dict(_header("data_quality", False), **data_quality)
    _dump(os.path.join(out_dir, OUTPUT_FILES["data_quality"]), dq)
    written["data_quality"] = OUTPUT_FILES["data_quality"]
    for key in ("family_ablation", "calibration", "confidence_buckets", "economic_metrics"):
        body = (results or {}).get(key)
        if body is None:
            body = {"result_class": "INSUFFICIENT_DATA",
                    "reason": insufficient_reason or "no validated real research dataset",
                    "failed_requirements": failed_requirements or []}
        _dump(os.path.join(out_dir, OUTPUT_FILES[key]), dict(_header(key, False), **body))
        written[key] = OUTPUT_FILES[key]
    _dump(os.path.join(out_dir, OUTPUT_FILES["research_ledger"]),
          dict(_header("research_ledger", False), **(ledger_summary or {"entries": 0})))
    written["research_ledger"] = OUTPUT_FILES["research_ledger"]
    return written


def write_selftest(out_dir, results, extra=None):
    if not all(_is_synthetic(v) for v in results.values()):
        raise ValueError("the self-test file only accepts synthetic_only results")
    body = dict(_header("synthetic_selftest", True), results=results, generated_at_ms=int(time.time() * 1000),
                note="SYNTHETIC - code-correctness evidence only; never feature selection, tuning or a predictive claim")
    body.update(extra or {})
    p = os.path.join(out_dir, SELFTEST_FILE)
    _dump(p, body)
    return p
