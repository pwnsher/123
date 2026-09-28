"""
EXPERIMENT fingerprints and reproducibility metadata.

experiment_fingerprint covers: the dataset fingerprint, model name + hyperparameters / grids, feature families and
the structural-pruning fingerprint, the missing-data strategy, the calibration method and gates, the walk-forward and
purge specification (split config incl. purge_ms), sample / complexity gates, the fee model and its fingerprint, the
execution sizes, the bootstrap specification and the random seed. Any change to any of these changes the fingerprint.
"""
import hashlib
import json
import platform
import sys

SEED = 20260928


def _sha(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def experiment_fingerprint(dataset_fp, spec):
    """spec: a JSON-serializable dict of every experiment choice (see module doc)."""
    required = ("model", "families", "missing_strategy", "calibration", "split_config", "fee_model_fingerprint",
                "seed", "bootstrap")
    miss = [k for k in required if k not in spec]
    if miss:
        raise ValueError(f"experiment spec lacks {miss}")
    if "purge_ms" not in spec["split_config"]:
        raise ValueError("the walk-forward purge (purge_ms) must be part of the experiment fingerprint")
    body = {"dataset_fingerprint": dataset_fp, "spec": spec}
    return _sha(body), body


def environment():
    return {"python": sys.version.split()[0], "implementation": platform.python_implementation(),
            "platform": platform.platform(), "machine": platform.machine(),
            "packages": "standard library only (no numpy / scipy / sklearn / pandas)", "seed": SEED,
            "hash_seed_independent": True,
            "note": "all randomness uses random.Random(seed) instances; dict / set iteration never decides a result"}
