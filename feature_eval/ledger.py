"""
Append-only, hash-chained RESEARCH LEDGER (analysis_output/step6_research_ledger.jsonl + a JSON summary).

Every experiment run, hypothesis family, DEVELOPMENT evaluation and FINAL_HOLDOUT access is recorded with its
experiment / dataset fingerprints. A FINAL_HOLDOUT access is PERMANENT: once a dataset fingerprint's holdout has been
opened, any further holdout evaluation for that dataset (e.g. after re-tuning) is refused with HoldoutBurned unless a
new, later, never-seen holdout is defined (a new dataset fingerprint). Editing an earlier entry breaks the hash chain
and verify() fails.
"""
import hashlib
import json
import os
import time

GENESIS = "0" * 64


class HoldoutBurned(RuntimeError):
    pass


class LedgerCorrupt(RuntimeError):
    pass


def _h(prev, body):
    return hashlib.sha256((prev + json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)).encode()).hexdigest()


class Ledger:
    def __init__(self, path, clock=None):
        self.path = path
        self.clock = clock or (lambda: int(time.time() * 1000))

    def entries(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def verify(self):
        prev = GENESIS
        for i, e in enumerate(self.entries()):
            body = {k: v for k, v in e.items() if k != "hash"}
            if e.get("prev") != prev or _h(prev, body) != e.get("hash"):
                raise LedgerCorrupt(f"research ledger entry {i} was altered or reordered")
            prev = e["hash"]
        return True

    def append(self, kind, **fields):
        self.verify()
        es = self.entries()
        prev = es[-1]["hash"] if es else GENESIS
        body = {"seq": len(es), "kind": kind, "logged_at_ms": self.clock(), "prev": prev, **fields}
        body["hash"] = _h(prev, {k: v for k, v in body.items() if k != "hash"})
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(body, sort_keys=True, default=str) + "\n")
        return body

    def holdout_accesses(self, dataset_fingerprint=None):
        return [e for e in self.entries() if e["kind"] == "FINAL_HOLDOUT_ACCESS"
                and (dataset_fingerprint is None or e.get("dataset_fingerprint") == dataset_fingerprint)]

    def open_holdout(self, dataset_fingerprint, experiment_fingerprint, reason):
        """Record the ONE permitted holdout evaluation for this dataset; a second one is refused."""
        prior = self.holdout_accesses(dataset_fingerprint)
        if prior:
            raise HoldoutBurned(f"the FINAL_HOLDOUT of dataset {dataset_fingerprint[:12]} was already opened "
                                f"(ledger seq {prior[0]['seq']}); results after further tuning cannot use it - define a "
                                "new, later holdout")
        return self.append("FINAL_HOLDOUT_ACCESS", dataset_fingerprint=dataset_fingerprint,
                           experiment_fingerprint=experiment_fingerprint, reason=reason, permanent=True)

    def summary(self):
        es = self.entries()
        kinds = {}
        for e in es:
            kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
        return {"entries": len(es), "by_kind": kinds, "head_hash": es[-1]["hash"] if es else GENESIS,
                "holdout_accesses": [{"seq": e["seq"], "dataset_fingerprint": e.get("dataset_fingerprint"),
                                      "experiment_fingerprint": e.get("experiment_fingerprint"),
                                      "reason": e.get("reason")} for e in self.holdout_accesses()],
                "chain_verified": self.verify()}
