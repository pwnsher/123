"""
CHRONOLOGICAL, PURGED splits. Never a random shuffle.

Markets are ordered by close time; all markets with the SAME close time (BTC / ETH / SOL / XRP of one 15-minute slot)
always fall in the same partition.

    partition(markets, cfg)          -> TRAIN / DEVELOPMENT / FINAL_HOLDOUT (+ purged markets, exact boundaries)
    walk_forward(markets, cfg)       -> expanding folds over TRAIN + DEVELOPMENT: fold k trains on every market before
                                        its test block (purged) and tests on the block; FINAL_HOLDOUT is never used
    calibration_split(train_markets) -> (fit part, later calibration part), purged, strictly chronological

PURGE / EMBARGO. A later partition's rows read up to max_causal_lookback_ms of history before their first checkpoint
(the longest retention of any frozen engine) and a label is known label_horizon_ms after the close. An EARLIER market
is purged when its close + label horizon falls inside the later partition's information window
[first checkpoint - lookback, ...]; the purge duration (lookback + label horizon) is part of every experiment
fingerprint.
"""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SplitConfig:
    train_fraction: float = 0.6
    development_fraction: float = 0.2          # FINAL_HOLDOUT = the remaining (latest) share
    walk_forward_folds: int = 4
    min_train_markets: int = 40
    calibration_fraction: float = 0.25         # the LATEST share of a fold's training data calibrates
    max_causal_lookback_ms: int = 1_500_000
    label_horizon_ms: int = 0

    @property
    def purge_ms(self):
        return self.max_causal_lookback_ms + self.label_horizon_ms

    def to_dict(self):
        d = asdict(self)
        d["purge_ms"] = self.purge_ms
        return d


def market_table(rows):
    """[{market, close_ts_ms, first_checkpoint_ts_ms, asset}] sorted by (close time, market)."""
    m = {}
    for r in rows:
        e = m.setdefault(r["market_ticker"], {"market": r["market_ticker"], "close_ts_ms": r["close_ts_ms"],
                                              "first_checkpoint_ts_ms": r["checkpoint_ts_ms"], "asset": r["asset"]})
        e["first_checkpoint_ts_ms"] = min(e["first_checkpoint_ts_ms"], r["checkpoint_ts_ms"])
    return sorted(m.values(), key=lambda e: (e["close_ts_ms"], e["market"]))


def _close_groups(table):
    groups = []
    for e in table:
        if groups and groups[-1][0]["close_ts_ms"] == e["close_ts_ms"]:
            groups[-1].append(e)
        else:
            groups.append([e])
    return groups


def _purge(earlier, later, cfg):
    """Drop earlier markets whose (close + label horizon) reaches the later block's information window."""
    if not later:
        return earlier, []
    start = min(e["first_checkpoint_ts_ms"] for e in later) - cfg.max_causal_lookback_ms
    keep = [e for e in earlier if e["close_ts_ms"] + cfg.label_horizon_ms <= start]
    return keep, [e for e in earlier if e not in keep]


def partition(table, cfg=None):
    cfg = cfg or SplitConfig()
    groups = _close_groups(table)
    n = len(groups)
    a = int(round(n * cfg.train_fraction))
    b = int(round(n * (cfg.train_fraction + cfg.development_fraction)))
    tr = [e for g in groups[:a] for e in g]
    dv = [e for g in groups[a:b] for e in g]
    ho = [e for g in groups[b:] for e in g]
    tr, p1 = _purge(tr, dv or ho, cfg)
    dv, p2 = _purge(dv, ho, cfg)

    def bounds(block):
        return [block[0]["close_ts_ms"], block[-1]["close_ts_ms"]] if block else None
    return {"TRAIN": [e["market"] for e in tr], "DEVELOPMENT": [e["market"] for e in dv],
            "FINAL_HOLDOUT": [e["market"] for e in ho], "purged": [e["market"] for e in p1 + p2],
            "boundaries_close_ts_ms": {"TRAIN": bounds(tr), "DEVELOPMENT": bounds(dv), "FINAL_HOLDOUT": bounds(ho)},
            "close_time_groups": n, "config": cfg.to_dict()}


def walk_forward(table, parts, cfg=None):
    """Expanding folds inside TRAIN + DEVELOPMENT: the DEVELOPMENT markets are cut into walk_forward_folds
    chronological blocks; fold k trains on every earlier non-holdout market (purged against the block)."""
    cfg = cfg or SplitConfig()
    holdout = set(parts["FINAL_HOLDOUT"])
    dev = set(parts["DEVELOPMENT"])
    usable = [e for e in table if e["market"] not in holdout and e["market"] not in set(parts["purged"])]
    dev_groups = _close_groups([e for e in usable if e["market"] in dev])
    k = max(1, min(cfg.walk_forward_folds, len(dev_groups)))
    folds = []
    for i in range(k):
        lo = (len(dev_groups) * i) // k
        hi = (len(dev_groups) * (i + 1)) // k
        block = [e for g in dev_groups[lo:hi] for e in g]
        if not block:
            continue
        first_close = block[0]["close_ts_ms"]
        earlier = [e for e in usable if e["close_ts_ms"] < first_close]
        train, purged = _purge(earlier, block, cfg)
        folds.append({"fold": len(folds), "train": [e["market"] for e in train], "test": [e["market"] for e in block],
                      "purged": [e["market"] for e in purged],
                      "train_close_range": [train[0]["close_ts_ms"], train[-1]["close_ts_ms"]] if train else None,
                      "test_close_range": [block[0]["close_ts_ms"], block[-1]["close_ts_ms"]]})
    return folds


def calibration_split(train_markets, table, cfg=None):
    """Latest calibration_fraction of a fold's training markets calibrates (purged against the fit part)."""
    cfg = cfg or SplitConfig()
    t = [e for e in table if e["market"] in set(train_markets)]
    groups = _close_groups(t)
    cut = int(round(len(groups) * (1 - cfg.calibration_fraction)))
    fit = [e for g in groups[:cut] for e in g]
    cal = [e for g in groups[cut:] for e in g]
    fit, purged = _purge(fit, cal, cfg)
    return [e["market"] for e in fit], [e["market"] for e in cal], [e["market"] for e in purged]


def inner_split(table, validation_fraction, cfg=None):
    """Hyperparameter-selection split INSIDE one training block (Step 6.1). Market level, never row level:
        * rows are grouped by market ticker (market_table) and markets with the same close time stay together;
        * close-time groups are sorted chronologically and cut by GROUP count (the latest share validates);
        * inner-training markets are purged with the same causal rule as the outer splits (max causal lookback +
          label horizon), so no inner-training information window overlaps the validation block's.
    -> (inner_train_markets, inner_validation_markets, purged_markets, info). Guarantees (asserted): zero market
    overlap, every inner-training market closes before every validation market, information windows disjoint."""
    cfg = cfg or SplitConfig()
    groups = _close_groups(table)
    info = {"close_groups": len(groups), "validation_fraction": validation_fraction, "purge_ms": cfg.purge_ms,
            "max_causal_lookback_ms": cfg.max_causal_lookback_ms, "label_horizon_ms": cfg.label_horizon_ms}
    if len(groups) < 2:
        info["status"] = "TOO_FEW_CLOSE_GROUPS"
        return [], [], [], info
    cut = int(round(len(groups) * (1 - validation_fraction)))
    cut = max(1, min(cut, len(groups) - 1))
    tr = [e for g in groups[:cut] for e in g]
    va = [e for g in groups[cut:] for e in g]
    tr, purged = _purge(tr, va, cfg)
    trm, vam = [e["market"] for e in tr], [e["market"] for e in va]
    if set(trm) & set(vam):
        raise AssertionError("inner split: a market is in both inner training and inner validation")
    if tr and max(e["close_ts_ms"] for e in tr) >= min(e["close_ts_ms"] for e in va):
        raise AssertionError("inner split: an inner-training market closes at / after a validation market")
    if tr:
        start = min(e["first_checkpoint_ts_ms"] for e in va) - cfg.max_causal_lookback_ms
        if max(e["close_ts_ms"] for e in tr) + cfg.label_horizon_ms > start:
            raise AssertionError("inner split: information windows overlap across the inner boundary")
    info.update(status="OK" if tr and va else "EMPTY_SIDE", inner_train_markets=len(trm),
                inner_validation_markets=len(vam), purged=len(purged),
                boundary_close_ts_ms=[tr[-1]["close_ts_ms"] if tr else None, va[0]["close_ts_ms"]])
    return trm, vam, [e["market"] for e in purged], info


def assert_chronological(folds, table):
    """Every training market closes strictly before every test market of its fold (tested; mutation S1)."""
    close = {e["market"]: e["close_ts_ms"] for e in table}
    for f in folds:
        if f["train"] and f["test"] and max(close[m] for m in f["train"]) >= min(close[m] for m in f["test"]):
            raise AssertionError(f"fold {f['fold']}: a training market closes at / after a test market")
    return True
