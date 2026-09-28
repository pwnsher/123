"""
SYNTHETIC research matrices - for CODE CORRECTNESS ONLY (every row / meta is flagged synthetic_only; results computed
on them are SYNTHETIC_ONLY and are written to a separate self-test file, never to the real Step-6 outputs).

Planted structure (known ground truth, so the machinery can be checked):
    y ~ Bernoulli(sigmoid(1.2 u + signal_strength * z))           u: what the legacy probability knows, z: hidden driver
    legacy_p_up = sigmoid(1.2 u)                                    the frozen-baseline stand-in
    PLANTED_SIGNAL   SPOT_PRICE_MOMENTUM: mom.syn.z (z + small noise), mom.syn.z_dup (exact duplicate),
                     mom.syn.const (constant), mom.syn.z_scaled (2 x mom.syn.z: algebraically identical)
    PLANTED_NOISE    PERP_FLOW, ORDER_FLOW (40 % missing), SETTLEMENT, SPOT_VOLATILITY (vol.cf.rv.5m = regime slices)
    COINBASE-tagged  micro.coinbase.syn_noise (gated unless the sequence policy is verified)
    EXECUTION_STATE  kalshi.yes_ask (never a model input)
Executable ladders are drawn around the legacy price; some are too thin for the larger research sizes
(NOT_EXECUTABLE).
"""
import random

from feature_eval.dataset import CHECKPOINT_GRID_S, ResearchMatrix, dataset_fingerprint
from feature_eval.metrics import sigmoid

SESSION_ID = "SYNTHETIC-step6-selftest"
ASSETS = ("BTC", "ETH", "SOL", "XRP")


def _rec(name, layer, family, role="MODEL_CANDIDATE", tags=()):
    return {"name": name, "layer": layer, "family": family, "role": role, "tags": list(tags), "assets": list(ASSETS)}


def records():
    return [
        _rec("mom.syn.z", "STEP3_SPOT", "SPOT_PRICE_MOMENTUM"),
        _rec("mom.syn.z_dup", "STEP3_SPOT", "SPOT_PRICE_MOMENTUM"),
        _rec("mom.syn.z_scaled", "STEP3_SPOT", "SPOT_PRICE_MOMENTUM"),
        _rec("mom.syn.const", "STEP3_SPOT", "SPOT_PRICE_MOMENTUM"),
        _rec("mom.syn.noise", "STEP3_SPOT", "SPOT_PRICE_MOMENTUM"),
        _rec("vol.cf.rv.5m", "STEP3_SPOT", "SPOT_VOLATILITY"),
        _rec("vol.syn.noise", "STEP3_SPOT", "SPOT_VOLATILITY"),
        _rec("settle.syn.noise", "STEP2_SETTLEMENT", "SETTLEMENT"),
        _rec("perp.syn.noise1", "STEP4_PERP", "PERP_FLOW"),
        _rec("perp.syn.noise2", "STEP4_PERP", "PERP_FLOW"),
        _rec("micro.syn.flow1", "STEP5_MICRO", "ORDER_FLOW"),
        _rec("micro.syn.flow2", "STEP5_MICRO", "ORDER_FLOW"),
        _rec("micro.coinbase.syn_noise", "STEP5_MICRO", "SPOT_BOOK", tags=("COINBASE_L2_SEQUENCE",)),
        _rec("kalshi.yes_ask", "STEP3_SPOT", "KALSHI_MARKET_STATE", role="EXECUTION_STATE"),
    ]


def make_matrix(n_slots=200, assets=ASSETS, checkpoints_s=CHECKPOINT_GRID_S, seed=7, signal_strength=1.0,
                missing_share=0.4, start_ms=1_700_000_000_000, thin_book_share=0.3):
    rng = random.Random(seed)
    recs = records()
    cols = [r["name"] for r in recs]
    rows = []
    for s in range(n_slots):
        close = start_ms + (s + 1) * 900_000
        for a in assets:
            tk = f"SYN{a}-{s:05d}"
            u = rng.gauss(0, 1)
            z = rng.gauss(0, 1)
            y = 1 if rng.random() < sigmoid(1.2 * u + signal_strength * z) else 0
            vol = abs(rng.gauss(1.0, 0.4))
            settle_noise = rng.gauss(0, 1)
            for cp in checkpoints_s:
                t = close - cp * 1000
                leg = sigmoid(1.2 * u + rng.gauss(0, 0.05))
                zf = z + rng.gauss(0, 0.2)
                vals = {"mom.syn.z": zf, "mom.syn.z_dup": zf, "mom.syn.z_scaled": 2 * zf, "mom.syn.const": 1.0,
                        "mom.syn.noise": rng.gauss(0, 1), "vol.cf.rv.5m": vol, "vol.syn.noise": rng.gauss(0, 1),
                        "settle.syn.noise": settle_noise, "perp.syn.noise1": rng.gauss(0, 1),
                        "perp.syn.noise2": rng.gauss(0, 1), "micro.syn.flow1": rng.gauss(0, 1),
                        "micro.syn.flow2": rng.gauss(0, 1), "micro.coinbase.syn_noise": rng.gauss(0, 1),
                        "kalshi.yes_ask": None}
                sts = {}
                for n in cols:
                    sts[n] = "READY"
                    if n.startswith("micro.syn") and rng.random() < missing_share:
                        vals[n], sts[n] = None, "NOT_READY"
                price = min(97, max(3, round(leg * 100)))
                thin = rng.random() < thin_book_share
                q = (5.0, 3.0) if thin else (40.0, 60.0)
                ex = {"yes_ask_ladder": [[price + 1, q[0]], [price + 2, q[1]]],
                      "no_ask_ladder": [[100 - price + 1, q[0]], [100 - price + 2, q[1]]],
                      "yes_bid": price - 1, "yes_ask": price + 1, "source": "synthetic_book"}
                vals["kalshi.yes_ask"] = price + 1
                rows.append({"market_ticker": tk, "asset": a, "checkpoint_s": cp, "checkpoint_ts_ms": t,
                             "close_ts_ms": close, "strike": 100.0, "session_id": SESSION_ID,
                             "x": [vals[n] for n in cols], "st": [sts[n] for n in cols], "legacy_p_up": leg,
                             "legacy_status": "OK", "execution": ex, "y": y, "label_source": "OFFICIAL_RESULT",
                             "official_result": "yes" if y else "no", "synthetic": True})
    smeta = [{"session_id": SESSION_ID, "raw_store_sha256": f"synthetic-seed-{seed}-slots-{n_slots}", "synthetic": True}]
    fp, body = dataset_fingerprint(smeta, "synthetic-universe", {"gate_status": "SYNTHETIC_ONLY"}, checkpoints_s,
                                   assets, [], {"seed": seed, "signal_strength": signal_strength,
                                                "missing_share": missing_share, "synthetic_mode": True})
    meta = {"dataset_fingerprint": fp, "fingerprint_body": body, "synthetic_only": True, "assets": list(assets),
            "checkpoint_grid_s": list(checkpoints_s), "sessions_used": [SESSION_ID], "session_meta": smeta,
            "settlement_gate": {"status": "SYNTHETIC_ONLY"}, "quality_reports": {}, "rows": len(rows)}
    return ResearchMatrix(cols, rows, meta), recs


