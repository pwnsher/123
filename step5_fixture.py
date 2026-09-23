"""Synthetic Step 5 fixtures — TESTS ONLY (imported by test_stage11.py)."""
import datetime as dt
import math
import random

import perp_probability as pp
import perp_shadow as ps

ENTRY_COST = 2.0


def gen_rows(pol, start_ts, n_groups=450, days=20.0, k=3.0, k_fn=None, delta_sd=0.05, seed=1,
             step4_block_frac=0.03, unfilled=0.03, unavailable=0.01):
    """Complete, internally consistent Step 4 shadow-journal rows for NEW first signals after
    start_ts, plus labels and paper calls. Four coins share each close time.
    Truth: P(UP) = p_base + k * delta_capped (k may vary by row through k_fn)."""
    rng = random.Random(seed)
    thr = float(pol["conflict_threshold"])
    rows, labels, calls = [], {}, {}
    for gi in range(n_groups):
        close = start_ts + 600 + gi * days * 86400.0 / n_groups
        for ci, coin in enumerate(("BTC", "ETH", "SOL", "XRP")):
            tk = f"S5-{gi:05d}-{coin}"
            ts = close - 300 - ci
            fav = "UP" if rng.random() < 0.5 else "DOWN"
            conf = rng.uniform(93.0, 99.0)
            ask = conf - rng.uniform(2.5, 8.0)
            p_up_pct = conf if fav == "UP" else 100.0 - conf
            p_pct = round(p_up_pct, 4)
            pb = p_pct / 100.0
            if rng.random() < step4_block_frac:
                d = -0.2 if fav == "UP" else 0.2                  # Step 4 would block these
            else:
                d = rng.gauss(0.0, delta_sd)
            pc = min(max(pb + d, 1e-6), 1 - 1e-6)
            pbf, pcf = ps.p_favored(pb, fav), ps.p_favored(pc, fav)
            cs = pcf - pbf
            dec = ps.WOULD_BLOCK if cs <= thr else ps.ALLOW
            if rng.random() < unavailable:
                dec = ps.UNAVAILABLE
            kk = k_fn(gi, coin, fav) if k_fn else k
            true_up = min(max(pb + kk * pp.cap_delta(pc - pb), 0.001), 0.999)
            y = 1 if rng.random() < true_up else 0
            win = (y == 1) if fav == "UP" else (y == 0)
            raw = conf - ask
            row = {"policy_id": pol["policy_id"], "policy_hash": pol["policy_hash"],
                   "conflict_threshold": repr(thr), "binary_ticker": tk, "coin": coin, "fav": fav,
                   "signal_ts_epoch_ms": str(int(ts * 1000)),
                   "binary_close_time": dt.datetime.fromtimestamp(close, dt.timezone.utc).isoformat(),
                   "shadow_decision": dec, "base_p_up": repr(p_pct), "base_conf": repr(round(conf, 1)),
                   "side_ask": repr(ask), "raw_edge": repr(round(raw, 1)), "net_edge": repr(round(raw - ENTRY_COST, 1)),
                   "candidate_feature_value": repr(rng.gauss(0, 3))}
            if dec != ps.UNAVAILABLE:
                row.update(model_b_p_up=repr(pb), model_c_p_up=repr(pc), model_b_p_favored=repr(pbf),
                           model_c_p_favored=repr(pcf), conflict_score=repr(cs))
            rows.append(row)
            labels[tk] = {"y": y, "coin": coin, "close": close}
            if rng.random() < unfilled:
                calls[tk] = {"ticker": tk, "coin": coin, "exit_reason": "unfilled", "win": None,
                             "per_contract": 0.0, "pnl": 0.0, "signal_ts_epoch_ms": int(ts * 1000)}
            else:
                pcn = round((100.0 - ask - ENTRY_COST) if win else (-ask - ENTRY_COST), 1)
                calls[tk] = {"ticker": tk, "coin": coin, "exit_reason": "settle", "win": win,
                             "per_contract": pcn, "pnl": pcn * 3, "contracts": 3, "signal_ts_epoch_ms": int(ts * 1000)}
    return rows, labels, calls
