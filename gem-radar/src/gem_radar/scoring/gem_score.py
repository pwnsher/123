"""Gem Score (0-100): five deterministic category functions.

Points are earned only from verified evidence. An UNKNOWN (or STALE) input
earns none of its points, is listed under `missing`, and lowers confidence;
it never produces a red flag and is never replaced by an assumed value.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Optional

from ..core.types import METRICS, CategoryScore, Metric

Tiers = list[tuple[float, float]]  # [(threshold, points)] checked in order


def tier_at_least(v: float, tiers: Tiers) -> float:
    for threshold, pts in tiers:
        if v >= threshold:
            return pts
    return 0.0


def tier_at_most(v: float, tiers: Tiers) -> float:
    for threshold, pts in tiers:
        if v <= threshold:
            return pts
    return 0.0


class _Scorer:
    def __init__(self, key: str, label: str, max_points: float, metrics: dict[str, Metric]):
        self.cs = CategoryScore(key, label, 0.0, max_points)
        self.metrics = metrics

    def val(self, name: str) -> Optional[Any]:
        m = self.metrics.get(name)
        if m is None:
            return None
        self.cs.raw_inputs[name] = {"value": m.value, "status": m.status.value,
                                    "readings": len(m.readings)}
        if m.usable:
            self.cs.normalized_inputs[name] = m.value
            if m.status.value == "CONFLICT":
                self.cs.warnings.append(f"{METRICS[name].label}: sources disagree ({m.note})")
            return m.value
        return None

    def check(self, check: str, max_pts: float, inputs: list[str],
              fn: Callable[..., tuple[float, str]]) -> None:
        vals = [self.val(i) for i in inputs]
        if any(v is None for v in vals):
            missing = [i for i, v in zip(inputs, vals, strict=True) if v is None]
            self.cs.missing.extend(m for m in missing if m not in self.cs.missing)
            self.cs.awarded.append({"check": check, "points": 0, "max": max_pts,
                                    "detail": "UNKNOWN: " + ", ".join(missing)})
            return
        pts, detail = fn(*vals)
        pts = max(0.0, min(max_pts, pts))
        self.cs.points += pts
        self.cs.awarded.append({"check": check, "points": pts, "max": max_pts, "detail": detail})

    def done(self) -> CategoryScore:
        self.cs.points = round(self.cs.points, 2)
        return self.cs


def _usd(v: float) -> str:
    return f"${v:,.0f}"


def score_liquidity(m: dict[str, Metric], cfg: dict) -> CategoryScore:
    s = _Scorer("liquidity", "Liquidity", 25, m)
    s.check("liquidity depth", 15, ["liquidity_usd"], lambda v: (tier_at_least(
        v, [(2_000_000, 15), (500_000, 13), (100_000, 10), (25_000, 6), (5_000, 3)]), _usd(v)))
    s.check("LP burned/locked", 10, ["lp_locked_pct"], lambda v: (tier_at_least(
        v, [(95, 10), (80, 7), (50, 4)]), f"{v:.1f}% locked/burned"))
    return s.done()


def score_contract(m: dict[str, Metric], cfg: dict, chain: str) -> CategoryScore:
    s = _Scorer("contract", "Contract", 25, m)
    s.check("not a honeypot", 6, ["is_honeypot"],
            lambda hp: (0 if hp else 6, "honeypot evidence" if hp else "no honeypot detected"))

    def taxes(buy: float, sell: float) -> tuple[float, str]:
        worst = max(buy, sell)
        return tier_at_most(worst, [(1, 5), (5, 3), (10, 1)]), f"buy {buy:.1f}% / sell {sell:.1f}%"

    if chain == "solana":
        s.check("transfer fee", 5, ["transfer_tax_pct"], lambda t: (
            tier_at_most(t, [(1, 5), (5, 3), (10, 1)]), f"transfer fee {t:.2f}%"))
    else:
        s.check("buy/sell tax", 5, ["buy_tax_pct", "sell_tax_pct"], taxes)
    s.check("mint authority revoked", 5, ["mint_authority_active"],
            lambda a: (0 if a else 5, "active" if a else "revoked / not mintable"))
    if chain == "solana":
        s.check("freeze authority revoked", 4, ["freeze_authority_active"],
                lambda a: (0 if a else 4, "active" if a else "revoked"))
        s.check("no dangerous Token-2022 extensions", 3,
                ["permanent_delegate", "transfer_hook", "non_transferable"],
                lambda pd, th, nt: (0 if (pd or nt) else (1 if th else 3),
                                    f"permanent_delegate={pd} transfer_hook={th} non_transferable={nt}"))
        s.check("metadata immutable", 2, ["metadata_mutable"],
                lambda mu: (0 if mu else 2, "mutable" if mu else "immutable"))
    else:
        s.check("no pause/blacklist controls", 4, ["transfer_pausable", "blacklist_enabled"],
                lambda p, b: (4 - (2 if p else 0) - (2 if b else 0),
                              f"pausable={p} blacklist={b}"))
        s.check("verified source", 2, ["is_open_source"],
                lambda o: (2 if o else 0, "verified" if o else "unverified"))
        s.check("not upgradeable proxy", 1, ["is_proxy"],
                lambda p: (0 if p else 1, "proxy" if p else "not a proxy"))
        s.check("no owner backdoors", 2,
                ["hidden_owner", "can_take_back_ownership", "owner_can_change_balance"],
                lambda h, t, c: (0 if (h or t or c) else 2,
                                 f"hidden_owner={h} reclaim={t} change_balance={c}"))
    return s.done()


def score_holders(m: dict[str, Metric], cfg: dict) -> CategoryScore:
    s = _Scorer("holders", "Holders", 20, m)
    s.check("top-10 concentration", 8, ["top10_pct"], lambda v: (tier_at_most(
        v, [(20, 8), (30, 6), (45, 3), (60, 1)]), f"top10 {v:.1f}%"))
    s.check("largest holder", 4, ["top1_pct"], lambda v: (tier_at_most(
        v, [(5, 4), (10, 3), (20, 1)]), f"top1 {v:.1f}%"))
    s.check("dev/deployer holdings", 5, ["dev_pct"], lambda v: (tier_at_most(
        v, [(1, 5), (5, 3), (10, 1)]), f"dev {v:.2f}%"))
    s.check("holder count", 3, ["holder_count"], lambda v: (tier_at_least(
        v, [(10_000, 3), (2_000, 2), (500, 1)]), f"{v:,} holders"))
    return s.done()


def score_volume(m: dict[str, Metric], cfg: dict) -> CategoryScore:
    s = _Scorer("volume", "Volume", 15, m)
    wash = float(cfg["thresholds"]["wash_volume_liquidity_ratio"])
    s.check("24h volume", 5, ["volume_24h_usd"], lambda v: (tier_at_least(
        v, [(1_000_000, 5), (250_000, 4), (50_000, 2), (10_000, 1)]), _usd(v)))

    def ratio(vol: float, liq: float) -> tuple[float, str]:
        if liq <= 0:
            return 0, "no liquidity"
        r = vol / liq
        s.cs.normalized_inputs["volume_liquidity_ratio"] = round(r, 3)
        if r > wash:
            return 0, f"vol/liq {r:.1f}x (possible wash trading)"
        if 0.1 <= r <= 5:
            return 4, f"vol/liq {r:.2f}x"
        return (2 if r > 5 else 1), f"vol/liq {r:.2f}x"

    s.check("volume vs liquidity", 4, ["volume_24h_usd", "liquidity_usd"], ratio)

    def balance(buys: int, sells: int) -> tuple[float, str]:
        total = buys + sells
        if total == 0:
            return 0, "no trades"
        share = sells / total
        s.cs.normalized_inputs["sell_share"] = round(share, 3)
        if 0.3 <= share <= 0.7:
            return 3, f"{buys} buys / {sells} sells"
        return (1 if 0.15 <= share <= 0.85 else 0), f"{buys} buys / {sells} sells (one-sided)"

    s.check("buy/sell balance", 3, ["buys_24h", "sells_24h"], balance)
    s.check("trade count", 3, ["buys_24h", "sells_24h"], lambda b, se: (tier_at_least(
        b + se, [(2000, 3), (500, 2), (100, 1)]), f"{b + se:,} trades 24h"))
    return s.done()


def score_age_social(m: dict[str, Metric], cfg: dict, now: Optional[float] = None) -> CategoryScore:
    s = _Scorer("age_social", "Age/Social", 15, m)
    now = now or time.time()

    def age(ts: float) -> tuple[float, str]:
        days = max(0.0, (now - ts) / 86400)
        s.cs.normalized_inputs["pool_age_days"] = round(days, 2)
        return tier_at_least(days, [(365, 9), (90, 7), (30, 5), (7, 3), (1, 1)]), f"{days:.1f} days"

    s.check("pool age", 9, ["pool_created_at"], age)
    s.check("website listed", 2, ["has_website"], lambda w: (2 if w else 0, str(w)))
    s.check("social links listed", 4, ["social_link_count"], lambda c: (
        4 if c >= 2 else (2 if c == 1 else 0), f"{c} link(s); presence only, not engagement"))
    return s.done()


def score_all(m: dict[str, Metric], cfg: dict, chain: str, now: Optional[float] = None
              ) -> dict[str, CategoryScore]:
    return {
        "liquidity": score_liquidity(m, cfg),
        "contract": score_contract(m, cfg, chain),
        "holders": score_holders(m, cfg),
        "volume": score_volume(m, cfg),
        "age_social": score_age_social(m, cfg, now),
    }
