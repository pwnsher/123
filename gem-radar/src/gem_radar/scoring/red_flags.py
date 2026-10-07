"""Red flags. Only positive evidence fires a flag; UNKNOWN never does.
Any CRITICAL flag caps the final score at cfg["critical_cap"] (default 20)."""
from __future__ import annotations

from typing import Any, Optional

from ..core.enums import Severity
from ..core.types import Metric, RedFlag


def _v(m: dict[str, Metric], name: str) -> Optional[Any]:
    x = m.get(name)
    return x.value if x is not None and x.usable else None


def _src(m: dict[str, Metric], name: str) -> list[str]:
    x = m.get(name)
    return sorted({r.source for r in x.readings}) if x else []


def detect(m: dict[str, Metric], cfg: dict, chain: str, extras: Optional[dict] = None
           ) -> list[RedFlag]:
    t = cfg["thresholds"]
    extras = extras or {}
    flags: list[RedFlag] = []

    def add(code: str, sev: Severity, msg: str, *metrics: str, **ev: Any) -> None:
        evidence = {k: _v(m, k) for k in metrics}
        evidence["sources"] = sorted({s for k in metrics for s in _src(m, k)})
        evidence.update(ev)
        flags.append(RedFlag(code, sev, msg, evidence))

    C, H, M = Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM
    renounced = _v(m, "ownership_renounced")
    owner_active = renounced is False

    if _v(m, "is_honeypot") is True:
        add("HONEYPOT", C, "Honeypot evidence: sells are blocked or fail in simulation.",
            "is_honeypot")
    if _v(m, "cannot_sell_all") is True:
        add("CANNOT_SELL_ALL", C, "Holders cannot sell their full balance.", "cannot_sell_all")
    sell, buy = _v(m, "sell_tax_pct"), _v(m, "buy_tax_pct")
    if sell is not None and sell >= t["sell_tax_critical_pct"]:
        add("EXTREME_SELL_TAX", C, f"Sell tax {sell:.1f}% makes exiting prohibitively costly.",
            "sell_tax_pct")
    if buy is not None and buy >= t["buy_tax_critical_pct"]:
        add("EXTREME_BUY_TAX", C, f"Buy tax {buy:.1f}%.", "buy_tax_pct")
    if _v(m, "mint_authority_active") is True:
        add("ACTIVE_MINT_AUTHORITY", C,
            "Active mint authority allows additional supply creation.", "mint_authority_active")
    elif extras.get("goplus", {}).get("mint_function_present"):
        add("MINT_FUNCTION_UNVERIFIED_OWNER", H,
            "Contract has a mint function; whether an active owner can call it is unverified.")
    if _v(m, "freeze_authority_active") is True:
        add("ACTIVE_FREEZE_AUTHORITY", C,
            "Active freeze authority can freeze holder token accounts (blocks exits).",
            "freeze_authority_active")
    if _v(m, "owner_can_change_balance") is True:
        add("OWNER_CAN_CHANGE_BALANCES", C, "Owner can modify holder balances.",
            "owner_can_change_balance")
    if _v(m, "transfer_pausable") is True and not renounced:
        add("TRANSFERS_PAUSABLE", C if owner_active else H,
            "Transfers can be paused by the owner." if owner_active else
            "Transfers can be paused; owner status unverified.", "transfer_pausable",
            "ownership_renounced")
    if _v(m, "blacklist_enabled") is True and not renounced:
        sev = C if (owner_active and t.get("blacklist_is_critical")) else H
        add("BLACKLIST", sev, "Contract can blacklist addresses from trading.",
            "blacklist_enabled", "ownership_renounced")
    if _v(m, "permanent_delegate") is True:
        add("PERMANENT_DELEGATE", C,
            "Token-2022 permanent delegate can transfer or burn any holder's tokens.",
            "permanent_delegate")
    if _v(m, "non_transferable") is True:
        add("NON_TRANSFERABLE", C, "Token is non-transferable (cannot be sold).", "non_transferable")
    if _v(m, "rugged") is True:
        add("FLAGGED_RUGGED", C, "Token is flagged as rugged by RugCheck.", "rugged")
    dev = _v(m, "dev_pct")
    if dev is not None and dev > t["dev_critical_pct"]:
        add("DEV_CONCENTRATION", C,
            f"Dev/deployer holds {dev:.1f}% (> {t['dev_critical_pct']:.0f}% critical threshold).",
            "dev_pct", threshold=t["dev_critical_pct"])
    lp, top10 = _v(m, "lp_locked_pct"), _v(m, "top10_pct")
    if lp is not None and top10 is not None and lp < t["unlocked_lp_max_locked_pct"] \
            and top10 > t["unlocked_lp_top10_critical_pct"]:
        add("REMOVABLE_LIQUIDITY_CONCENTRATED", C,
            f"Liquidity is removable ({lp:.0f}% locked/burned) while top-10 holders own "
            f"{top10:.0f}%.", "lp_locked_pct", "top10_pct")

    # Non-critical
    if _v(m, "hidden_owner") is True:
        add("HIDDEN_OWNER", H, "Contract has a hidden owner.", "hidden_owner")
    if _v(m, "can_take_back_ownership") is True:
        add("RECLAIMABLE_OWNERSHIP", H, "Ownership can be reclaimed after renouncing.",
            "can_take_back_ownership")
    if _v(m, "tax_modifiable") is True and not renounced:
        add("TAX_MODIFIABLE", H, "Taxes/slippage can be changed by the owner.", "tax_modifiable")
    if _v(m, "transfer_hook") is True:
        add("TRANSFER_HOOK", H, "Token-2022 transfer hook runs custom code on every transfer.",
            "transfer_hook")
    if _v(m, "sell_simulation_ok") is False:
        add("SELL_UNVERIFIED", H, "Buy/sell simulation failed: sellability is unverified.",
            "sell_simulation_ok")
    if _v(m, "is_proxy") is True:
        add("UPGRADEABLE", M, "Upgradeable proxy: logic can change after launch.", "is_proxy")
    if _v(m, "trading_cooldown") is True:
        add("TRADING_COOLDOWN", M, "Trading cooldown restricts sells.", "trading_cooldown")
    if _v(m, "contract_has_code") is False:
        add("NO_BYTECODE", H, "No contract bytecode at this address on the selected chain.",
            "contract_has_code")
    if top10 is not None and top10 > t["top10_high_pct"]:
        add("HIGH_CONCENTRATION", H, f"Top-10 holders own {top10:.0f}%.", "top10_pct")
    vol, liq = _v(m, "volume_24h_usd"), _v(m, "liquidity_usd")
    if vol is not None and liq and vol / liq > t["wash_volume_liquidity_ratio"]:
        add("SUSPICIOUS_VOLUME", M, f"24h volume is {vol / liq:.0f}x liquidity (possible wash "
            "trading).", "volume_24h_usd", "liquidity_usd")
    buys, sells = _v(m, "buys_24h"), _v(m, "sells_24h")
    if buys is not None and sells == 0 and buys >= 20:
        add("NO_SELLS", H, f"{buys} buys and zero sells in 24h.", "buys_24h", "sells_24h")
    if _v(m, "metadata_mutable") is True:
        add("MUTABLE_METADATA", M, "Token metadata can still be changed.", "metadata_mutable")
    return flags


def has_critical(flags: list[RedFlag]) -> bool:
    return any(f.severity == Severity.CRITICAL for f in flags)
