"""Data model. Every externally derived value is a Reading with provenance."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional

from .enums import Category, ProviderStatus, Severity, Status

Kind = Literal["usd", "pct", "count", "bool", "ts", "ratio", "text"]
Better = Literal["higher", "lower", "false", None]


@dataclass(frozen=True)
class MetricDef:
    name: str
    category: Category
    kind: Kind
    freshness: str            # key into config["freshness_seconds"]
    better: Better = None     # which reading is the conservative one in a conflict:
                              # "higher" -> the lower reading is conservative
    rel_tol: float = 0.0      # relative tolerance before two readings conflict
    abs_tol: float = 0.0      # absolute tolerance (pct points / counts)
    important: bool = False   # a conflict here triggers deep scan
    label: str = ""


def _m(name, cat, kind, fresh, better=None, rel=0.0, abs_=0.0, important=False, label=""):
    return MetricDef(name, cat, kind, fresh, better, rel, abs_, important, label or name)


C = Category
METRICS: dict[str, MetricDef] = {d.name: d for d in [
    # market (informational, plus inputs to volume scoring)
    _m("price_usd", C.MARKET, "usd", "market", None, 0.05, label="price (USD)"),
    _m("market_cap_usd", C.MARKET, "usd", "market", None, 0.10, label="market cap (USD)"),
    _m("fdv_usd", C.MARKET, "usd", "market", None, 0.10, label="FDV (USD)"),
    _m("price_change_24h_pct", C.MARKET, "pct", "market", None, 0.0, 5.0, label="price change 24h"),
    # liquidity
    _m("liquidity_usd", C.LIQUIDITY, "usd", "liquidity", "higher", 0.25, important=True,
       label="liquidity (USD)"),
    _m("pool_count", C.LIQUIDITY, "count", "liquidity", None, 0.0, 0, label="pools"),
    _m("top_pool_share_pct", C.LIQUIDITY, "pct", "liquidity", None, 0.0, 10.0,
       label="largest pool share of liquidity"),
    _m("lp_locked_pct", C.LIQUIDITY, "pct", "liquidity", "higher", 0.0, 10.0, important=True,
       label="LP burned/locked"),
    # contract
    _m("is_honeypot", C.CONTRACT, "bool", "contract", "false", important=True, label="honeypot"),
    _m("sell_simulation_ok", C.CONTRACT, "bool", "contract", None, label="sell simulation succeeded"),
    _m("cannot_sell_all", C.CONTRACT, "bool", "contract", "false", important=True,
       label="cannot sell all"),
    _m("buy_tax_pct", C.CONTRACT, "pct", "contract", "lower", 0.0, 2.0, important=True,
       label="buy tax"),
    _m("sell_tax_pct", C.CONTRACT, "pct", "contract", "lower", 0.0, 2.0, important=True,
       label="sell tax"),
    _m("transfer_tax_pct", C.CONTRACT, "pct", "contract", "lower", 0.0, 2.0, label="transfer tax"),
    _m("mint_authority_active", C.CONTRACT, "bool", "contract", "false", important=True,
       label="mint authority active"),
    _m("freeze_authority_active", C.CONTRACT, "bool", "contract", "false", important=True,
       label="freeze authority active"),
    _m("ownership_renounced", C.CONTRACT, "bool", "contract", None, label="ownership renounced"),
    _m("owner_can_change_balance", C.CONTRACT, "bool", "contract", "false",
       label="owner can change balances"),
    _m("hidden_owner", C.CONTRACT, "bool", "contract", "false", label="hidden owner"),
    _m("can_take_back_ownership", C.CONTRACT, "bool", "contract", "false",
       label="can reclaim ownership"),
    _m("transfer_pausable", C.CONTRACT, "bool", "contract", "false", label="transfers pausable"),
    _m("blacklist_enabled", C.CONTRACT, "bool", "contract", "false", label="blacklist function"),
    _m("is_proxy", C.CONTRACT, "bool", "contract", "false", label="upgradeable proxy"),
    _m("is_open_source", C.CONTRACT, "bool", "contract", None, label="verified source"),
    _m("tax_modifiable", C.CONTRACT, "bool", "contract", "false", label="tax/slippage modifiable"),
    _m("trading_cooldown", C.CONTRACT, "bool", "contract", "false", label="trading cooldown"),
    _m("permanent_delegate", C.CONTRACT, "bool", "contract", "false",
       label="Token-2022 permanent delegate"),
    _m("transfer_hook", C.CONTRACT, "bool", "contract", "false", label="Token-2022 transfer hook"),
    _m("non_transferable", C.CONTRACT, "bool", "contract", "false", label="non-transferable"),
    _m("metadata_mutable", C.CONTRACT, "bool", "contract", "false", label="metadata mutable"),
    _m("rugged", C.CONTRACT, "bool", "contract", "false", important=True,
       label="flagged as rugged"),
    _m("contract_has_code", C.CONTRACT, "bool", "contract", None, label="contract bytecode present"),
    # holders
    _m("holder_count", C.HOLDERS, "count", "holders", "higher", 0.20, label="holders"),
    _m("top1_pct", C.HOLDERS, "pct", "holders", "lower", 0.0, 3.0, important=True,
       label="top holder %"),
    _m("top5_pct", C.HOLDERS, "pct", "holders", "lower", 0.0, 5.0, label="top 5 %"),
    _m("top10_pct", C.HOLDERS, "pct", "holders", "lower", 0.0, 5.0, important=True,
       label="top 10 %"),
    _m("dev_pct", C.HOLDERS, "pct", "holders", "lower", 0.0, 2.0, important=True,
       label="dev/deployer %"),
    # activity
    _m("volume_24h_usd", C.VOLUME, "usd", "market", "higher", 0.30, label="volume 24h (USD)"),
    _m("buys_24h", C.VOLUME, "count", "market", None, 0.25, label="buys 24h"),
    _m("sells_24h", C.VOLUME, "count", "market", None, 0.25, label="sells 24h"),
    # age / social
    _m("pool_created_at", C.AGE_SOCIAL, "ts", "age", "lower", 0.0, 86400.0,
       label="first pool created"),
    _m("has_website", C.AGE_SOCIAL, "bool", "social", None, label="website listed"),
    _m("social_link_count", C.AGE_SOCIAL, "count", "social", None, 0.0, 0,
       label="social links listed"),
]}


@dataclass
class Reading:
    """One value from one source at one time."""

    metric: str
    value: Any                # normalized value (None never appears: absent == no reading)
    source: str               # provider name (e.g. "dexscreener", "local:file.csv", "mock:...")
    fetched_at: float         # unix seconds when the source produced/captured it
    status: Status            # LIVE / CACHED / STALE for this reading
    raw: Any = None           # provider's raw value (for audit)
    note: str = ""
    is_mock: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class Metric:
    name: str
    status: Status
    value: Any = None                         # value used for scoring (None = not scorable)
    readings: list[Reading] = field(default_factory=list)
    disagreement: Optional[dict] = None       # set when CONFLICT
    note: str = ""

    @property
    def definition(self) -> MetricDef:
        return METRICS[self.name]

    @property
    def usable(self) -> bool:
        return self.value is not None and self.status in (Status.LIVE, Status.CACHED, Status.CONFLICT)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status.value,
            "value": self.value,
            "readings": [r.to_dict() for r in self.readings],
            "disagreement": self.disagreement,
            "note": self.note,
        }


@dataclass
class ProviderReport:
    name: str
    status: ProviderStatus
    fetched_at: Optional[float] = None
    message: str = ""
    readings: list[Reading] = field(default_factory=list)
    extra: dict = field(default_factory=dict)  # non-metric facts (e.g. pool addresses)

    def to_dict(self, with_readings: bool = False) -> dict:
        d: dict[str, Any] = {"name": self.name, "status": self.status.value, "fetched_at": self.fetched_at,
             "message": self.message}
        if with_readings:
            d["readings"] = [r.to_dict() for r in self.readings]
        return d


@dataclass
class CategoryScore:
    key: str
    label: str
    points: float
    max_points: float
    raw_inputs: dict = field(default_factory=dict)
    normalized_inputs: dict = field(default_factory=dict)
    awarded: list[dict] = field(default_factory=list)   # [{check, points, max, detail}]
    missing: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RedFlag:
    code: str
    severity: Severity
    message: str
    evidence: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"code": self.code, "severity": self.severity.value, "message": self.message,
                "evidence": self.evidence}
