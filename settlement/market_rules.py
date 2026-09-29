"""
PER-MARKET contract-rule provenance (Step 6.2).

The static series table (settlement.rules) is only a fallback / expectation. Each market's OWN rule text, captured from
the Kalshi market object (rules_primary / rules_secondary), is retained verbatim and interpreted by a NARROW,
DETERMINISTIC parser for the known 15-minute crypto wording. Nothing here uses an LLM or any fuzzy interpretation:
unknown wording fails closed.

    contract_snapshot(obj, source, capture_ts_ms, schema_fingerprint)  -> the retained rule / fee metadata of one market
    parse_rule_text(primary, secondary, asset)                          -> structured interpretation (or UNRECOGNIZED)
    resolve_market_rule(market)                                         -> (rule for the outcome | None, provenance)

Resolution priority
    1. a TRUSTED per-market snapshot (Kalshi market API, integrity hash intact, same ticker) whose text parses:
         consistent with the static series rule (or no documented static rule)  -> RULE_VERIFIED_FOR_MARKET
         contradicting the static rule / the market strike / another snapshot     -> RULE_CONFLICT (fail closed)
       a trusted snapshot whose text does NOT parse                               -> RULE_TEXT_UNRECOGNIZED (fail closed;
                                                                                    the static table never overrides it)
    2. no snapshot: the documented static series rule, only for the period it is evidenced for:
         a verified effective interval covering the close                         -> RULE_VERIFIED_FOR_MARKET
         observed current at / before the market's close (close >= observed)       -> RULE_CURRENT_OBSERVED
         observed only AFTER the market closed (history unknown)                    -> RULE_HISTORICALLY_UNVERIFIED
                                                                                    (diagnostic outcome only)
         static rule not documented (e.g. SOL)                                      -> RULE_UNVERIFIED (fail closed)
    3. nothing                                                                      -> RULE_UNKNOWN (fail closed)

Only RULE_VERIFIED_FOR_MARKET and RULE_CURRENT_OBSERVED may support a gold RECONSTRUCTED label (GOLD_RULE_STATUSES).
Official Kalshi results are authoritative independently of this module.
"""
import hashlib
import json
import re
from dataclasses import replace

from settlement.rules import GT, GTE, SettlementRule, exact, rule_for

SNAPSHOT_VERSION = 1
PARSER_VERSION = "crypto15m_rule_parser_v1"
TRUSTED_SNAPSHOT_SOURCES = ("kalshi_market_api",)
FEE_FIELDS = ("fee_type", "fee_multiplier", "fee_type_override", "fee_multiplier_override")

RULE_VERIFIED_FOR_MARKET = "RULE_VERIFIED_FOR_MARKET"
RULE_CURRENT_OBSERVED = "RULE_CURRENT_OBSERVED"
RULE_HISTORICALLY_UNVERIFIED = "RULE_HISTORICALLY_UNVERIFIED"
RULE_CONFLICT = "RULE_CONFLICT"
RULE_TEXT_UNRECOGNIZED = "RULE_TEXT_UNRECOGNIZED"
RULE_SNAPSHOT_UNTRUSTED = "RULE_SNAPSHOT_UNTRUSTED"
RULE_UNVERIFIED = "RULE_UNVERIFIED"
RULE_UNKNOWN = "RULE_UNKNOWN"
GOLD_RULE_STATUSES = (RULE_VERIFIED_FOR_MARKET, RULE_CURRENT_OBSERVED)
DIAGNOSTIC_OUTCOME_STATUSES = GOLD_RULE_STATUSES + (RULE_HISTORICALLY_UNVERIFIED,)

INDEX_NAMES = {"BTC": ("brti", "bitcoin real-time index", "bitcoin real time index"),
               "ETH": ("ethusd_rti", "ether real-time index", "ether real time index", "ethereum real-time index",
                       "ethereum real time index"),
               "SOL": ("solusd_rti", "solana real-time index", "solana real time index"),
               "XRP": ("xrpusd_rti", "xrp real-time index", "xrp real time index")}
_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8}


def rule_text_sha256(primary, secondary):
    return hashlib.sha256(json.dumps([primary or "", secondary or ""], ensure_ascii=False).encode("utf-8")).hexdigest()


def _norm_str(v):
    return v if isinstance(v, str) else None


def contract_snapshot(obj, source="kalshi_market_api", capture_ts_ms=None, schema_fingerprint="", event_obj=None):
    """Retain a market's ORIGINAL rule text + fee metadata (market-level values win over event-level ones)."""
    if not isinstance(obj, dict):
        return None
    ticker = obj.get("ticker")
    primary, secondary = _norm_str(obj.get("rules_primary")), _norm_str(obj.get("rules_secondary"))
    fee = {}
    for src_name, src in (("event", event_obj), ("market", obj)):
        if isinstance(src, dict):
            for k in FEE_FIELDS:
                if k in src:
                    fee[k] = src[k]
                    fee[f"{k}_from"] = src_name
    updated = None
    for k in ("updated_time", "last_updated_time"):
        if isinstance(obj.get(k), str):
            updated = obj[k]
            break
    series = obj.get("series_ticker") if isinstance(obj.get("series_ticker"), str) else (
        str(ticker).split("-")[0] if isinstance(ticker, str) else None)
    return {"snapshot_version": SNAPSHOT_VERSION, "ticker": ticker, "series_ticker": series,
            "event_ticker": obj.get("event_ticker") if isinstance(obj.get("event_ticker"), str) else None,
            "rules_primary": primary, "rules_secondary": secondary,
            "rule_text_sha256": rule_text_sha256(primary, secondary) if (primary or secondary) else None,
            "capture_ts_ms": capture_ts_ms, "market_updated_ts": updated, "source": source,
            "schema_fingerprint": schema_fingerprint, "fee_metadata": fee,
            "fee_metadata_state": "CAPTURED" if fee else "NOT_PRESENT"}


def parse_rule_text(primary, secondary, asset):
    """Deterministic, narrowly scoped interpretation of the known crypto-15m rule wording. Anything else is
    UNRECOGNIZED. Recognised: 'at least' / 'at or above' (GREATER_THAN_OR_EQUAL), 'above' (GREATER_THAN); 'nearest N
    decimal place(s)'; the asset's CF Benchmarks index id / name; a 60-second average; an optional target number."""
    text = " ".join(" ".join(x for x in (primary or "", secondary or "") if x).split())
    low = text.lower()
    out = {"parser_version": PARSER_VERSION, "status": "UNRECOGNIZED", "comparison_operator": None,
           "settlement_decimal_places": None, "index_confirmed": False, "averaging_60s_confirmed": False,
           "target_value": None, "reasons": [], "matched": []}
    if not text:
        out["reasons"].append("no rule text")
        return out
    comps = set()
    rest = low
    for pat in (r"\bat least\b", r"\bat or above\b"):
        if re.search(pat, rest):
            comps.add(GTE)
            out["matched"].append(pat)
            rest = re.sub(pat, " ", rest)
    if re.search(r"\babove\b", rest):
        comps.add(GT)
        out["matched"].append(r"\babove\b")
    if len(comps) != 1:
        out["reasons"].append(f"comparator not uniquely recognised ({sorted(comps) or 'none'})")
    else:
        out["comparison_operator"] = comps.pop()
    dps = {int(m) if m.isdigit() else _WORDS.get(m) for m in
           re.findall(r"\bnearest (\d+|one|two|three|four|five|six|seven|eight) decimal places?\b", low)}
    dps.discard(None)
    if len(dps) != 1:
        out["reasons"].append(f"settlement precision not uniquely recognised ({sorted(dps) or 'none'})")
    else:
        out["settlement_decimal_places"] = dps.pop()
    names = INDEX_NAMES.get(asset, ())
    out["index_confirmed"] = any(n in low for n in names)
    others = [a for a, ns in INDEX_NAMES.items() if a != asset and any(n in low for n in ns)]
    if not out["index_confirmed"]:
        out["reasons"].append(f"the {asset} CF Benchmarks index is not named")
    if others:
        out["reasons"].append(f"another asset's index is named: {others}")
    out["averaging_60s_confirmed"] = bool(re.search(r"\b(60|sixty)[- ]seconds?\b", low)) and "average" in low
    if not out["averaging_60s_confirmed"]:
        out["reasons"].append("the 60-second average is not described")
    m = re.search(r"\b(?:at least|at or above|above) \$?([0-9][0-9,]*(?:\.[0-9]+)?)", low)
    if m:
        out["target_value"] = m.group(1).replace(",", "")
    if not out["reasons"]:
        out["status"] = "PARSED"
    return out


def _snapshot_trusted(snap, market):
    if not isinstance(snap, dict):
        return False, "no snapshot"
    if snap.get("source") not in TRUSTED_SNAPSHOT_SOURCES:
        return False, f"untrusted snapshot source {snap.get('source')!r}"
    if snap.get("ticker") != market.ticker:
        return False, "snapshot ticker differs from the market"
    if not (snap.get("rules_primary") or snap.get("rules_secondary")):
        return False, "snapshot carries no rule text"
    if snap.get("rule_text_sha256") != rule_text_sha256(snap.get("rules_primary"), snap.get("rules_secondary")):
        return False, "rule-text hash does not match the retained text"
    return True, ""


def _market_rule(market, snap, parsed):
    cap = snap.get("capture_ts_ms")
    return SettlementRule(
        rule_id=f"market:{market.ticker}:{snap['rule_text_sha256'][:16]}", version=1,
        series=(market.series or str(market.ticker).split("-")[0]).upper(), asset=market.asset,
        comparison_operator=parsed["comparison_operator"], settlement_decimal_places=parsed["settlement_decimal_places"],
        rounding_method="NEAREST", tie_behavior="UNSPECIFIED", window_policy_id="cf_rti_60s_start_incl_asof_v1",
        reconstruction_policy_id="strict_v1",
        rule_source=f"captured market rules_primary/rules_secondary ({snap.get('source')}); {PARSER_VERSION}",
        rule_url="", observed_date="", observed_ts_ms=int(cap) if isinstance(cap, int) else 0,
        effective_from_ms=None, effective_to_ms=None, status="DOCUMENTED",
        note=f"rule_text_sha256={snap['rule_text_sha256']}")


def resolve_market_rule(market, rules=None):
    """-> (SettlementRule for computing an outcome | None, provenance dict). See the module docstring."""
    from settlement.assets import series_of
    kw = {} if rules is None else {"rules": rules}
    static = rule_for(market.series or series_of(market.ticker), market.close_ts_ms, **kw)
    snap = market.rule_snapshot
    info = {"status": None, "basis": None, "gold_eligible": False, "rule_text_sha256": None, "parsed": None,
            "static_rule_id": static.rule_id if static else None,
            "static_rule_fingerprint": static.fingerprint() if static else None, "reasons": [],
            "rule_id": None, "rule_fingerprint": None}
    if snap is not None:
        ok, why = _snapshot_trusted(snap, market)
        info["rule_text_sha256"] = snap.get("rule_text_sha256") if isinstance(snap, dict) else None
        if not ok:
            if isinstance(snap, dict) and (snap.get("rules_primary") or snap.get("rules_secondary")):
                info.update(status=RULE_SNAPSHOT_UNTRUSTED, basis="SNAPSHOT", reasons=[why])
                return None, info
            snap = None                                 # an empty snapshot is the same as no snapshot
    if snap is not None:
        parsed = parse_rule_text(snap.get("rules_primary"), snap.get("rules_secondary"), market.asset)
        info["parsed"] = parsed
        info["basis"] = "MARKET_RULE_TEXT"
        if snap.get("conflicting_snapshots"):
            info.update(status=RULE_CONFLICT, reasons=["the market's rule text changed between snapshots"])
            return None, info
        if parsed["status"] != "PARSED":
            info.update(status=RULE_TEXT_UNRECOGNIZED, reasons=parsed["reasons"])
            return None, info
        conflicts = []
        if static is not None and static.known:
            if static.comparison_operator != parsed["comparison_operator"]:
                conflicts.append(f"comparator {parsed['comparison_operator']} vs static {static.comparison_operator}")
            if static.settlement_decimal_places != parsed["settlement_decimal_places"]:
                conflicts.append(f"precision {parsed['settlement_decimal_places']} vs static "
                                 f"{static.settlement_decimal_places}")
        if parsed["target_value"] is not None and market.strike is not None and \
                exact(parsed["target_value"]) != exact(market.strike):
            conflicts.append(f"rule target {parsed['target_value']} vs market strike {market.strike}")
        if conflicts:
            info.update(status=RULE_CONFLICT, reasons=conflicts)
            return None, info
        rule = _market_rule(market, snap, parsed)
        info.update(status=RULE_VERIFIED_FOR_MARKET, gold_eligible=True, rule_id=rule.rule_id,
                    rule_fingerprint=rule.fingerprint())
        return rule, info
    info["basis"] = "STATIC_SERIES_RULE"
    if static is None:
        info.update(status=RULE_UNKNOWN, reasons=["no captured market rule and no static series rule"])
        return None, info
    if not static.known:
        info.update(status=RULE_UNVERIFIED, reasons=["static series rule is not documented"])
        return None, info
    info.update(rule_id=static.rule_id, rule_fingerprint=static.fingerprint())
    if static.effective_from_ms is not None and static.applies_to(market.close_ts_ms):
        info.update(status=RULE_VERIFIED_FOR_MARKET, gold_eligible=True, basis="STATIC_RULE_VERIFIED_INTERVAL")
        return static, info
    if market.close_ts_ms is not None and market.close_ts_ms >= static.observed_ts_ms:
        info.update(status=RULE_CURRENT_OBSERVED, gold_eligible=True)
        return static, info
    info.update(status=RULE_HISTORICALLY_UNVERIFIED,
                reasons=["the static rule was observed only after this market closed; its historical effective "
                         "period is unknown"])
    return static, info


def with_snapshots(market, snapshots):
    """Attach the market's captured snapshots (several captures of the same market must carry the same text)."""
    snaps = [s for s in snapshots if isinstance(s, dict) and s.get("rule_text_sha256")]
    if not snaps:
        return market
    hashes = sorted({s["rule_text_sha256"] for s in snaps})
    first = min(snaps, key=lambda s: (s.get("capture_ts_ms") or 0))
    snap = dict(first, all_rule_text_sha256=hashes)
    if len(hashes) > 1:
        snap["conflicting_snapshots"] = True
    return replace(market, rule_snapshot=snap)
