"""
SETTLEMENT-LABEL GATE. Which market outcomes may serve as training / evaluation labels?

Label sources (per market)
    OFFICIAL_RESULT          Kalshi's published result ("yes" / "no") captured from the Kalshi market API (a RESOLUTION
                             event). This is the payoff-defining outcome, not a settlement-derived quantity. If the
                             settlement reconstruction exists and DISAGREES with it, the market is LABEL_CONFLICT and
                             excluded (a data problem to investigate, never averaged away).
    RECONSTRUCTED_VERIFIED   no official result, but the outcome reconstructed by the Step-2 settlement engine under a
                             window convention that the gate has VERIFIED on real markets
    SETTLEMENT_UNVERIFIED    reconstructed only, convention not verified -> excluded from model / promotion research
    RULE_UNVERIFIED_FOR_MARKET  convention verified, but the contract rule is not evidenced for THIS market (e.g. a
                             rule observed only after the market closed, conflicting / unparsed market rule text):
                             diagnostic only, never gold (Step 6.2)
    UNLABELED                neither
The convention gate (settlement.resolution.verify_all on REAL markets with official expiration values): the convention
is chosen by RECONSTRUCTION AGREEMENT ONLY - never by model performance - and is VERIFIED only with enough compared
markets, a high share of EXACT expiration-value matches at each contract's official precision (Step 6.1; no universal
tolerance) and near-perfect outcome agreement, AND only when it is the UNIQUE passing convention: tied /
indistinguishable passing conventions stay SETTLEMENT_UNVERIFIED (AMBIGUOUS_CONVENTION). Competing conventions are
reported. Synthetic data -> SYNTHETIC_ONLY (never verifies anything).
"""
from dataclasses import asdict, dataclass

LABEL_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED", "SETTLEMENT_UNVERIFIED", "RULE_UNVERIFIED_FOR_MARKET",
                 "LABEL_CONFLICT", "UNLABELED")
GOLD_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED")


@dataclass(frozen=True)
class LabelGateConfig:
    min_markets_compared: int = 50
    min_exact_match_share: float = 0.98          # exact expiration-value match at the contract's OFFICIAL precision
    min_outcome_agreement_share: float = 0.99
    label_window_policy: str = "cf_rti_60s_start_incl_asof_v1"

    def to_dict(self):
        return asdict(self)


def convention_gate(markets, resolutions, observations, synthetic=False, config=None):
    """-> {status: VERIFIED | SETTLEMENT_UNVERIFIED | SYNTHETIC_ONLY, reason_code, convention, passing, ...}

    VERIFIED only when the real data UNIQUELY identifies the declared label convention: it is the ONLY policy that
    passes the empirical gate (exact expiration-value matches at official precision + outcome agreement) and the
    verifier did not report a tie. Several passing / indistinguishable policies -> SETTLEMENT_UNVERIFIED
    (AMBIGUOUS_CONVENTION), even when the declared policy is among them. The declared policy failing while another
    passes -> SETTLEMENT_UNVERIFIED (REQUIRES_VERSIONED_POLICY_MIGRATION): the label convention is never switched
    silently. Official Kalshi results do not depend on this gate."""
    from settlement.resolution import verify_all
    from settlement.rules import rule_set_fingerprint
    cfg = config or LabelGateConfig()
    va = verify_all(markets, resolutions, observations)
    pol = va["policies"]
    summary = {pid: {"compared": s["expiration_value"]["compared"],
                     "exact_after_rounding": s["expiration_value"]["exact_after_rounding"],
                     "within_half_unit": s["expiration_value"]["within_half_unit"],
                     "outcome_compared": s["compared"], "outcome_agree": s["agree"], "outcome_disagree": s["disagree"],
                     "max_raw_abs_diff": s["expiration_value"]["max_raw_abs_diff"]} for pid, s in pol.items()}
    out = {"config": cfg.to_dict(), "by_convention": summary, "verify_all_verdict": va["convention_verdict"],
           "label_window_policy": cfg.label_window_policy, "settlement_rule_set_fingerprint": rule_set_fingerprint()}
    if synthetic:
        out.update(status="SYNTHETIC_ONLY", reason_code="SYNTHETIC", convention=None,
                   reason="synthetic data never verifies a settlement convention")
        return out
    compared = max((s["compared"] for s in summary.values()), default=0)
    if compared < cfg.min_markets_compared:
        out.update(status="SETTLEMENT_UNVERIFIED", reason_code="INSUFFICIENT_MARKETS", convention=None,
                   reason=f"only {compared} real markets with an official expiration value at a known official "
                          f"precision (< {cfg.min_markets_compared})")
        return out

    def ok(s):
        return bool(s["compared"] and s["exact_after_rounding"] / s["compared"] >= cfg.min_exact_match_share and
                    s["outcome_compared"] and s["outcome_agree"] / s["outcome_compared"] >= cfg.min_outcome_agreement_share)
    passing = sorted(pid for pid, s in summary.items() if ok(s))
    out["competing_conventions_passing"] = passing
    tied = va["convention_verdict"].get("status") == "EVALUATED_TIED" and \
        cfg.label_window_policy in va["convention_verdict"].get("best_policies", [])
    if passing == [cfg.label_window_policy] and not tied:
        out.update(status="VERIFIED", reason_code="UNIQUE_CONVENTION", convention=cfg.label_window_policy,
                   reason=f"{cfg.label_window_policy} is the only passing convention: exact official-precision match "
                          f"on {summary[cfg.label_window_policy]['exact_after_rounding']}/"
                          f"{summary[cfg.label_window_policy]['compared']} markets")
    elif len(passing) > 1 or (passing == [cfg.label_window_policy] and tied):
        out.update(status="SETTLEMENT_UNVERIFIED", reason_code="AMBIGUOUS_CONVENTION", convention=None,
                   reason=f"conventions {passing} pass equally / indistinguishably: the data does not identify a "
                          "unique window convention, so reconstructed labels stay non-gold")
    elif passing:
        out.update(status="SETTLEMENT_UNVERIFIED", reason_code="REQUIRES_VERSIONED_POLICY_MIGRATION", convention=None,
                   reason=f"the declared label convention {cfg.label_window_policy} fails while {passing} passes: "
                          "adopt it only through a deliberate, versioned policy migration")
    else:
        out.update(status="SETTLEMENT_UNVERIFIED", reason_code="NO_CONVENTION_PASSES", convention=None,
                   reason="no convention reaches the agreement gate")
    return out


def market_label(labels_row, gate_status):
    """labels_row: settlement.checkpoints.labels_for(...) output. -> (y 0/1 | None, label_source)."""
    official = labels_row.get("official_result")
    recon = labels_row.get("reconstructed_outcome")
    if official in ("yes", "no"):
        if recon in ("yes", "no") and recon != official:
            return None, "LABEL_CONFLICT"
        return (1 if official == "yes" else 0), "OFFICIAL_RESULT"
    if recon in ("yes", "no"):
        # Step 6.2: a reconstruction is gold only when the CONTRACT RULE is evidenced for this very market (its own
        # captured rule text, or the series rule within its evidenced period) - never a rule observed only later
        from settlement.market_rules import GOLD_RULE_STATUSES
        rule_ok = labels_row.get("settlement_rule_status") in GOLD_RULE_STATUSES
        if gate_status == "VERIFIED" and rule_ok:
            return (1 if recon == "yes" else 0), "RECONSTRUCTED_VERIFIED"
        if gate_status == "VERIFIED":
            return (1 if recon == "yes" else 0), "RULE_UNVERIFIED_FOR_MARKET"
        return (1 if recon == "yes" else 0), "SETTLEMENT_UNVERIFIED"
    return None, "UNLABELED"


def is_gold(label_source):
    """Only gold labels may enter model / promotion research."""
    return label_source in GOLD_SOURCES
