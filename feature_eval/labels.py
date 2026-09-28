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
    UNLABELED                neither
The convention gate (settlement.resolution.verify_all on REAL markets with official expiration values): the convention
is chosen by RECONSTRUCTION AGREEMENT ONLY - never by model performance - and is VERIFIED only with enough compared
markets, a high share within the expiration-value tolerance and near-perfect outcome agreement. Competing conventions
are reported. Synthetic data -> SYNTHETIC_ONLY (never verifies anything).
"""
from dataclasses import asdict, dataclass

LABEL_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED", "SETTLEMENT_UNVERIFIED", "LABEL_CONFLICT", "UNLABELED")
GOLD_SOURCES = ("OFFICIAL_RESULT", "RECONSTRUCTED_VERIFIED")


@dataclass(frozen=True)
class LabelGateConfig:
    min_markets_compared: int = 50
    min_within_tolerance_share: float = 0.98
    min_outcome_agreement_share: float = 0.99
    label_window_policy: str = "cf_rti_60s_start_incl_asof_v1"

    def to_dict(self):
        return asdict(self)


def convention_gate(markets, resolutions, observations, synthetic=False, config=None):
    """-> {status: VERIFIED | SETTLEMENT_UNVERIFIED | SYNTHETIC_ONLY, convention, competing, reason, verify_all summary}"""
    from settlement.resolution import verify_all
    cfg = config or LabelGateConfig()
    va = verify_all(markets, resolutions, observations)
    pol = va["policies"]
    summary = {pid: {"compared": s["expiration_value"]["compared"], "within_tolerance": s["expiration_value"]["within_tolerance"],
                     "outcome_compared": s["compared"], "outcome_agree": s["agree"], "outcome_disagree": s["disagree"],
                     "max_abs_diff": s["expiration_value"]["max_abs_diff"]} for pid, s in pol.items()}
    out = {"config": cfg.to_dict(), "by_convention": summary, "verify_all_verdict": va["convention_verdict"],
           "label_window_policy": cfg.label_window_policy}
    if synthetic:
        out.update(status="SYNTHETIC_ONLY", convention=None, reason="synthetic data never verifies a settlement convention")
        return out
    compared = max((s["compared"] for s in summary.values()), default=0)
    if compared < cfg.min_markets_compared:
        out.update(status="SETTLEMENT_UNVERIFIED", convention=None,
                   reason=f"only {compared} real markets with an official expiration value (< {cfg.min_markets_compared})")
        return out

    def ok(s):
        return (s["compared"] and s["within_tolerance"] / s["compared"] >= cfg.min_within_tolerance_share and
                s["outcome_compared"] and s["outcome_agree"] / s["outcome_compared"] >= cfg.min_outcome_agreement_share)
    passing = sorted(pid for pid, s in summary.items() if ok(s))
    out["competing_conventions_passing"] = passing
    if cfg.label_window_policy in passing:
        out.update(status="VERIFIED", convention=cfg.label_window_policy,
                   reason=f"{cfg.label_window_policy} agrees on {summary[cfg.label_window_policy]['within_tolerance']}/"
                          f"{summary[cfg.label_window_policy]['compared']} markets"
                          + ("" if len(passing) == 1 else f"; indistinguishable from {[p for p in passing if p != cfg.label_window_policy]}"))
    elif passing:
        out.update(status="SETTLEMENT_UNVERIFIED", convention=None,
                   reason=f"the label convention {cfg.label_window_policy} fails while {passing} pass: re-generate labels "
                          f"with a verified convention (a deliberate, versioned change) before using reconstructed labels")
    else:
        out.update(status="SETTLEMENT_UNVERIFIED", convention=None, reason="no convention reaches the agreement gate")
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
        if gate_status == "VERIFIED":
            return (1 if recon == "yes" else 0), "RECONSTRUCTED_VERIFIED"
        return (1 if recon == "yes" else 0), "SETTLEMENT_UNVERIFIED"
    return None, "UNLABELED"


def is_gold(label_source):
    """Only gold labels may enter model / promotion research."""
    return label_source in GOLD_SOURCES
