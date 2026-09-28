"""
Research calibration methods (NOT production calibration) - strictly chronological, with predeclared sample gates.

    none      the model probability as is
    platt     logistic on the logit of the model probability (2 parameters)
    isotonic  pool-adjacent-violators on the model probability (a step function)

Calibration observations are the LATEST part of a fold's training period (feature_eval.splits.calibration_split): they
are strictly later than the model-fit observations and strictly earlier than the evaluation block. Gates (independent
markets in the calibration set) are predeclared: Platt needs >= min_platt_markets, isotonic >= min_isotonic_markets
(substantially larger); below the gate the method is DISABLED and reported as such, never silently run.
"""
from dataclasses import asdict, dataclass

from feature_eval.metrics import logit
from feature_eval.models import fit_logistic, predict_logistic

METHODS = ("none", "platt", "isotonic")


@dataclass(frozen=True)
class CalibrationGates:
    min_platt_markets: int = 40
    min_isotonic_markets: int = 400

    def to_dict(self):
        return asdict(self)


def gate(method, n_markets, gates=None):
    g = gates or CalibrationGates()
    if method == "none":
        return True, "no calibration"
    if method == "platt":
        return (n_markets >= g.min_platt_markets,
                f"{n_markets} calibration markets vs gate {g.min_platt_markets}")
    if method == "isotonic":
        return (n_markets >= g.min_isotonic_markets,
                f"{n_markets} calibration markets vs gate {g.min_isotonic_markets}")
    raise ValueError(method)


class Platt:
    def fit(self, p, y, w):
        self.beta = fit_logistic([[logit(v)] for v in p], y, w)
        return self

    def apply(self, p):
        return predict_logistic(self.beta, [[logit(v)] for v in p])


class Isotonic:
    """Weighted PAV; piecewise-constant with linear interpolation-free lookup (right-continuous steps)."""

    def fit(self, p, y, w):
        pts = sorted(zip(p, y, w))
        blocks = []                                   # [x_max, sum_wy, sum_w]
        for x, yi, wi in pts:
            blocks.append([x, wi * yi, wi])
            while len(blocks) > 1 and blocks[-2][1] / blocks[-2][2] >= blocks[-1][1] / blocks[-1][2]:
                b = blocks.pop()
                blocks[-1][0] = b[0]
                blocks[-1][1] += b[1]
                blocks[-1][2] += b[2]
        self.xs = [b[0] for b in blocks]
        self.vs = [b[1] / b[2] for b in blocks]
        return self

    def apply(self, p):
        import bisect
        out = []
        for v in p:
            i = min(bisect.bisect_left(self.xs, v), len(self.xs) - 1)
            out.append(min(1 - 1e-6, max(1e-6, self.vs[i])))
        return out


def fit_calibrator(method, p, y, w, n_markets, gates=None):
    ok, why = gate(method, n_markets, gates)
    if not ok:
        return None, {"method": method, "status": "DISABLED_BY_GATE", "reason": why}
    if method == "none":
        return None, {"method": "none", "status": "OK"}
    c = (Platt() if method == "platt" else Isotonic()).fit(p, y, w)
    return c, {"method": method, "status": "OK", "reason": why, "calibration_rows": len(p), "calibration_markets": n_markets}
