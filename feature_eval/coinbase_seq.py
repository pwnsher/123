"""
EMPIRICAL validation of Coinbase Advanced Trade `sequence_num` semantics from REAL raw captures.

Step 5 treats sequence_num as ONE contiguous counter per websocket CONNECTION that heartbeat / subscription envelopes
also advance (microstructure.sources.coinbase_l2, chain "coinbase_l2:c<conn>"). Coinbase's documentation also
contains wording that sequence numbers increase for each PRODUCT. Neither is assumed here: this module measures, on
the raw envelopes stored by the Step-5 collector (source coinbase_l2, stream "ws#<conn>"):

    global_contiguous_share      consecutive envelopes of a connection with seq == previous + 1
    per_channel_contiguous_share consecutive envelopes of the same channel with seq == previous(channel) + 1
    per_product_contiguous_share consecutive l2_data envelopes carrying product P with seq == previous(P) + 1
    heartbeats_in_sequence_share heartbeat envelopes whose seq == previous envelope's seq + 1 (same sequence space)
    multi_product_envelopes      envelopes whose events carry more than one product_id
    connection_starts            first sequence numbers of each connection (reset behaviour); snapshot positions

Verdict (status)
    UNVERIFIED_REAL_FEED          default; too little real evidence (min envelopes / connections / products /
                                  heartbeats), or ambiguous shares
    SYNTHETIC_ONLY                only synthetic sessions were available (never evidence)
    CONSISTENT_WITH_CURRENT_POLICY  connection-level contiguity holds (>= consistent_share) including heartbeats
    CONTRADICTS_CURRENT_POLICY    per-product or per-channel contiguity holds while connection-level does not ->
                                  Coinbase book data FAILS CLOSED (quality REJECT for coinbase_l2 books, Coinbase-book
                                  features excluded) and the required correction is documented; old sessions are
                                  never silently reinterpreted.
Until CONSISTENT_WITH_CURRENT_POLICY, Coinbase-book features (tag COINBASE_L2_SEQUENCE) are excluded from model
candidate sets (fail closed).
"""
import json
from dataclasses import asdict, dataclass

STATUSES = ("UNVERIFIED_REAL_FEED", "SYNTHETIC_ONLY", "CONSISTENT_WITH_CURRENT_POLICY", "CONTRADICTS_CURRENT_POLICY")
REQUIRED_CORRECTION = ("Track sequence_num per product (and per channel) in microstructure.sources.coinbase_l2 and key the "
                       "reconstruction chain by (connection, product); this is a Step-5 code change that requires a new "
                       "microstructure fingerprint and a new feature-universe version. Sessions captured under the old "
                       "policy stay failed-closed; they are not reinterpreted.")


@dataclass(frozen=True)
class SequenceEvidenceConfig:
    min_envelopes: int = 5000
    min_connections: int = 2
    min_products: int = 2
    min_heartbeats: int = 30
    consistent_share: float = 0.995
    contradiction_share: float = 0.98

    def to_dict(self):
        return asdict(self)


def _conn(stream):
    _, _, n = str(stream).partition("#")
    return int(n) if n.isdigit() else 0


def analyse_raw(raw_messages, synthetic=False, config=None):
    """raw_messages: RawMessage records of source coinbase_l2 (any order; sorted by ingest_seq here)."""
    cfg = config or SequenceEvidenceConfig()
    by_conn = {}
    for r in sorted((r for r in raw_messages if r.source == "coinbase_l2" and not str(r.stream).startswith("rest:")),
                    key=lambda r: r.ingest_seq):
        try:
            m = json.loads(r.text)
        except ValueError:
            continue
        if not isinstance(m, dict) or not isinstance(m.get("sequence_num"), int):
            continue
        prods = sorted({e.get("product_id") for e in (m.get("events") or []) if isinstance(e, dict) and e.get("product_id")})
        types = sorted({e.get("type") for e in (m.get("events") or []) if isinstance(e, dict) and e.get("type")})
        by_conn.setdefault(_conn(r.stream), []).append((m["sequence_num"], m.get("channel"), prods, types))
    tot = {"g": [0, 0], "c": [0, 0], "p": [0, 0], "hb": [0, 0]}
    multi = 0
    starts = []
    envelopes = hb = 0
    products = set()
    snapshots = []
    for conn, env in sorted(by_conn.items()):
        starts.append({"connection": conn, "first_sequence_num": env[0][0], "first_channel": env[0][1]})
        last_c, last_p = {}, {}
        for i, (seq, ch, prods, types) in enumerate(env):
            envelopes += 1
            products.update(prods)
            if len(prods) > 1:
                multi += 1
            if "snapshot" in types:
                snapshots.append({"connection": conn, "sequence_num": seq, "envelope_index": i})
            if i:
                ok = seq == env[i - 1][0] + 1
                tot["g"][0] += ok
                tot["g"][1] += 1
                if ch == "heartbeats":
                    tot["hb"][0] += ok
                    tot["hb"][1] += 1
            if ch == "heartbeats":
                hb += 1
            if ch in last_c:
                tot["c"][0] += seq == last_c[ch] + 1
                tot["c"][1] += 1
            last_c[ch] = seq
            if ch == "l2_data":
                for p in prods:
                    if p in last_p:
                        tot["p"][0] += seq == last_p[p] + 1
                        tot["p"][1] += 1
                    last_p[p] = seq

    def share(k):
        a, b = tot[k]
        return (a / b) if b else None
    out = {"config": cfg.to_dict(), "envelopes": envelopes, "connections": len(by_conn), "products": sorted(products),
           "heartbeats": hb, "global_contiguous_share": share("g"), "per_channel_contiguous_share": share("c"),
           "per_product_contiguous_share": share("p"), "heartbeats_in_sequence_share": share("hb"),
           "multi_product_envelopes": multi, "connection_starts": starts[:50], "snapshots": snapshots[:50],
           "answers": {}, "synthetic": bool(synthetic)}
    g, c, p, h = share("g"), share("c"), share("p"), share("hb")
    out["answers"] = {
        "advances_globally_per_connection": None if g is None else g >= cfg.consistent_share,
        "advances_per_channel": None if c is None else c >= cfg.consistent_share,
        "advances_per_product": None if p is None else p >= cfg.consistent_share,
        "heartbeats_share_the_sequence_space": None if h is None else h >= cfg.consistent_share,
        "one_envelope_can_carry_multiple_products": multi > 0,
        "sequence_restarts_on_new_connection": (len({s["first_sequence_num"] for s in starts}) == 1) if len(starts) > 1 else None,
    }
    out["status"], out["reason"] = _verdict(out, cfg)
    if out["status"] == "CONTRADICTS_CURRENT_POLICY":
        out["required_correction"] = REQUIRED_CORRECTION
    return out


def _verdict(o, cfg):
    if o["synthetic"]:
        return "SYNTHETIC_ONLY", "synthetic captures are never evidence about the real feed"
    short = []
    if o["envelopes"] < cfg.min_envelopes:
        short.append(f"envelopes {o['envelopes']} < {cfg.min_envelopes}")
    if o["connections"] < cfg.min_connections:
        short.append(f"connections {o['connections']} < {cfg.min_connections}")
    if len(o["products"]) < cfg.min_products:
        short.append(f"products {len(o['products'])} < {cfg.min_products}")
    if o["heartbeats"] < cfg.min_heartbeats:
        short.append(f"heartbeats {o['heartbeats']} < {cfg.min_heartbeats}")
    if short:
        return "UNVERIFIED_REAL_FEED", "insufficient real evidence: " + "; ".join(short)
    g, c, p, h = (o["global_contiguous_share"], o["per_channel_contiguous_share"], o["per_product_contiguous_share"],
                  o["heartbeats_in_sequence_share"])
    if g is not None and g >= cfg.consistent_share and (h is None or h >= cfg.consistent_share):
        return "CONSISTENT_WITH_CURRENT_POLICY", f"connection-level contiguity {g:.4f} (heartbeats {h})"
    if (p is not None and p >= cfg.contradiction_share) or (c is not None and c >= cfg.contradiction_share):
        return "CONTRADICTS_CURRENT_POLICY", (f"per-product {p} / per-channel {c} contiguity while connection-level is "
                                              f"{g}: the Step-5 chain policy does not match the real feed")
    return "UNVERIFIED_REAL_FEED", f"ambiguous: global {g}, per-channel {c}, per-product {p}"


def analyse_sessions(session_dirs, config=None):
    """Pool real sessions (synthetic ones are analysed separately and never counted as evidence)."""
    from feature_eval.quality import is_synthetic
    from microstructure.replay import load_micro_sessions
    import os
    real, syn = [], []
    for d in session_dirs:
        if not os.path.isdir(os.path.join(d, "micro")):
            continue
        L = load_micro_sessions([d], include_raw=True)
        (syn if is_synthetic(d) else real).extend(r for r in L.raw if r.source == "coinbase_l2")
    res = analyse_raw(real, synthetic=False, config=config)
    if not real and syn:
        res = analyse_raw(syn, synthetic=True, config=config)
    res["real_raw_envelopes"] = len(real)
    res["synthetic_raw_envelopes_ignored"] = len(syn)
    return res


def coinbase_features_allowed(status):
    """Fail closed: Coinbase-book features enter model candidate sets only when the real feed confirmed the policy."""
    return status == "CONSISTENT_WITH_CURRENT_POLICY"
