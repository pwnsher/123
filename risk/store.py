"""
Persistent risk state (Step 6.6): an append-only SQLite risk journal (RISK_STORE_SCHEMA_VERSION 1).

Everything is an EVENT appended inside an explicit transaction (BEGIN IMMEDIATE ... COMMIT; any exception, including a
simulated process death, rolls back). Triggers abort UPDATE / DELETE. Event ids are deterministic: an identical
re-append is a no-op ("DUPLICATE"); the same id with different content raises RiskStoreConflict. All current state
(breakers, consecutive-loss count, day peak equity, approvals, consumptions) is DERIVED by replaying the history, so a
restart reconstructs it exactly and nothing authoritative lives in memory.

Event kinds / ids
    DECISION            d:<risk_decision_id>      candidate, full snapshot, policy fingerprint, decision, caps, triggers
    APPROVAL            a:<risk_decision_id>      the issued RiskApproval (+ its execution binding hash)
    APPROVAL_SUPERSEDED x:<risk_decision_id>      an unexpired, unconsumed approval replaced by a newer decision
    CONSUMPTION         c:<risk_decision_id>      the ONE intent_id / execution_key the approval authorised (appended
                                                  only by StoreApprovalBook.verify_and_consume, after every re-check)
    BREAKER             b:<type>:<n>              previous_state, new_state, action, reason, snapshot hash, policy fp, day
    TRADE_RESULT        tr:<trade_id>:<UNKNOWN|FINAL>   realized PnL of a closed trade (UNKNOWN never counts as a win)
    OBSERVATION         o:<snapshot_hash>         a snapshot observed for breakers / day peak equity without a candidate
    BREAKER_RESET_REJECTED rr:<n>                 a refused reset attempt (audit only; breaker state is unchanged)
"""
import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal

from execution.money import UNKNOWN, canon, dec
from risk.breakers import BREAKER_TYPES, check_breaker_transition, utc_day
from risk.decision import RiskApproval
from risk.faults import NO_RISK_FAULTS

RISK_STORE_SCHEMA_VERSION = 1
RISK_EVENT_KINDS = ("DECISION", "APPROVAL", "APPROVAL_SUPERSEDED", "CONSUMPTION", "BREAKER", "TRADE_RESULT",
                    "OBSERVATION", "BREAKER_RESET_REJECTED")
RISK_DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    candidate_id TEXT,
    risk_decision_id TEXT,
    breaker_type TEXT,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_by_decision ON events (risk_decision_id, seq);
CREATE INDEX IF NOT EXISTS events_by_candidate ON events (candidate_id, seq);
CREATE TRIGGER IF NOT EXISTS risk_events_no_update BEFORE UPDATE ON events
    BEGIN SELECT RAISE(ABORT, 'risk journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS risk_events_no_delete BEFORE DELETE ON events
    BEGIN SELECT RAISE(ABORT, 'risk journal is append-only'); END;
"""


class RiskStoreError(RuntimeError):
    pass


class RiskStoreConflict(RiskStoreError):
    pass


def _j(v):
    if v is UNKNOWN:
        return "UNKNOWN"
    if isinstance(v, Decimal):
        return canon(v)
    if isinstance(v, dict):
        return {k: _j(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_j(x) for x in v]
    return v


class RiskStore:
    def __init__(self, path, faults=None):
        self.path = path
        self.faults = faults or NO_RISK_FAULTS
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.executescript(RISK_DDL)
        row = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                              (str(RISK_STORE_SCHEMA_VERSION),))
        elif int(row[0]) != RISK_STORE_SCHEMA_VERSION:
            raise RiskStoreError(f"risk store schema {row[0]} is not supported ({RISK_STORE_SCHEMA_VERSION})")
        self._in_tx = False

    def close(self):
        self.conn.close()

    # ---------------- transactions ----------------
    @contextmanager
    def transaction(self, before=None, after=None):
        if self._in_tx:
            yield self
            return
        if before:
            self.faults.hit(before)
        self.conn.execute("BEGIN IMMEDIATE")
        self._in_tx = True
        try:
            yield self
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        finally:
            self._in_tx = False
        if after:
            self.faults.hit(after)

    def append(self, kind, event_id, ts_ms, payload, candidate_id=None, risk_decision_id=None, breaker_type=None):
        if not self._in_tx:
            raise RiskStoreError("risk journal writes must happen inside transaction()")
        if kind not in RISK_EVENT_KINDS:
            raise RiskStoreError(f"unknown risk event kind {kind!r}")
        pj = json.dumps(_j(payload), sort_keys=True, separators=(",", ":"))
        row = self.conn.execute("SELECT kind, candidate_id, risk_decision_id, breaker_type, payload_json FROM events "
                                "WHERE event_id=?", (event_id,)).fetchone()
        if row is not None:
            if tuple(row) != (kind, candidate_id, risk_decision_id, breaker_type, pj):
                raise RiskStoreConflict(f"risk event {event_id} already recorded with different content")
            return "DUPLICATE"
        self.conn.execute("INSERT INTO events (event_id, kind, ts_ms, candidate_id, risk_decision_id, breaker_type, "
                          "payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (event_id, kind, int(ts_ms), candidate_id, risk_decision_id, breaker_type, pj))
        return "APPENDED"

    def events(self, kind=None, candidate_id=None, risk_decision_id=None):
        q, cond, args = "SELECT seq, event_id, kind, ts_ms, candidate_id, risk_decision_id, breaker_type, " \
                        "payload_json FROM events", [], []
        for col, v in (("kind", kind), ("candidate_id", candidate_id), ("risk_decision_id", risk_decision_id)):
            if v is not None:
                cond.append(f"{col}=?")
                args.append(v)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        out = []
        for r in self.conn.execute(q + " ORDER BY seq", args):
            out.append({"seq": r[0], "event_id": r[1], "kind": r[2], "ts_ms": r[3], "candidate_id": r[4],
                        "risk_decision_id": r[5], "breaker_type": r[6], "payload": json.loads(r[7])})
        return out

    def count(self, kind=None):
        if kind is None:
            return self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM events WHERE kind=?", (kind,)).fetchone()[0]

    # ---------------- breakers ----------------
    def breaker_states(self):
        """{type: (state, day_of_last_transition)} by replaying the breaker history (validating every transition)."""
        cur = {t: ("CLEAR", None) for t in BREAKER_TYPES}
        for e in self.events("BREAKER"):
            t, p = e["breaker_type"], e["payload"]
            if p["previous_state"] != cur[t][0]:
                raise RiskStoreError(f"breaker {t} history is not a chain at seq {e['seq']}")
            check_breaker_transition(t, p["previous_state"], p["new_state"])
            cur[t] = (p["new_state"], p["day"])
        return cur

    def breaker_transition(self, breaker_type, new_state, ts_ms, reason, snapshot_hash=None, policy_fingerprint=None):
        prev = self.breaker_states()[breaker_type][0]
        action = check_breaker_transition(breaker_type, prev, new_state)
        n = sum(1 for e in self.events("BREAKER") if e["breaker_type"] == breaker_type)
        return self.append("BREAKER", f"b:{breaker_type}:{n}", ts_ms,
                           {"breaker_id": f"{breaker_type}", "breaker_type": breaker_type, "previous_state": prev,
                            "new_state": new_state, "action": action, "reason": reason, "day": utc_day(ts_ms),
                            "snapshot_hash": snapshot_hash, "policy_fingerprint": policy_fingerprint},
                           breaker_type=breaker_type)

    # ---------------- trade results / consecutive losses ----------------
    def record_trade_result(self, trade_id, realized_pnl, ts_ms):
        """realized_pnl < 0 -> +1 loss; > 0 -> the streak resets to 0; == 0 -> neither (documented); UNKNOWN -> recorded
        as unresolved (never a win; the consecutive-loss state is UNKNOWN until a FINAL result arrives)."""
        with self.transaction():
            if realized_pnl is UNKNOWN:
                return self.append("TRADE_RESULT", f"tr:{trade_id}:UNKNOWN", ts_ms,
                                   {"trade_id": trade_id, "realized_pnl": "UNKNOWN"})
            return self.append("TRADE_RESULT", f"tr:{trade_id}:FINAL", ts_ms,
                               {"trade_id": trade_id, "realized_pnl": canon(dec(realized_pnl))})

    def loss_streak(self):
        """-> (consecutive_losses, unresolved_trade_results)."""
        n, unknown, final = 0, set(), set()
        for e in self.events("TRADE_RESULT"):
            p = e["payload"]
            if p["realized_pnl"] == "UNKNOWN":
                unknown.add(p["trade_id"])
                continue
            final.add(p["trade_id"])
            v = Decimal(p["realized_pnl"])
            if v < 0:
                n += 1
            elif v > 0:
                n = 0
        return n, len(unknown - final)

    # ---------------- snapshots / day peak ----------------
    def day_peak_equity(self, day):
        """The highest equity / day-peak equity seen in any persisted snapshot captured on UTC `day` (or None)."""
        best = None
        for e in self.events("DECISION") + self.events("OBSERVATION"):
            s = e["payload"]["snapshot"]
            if utc_day(s["captured_at"]) != day:
                continue
            for k in ("account_equity", "day_peak_equity"):
                if s[k] != "UNKNOWN":
                    v = Decimal(s[k])
                    best = v if best is None or v > best else best
        return best

    # ---------------- decisions / approvals ----------------
    def decision_record(self, decision_id):
        ev = self.events("DECISION", risk_decision_id=decision_id)
        return ev[0]["payload"] if ev else None

    def candidate_hash(self, candidate_id):
        ev = self.events("DECISION", candidate_id=candidate_id)
        return ev[0]["payload"]["decision"]["candidate_hash"] if ev else None

    def approval(self, decision_id):
        ev = self.events("APPROVAL", risk_decision_id=decision_id)
        return RiskApproval.from_dict(ev[0]["payload"]["approval"]) if ev else None

    def superseded(self, decision_id):
        return bool(self.events("APPROVAL_SUPERSEDED", risk_decision_id=decision_id))

    def consumption(self, decision_id):
        ev = self.events("CONSUMPTION", risk_decision_id=decision_id)
        return ev[0]["payload"] if ev else None

    def approvals_for_candidate(self, candidate_id):
        return [RiskApproval.from_dict(e["payload"]["approval"]) for e in self.events("APPROVAL",
                                                                                      candidate_id=candidate_id)]

    def candidate_consumed(self, candidate_id):
        return any(self.consumption(a.risk_decision_id) for a in self.approvals_for_candidate(candidate_id))
