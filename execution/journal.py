"""
Persistent, append-only execution journal (SQLite, JOURNAL_SCHEMA_VERSION 1).

Execution truth never lives only in process memory: every state transition, order reference, fill, fee, mark,
reconciliation verdict, settlement and audit record is an EVENT appended inside an explicit transaction
(BEGIN IMMEDIATE ... COMMIT; any exception -> ROLLBACK, nothing partial is ever visible). The engine persists a
transition BEFORE any externally observable follow-on work (e.g. SUBMITTING is committed before the adapter is
called).

Append-only is enforced by the database itself: triggers abort any UPDATE or DELETE of `events` / `intents`.
Every event has a deterministic event_id. Appending an event whose id already exists is a no-op when the content is
identical ("DUPLICATE") and an error when it differs (JournalConflict) - a replayed notification can never be
double-counted and history can never be silently rewritten.

reconstruct() replays the history (ordered by seq) through the authoritative transition table, so the current
state of every execution is rebuilt deterministically after a restart; an invalid history raises.
"""
import json
import sqlite3
from contextlib import contextmanager

from execution.faults import NO_FAULTS
from execution.money import jsonable
from execution.states import ExecState, check_transition

JOURNAL_SCHEMA_VERSION = 1
EVENT_KINDS = ("TRANSITION", "ORDER_REF", "FILL", "FEE", "MARK", "RECONCILIATION", "SETTLEMENT", "AUDIT")
EVENT_COLUMNS = ("event_id", "schema_version", "kind", "intent_id", "execution_key", "previous_state", "new_state",
                 "ts_ms", "reason", "actor", "market_ticker", "asset", "side", "order_id", "client_order_id",
                 "requested_size", "filled_size", "remaining_size", "limit_price", "average_fill_price", "fees",
                 "adapter_name", "adapter_event_id", "reconciliation_status", "reconciliation_reason", "payload_json")

DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    execution_key TEXT NOT NULL UNIQUE,
    client_order_id TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    asset TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    intent_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    intent_id TEXT,
    execution_key TEXT,
    previous_state TEXT,
    new_state TEXT,
    ts_ms INTEGER NOT NULL,
    reason TEXT,
    actor TEXT,
    market_ticker TEXT,
    asset TEXT,
    side TEXT,
    order_id TEXT,
    client_order_id TEXT,
    requested_size TEXT,
    filled_size TEXT,
    remaining_size TEXT,
    limit_price TEXT,
    average_fill_price TEXT,
    fees TEXT,
    adapter_name TEXT,
    adapter_event_id TEXT,
    reconciliation_status TEXT,
    reconciliation_reason TEXT,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_by_key ON events (execution_key, seq);
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
    BEGIN SELECT RAISE(ABORT, 'execution journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
    BEGIN SELECT RAISE(ABORT, 'execution journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS intents_no_update BEFORE UPDATE ON intents
    BEGIN SELECT RAISE(ABORT, 'recorded intents are immutable'); END;
CREATE TRIGGER IF NOT EXISTS intents_no_delete BEFORE DELETE ON intents
    BEGIN SELECT RAISE(ABORT, 'recorded intents are immutable'); END;
"""


class JournalError(RuntimeError):
    pass


class JournalConflict(JournalError):
    """An event id / intent id already exists with DIFFERENT content: history is never rewritten."""


def _txt(v):
    if v is None:
        return None
    v = jsonable(v)
    return v if isinstance(v, str) else str(v)


class ExecutionView:
    """The reconstructed state of one execution (journal replay)."""

    def __init__(self, execution_key, intent_id):
        self.execution_key, self.intent_id = execution_key, intent_id
        self.state = None
        self.transitions = 0
        self.order_id = None
        self.history = []                 # [(seq, previous, new, reason, actor)]
        self.last_verdict = None          # last reconciliation verdict
        self.last_verdict_payload = None

    def __repr__(self):
        return f"ExecutionView({self.intent_id}, {self.state})"


class Journal:
    def __init__(self, path, faults=None):
        self.path = path
        self.faults = faults or NO_FAULTS
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(DDL)
        row = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", (str(JOURNAL_SCHEMA_VERSION),))
        elif int(row[0]) != JOURNAL_SCHEMA_VERSION:
            raise JournalError(f"journal schema {row[0]} != supported {JOURNAL_SCHEMA_VERSION}")
        self._in_tx = False

    def close(self):
        self.conn.close()

    # ---------------- transactions ----------------
    @contextmanager
    def transaction(self):
        if self._in_tx:                                  # nested use joins the outer transaction (one atomic unit)
            yield self
            return
        self.faults.hit("before_journal_write")
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
        self.faults.hit("after_journal_write")

    def _require_tx(self):
        if not self._in_tx:
            raise JournalError("journal writes must happen inside journal.transaction()")

    # ---------------- intents ----------------
    def record_intent(self, intent, execution_key, client_order_id):
        """Insert an intent once. The same intent again -> 'DUPLICATE'; the same id with other content -> conflict."""
        self._require_tx()
        row = self.conn.execute("SELECT execution_key, content_hash FROM intents WHERE intent_id=?",
                                (intent.intent_id,)).fetchone()
        if row is not None:
            if row[0] != execution_key or row[1] != intent.content_hash():
                raise JournalConflict(f"intent {intent.intent_id} already recorded with different content")
            return "DUPLICATE"
        self.conn.execute("INSERT INTO intents (intent_id, execution_key, client_order_id, content_hash, asset, "
                          "market_ticker, side, intent_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                          (intent.intent_id, execution_key, client_order_id, intent.content_hash(), intent.asset,
                           intent.market_ticker, intent.side, json.dumps(intent.to_dict(), sort_keys=True)))
        return "APPENDED"

    def intent_row(self, intent_id=None, execution_key=None):
        q = "SELECT intent_id, execution_key, client_order_id, content_hash, intent_json FROM intents WHERE "
        row = self.conn.execute(q + ("intent_id=?" if intent_id else "execution_key=?"),
                                (intent_id or execution_key,)).fetchone()
        if row is None:
            return None
        return {"intent_id": row[0], "execution_key": row[1], "client_order_id": row[2], "content_hash": row[3],
                "intent": json.loads(row[4])}

    def intents(self):
        rows = self.conn.execute("SELECT intent_id, execution_key, client_order_id, asset, market_ticker, side, "
                                 "intent_json FROM intents ORDER BY rowid").fetchall()
        return [{"intent_id": r[0], "execution_key": r[1], "client_order_id": r[2], "asset": r[3],
                 "market_ticker": r[4], "side": r[5], "intent": json.loads(r[6])} for r in rows]

    # ---------------- events ----------------
    def append(self, event):
        """Append ONE event inside the current transaction. -> 'APPENDED' | 'DUPLICATE' (identical content)."""
        self._require_tx()
        if event.get("kind") not in EVENT_KINDS:
            raise JournalError(f"unknown event kind {event.get('kind')!r}")
        row = {c: None for c in EVENT_COLUMNS}
        row.update({k: _txt(v) if k not in ("schema_version", "ts_ms") else v for k, v in event.items()
                    if k in EVENT_COLUMNS and k != "payload_json"})
        row["schema_version"] = JOURNAL_SCHEMA_VERSION
        row["payload_json"] = json.dumps(event.get("payload") or {}, sort_keys=True, default=_txt)
        if not isinstance(row["ts_ms"], int):
            raise JournalError("event ts_ms must be an integer epoch ms")
        old = self.conn.execute(f"SELECT {', '.join(EVENT_COLUMNS)} FROM events WHERE event_id=?",
                                (row["event_id"],)).fetchone()
        if old is not None:
            if dict(zip(EVENT_COLUMNS, old)) != {c: row[c] for c in EVENT_COLUMNS}:
                raise JournalConflict(f"event {row['event_id']} already recorded with different content")
            return "DUPLICATE"
        self.conn.execute(f"INSERT INTO events ({', '.join(EVENT_COLUMNS)}) VALUES "
                          f"({', '.join('?' for _ in EVENT_COLUMNS)})", [row[c] for c in EVENT_COLUMNS])
        return "APPENDED"

    def append_many(self, events):
        """Several events as ONE atomic unit (a crash between them leaves none)."""
        self._require_tx()
        out = []
        for i, ev in enumerate(events):
            if i:
                self.faults.hit("journal_mid_transaction")
            out.append(self.append(ev))
        return out

    def events(self, execution_key=None, kind=None):
        q, args = f"SELECT seq, {', '.join(EVENT_COLUMNS)} FROM events", []
        cond = []
        if execution_key is not None:
            cond.append("execution_key=?")
            args.append(execution_key)
        if kind is not None:
            cond.append("kind=?")
            args.append(kind)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        out = []
        for r in self.conn.execute(q + " ORDER BY seq", args).fetchall():
            d = dict(zip(("seq",) + EVENT_COLUMNS, r))
            d["payload"] = json.loads(d.pop("payload_json"))
            out.append(d)
        return out

    def count(self, kind=None):
        if kind is None:
            return self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM events WHERE kind=?", (kind,)).fetchone()[0]

    # ---------------- reconstruction ----------------
    def reconstruct(self):
        """execution_key -> ExecutionView, rebuilt only from the persisted history (validating every transition)."""
        views = {}
        for r in self.intents():
            views[r["execution_key"]] = ExecutionView(r["execution_key"], r["intent_id"])
        for ev in self.events():
            v = views.get(ev["execution_key"])
            if v is None:
                continue
            if ev["kind"] == "TRANSITION":
                if ev["previous_state"] != (v.state.value if v.state else None):
                    raise JournalError(f"history of {v.intent_id} is not a chain at seq {ev['seq']}")
                check_transition(ev["previous_state"], ev["new_state"], ev["actor"],
                                 ev["payload"].get("evidence"))
                v.state = ExecState(ev["new_state"])
                v.transitions += 1
                v.history.append((ev["seq"], ev["previous_state"], ev["new_state"], ev["reason"], ev["actor"]))
                if ev["order_id"]:
                    v.order_id = ev["order_id"]
            elif ev["kind"] == "ORDER_REF" and ev["order_id"]:
                v.order_id = ev["order_id"]
            elif ev["kind"] == "RECONCILIATION":
                v.last_verdict = ev["reconciliation_status"]
                v.last_verdict_payload = ev["payload"]
        return views
