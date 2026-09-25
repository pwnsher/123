"""
Append-oriented local event store: gzip-compressed JSON-lines segments with per-line CRC32.

Layout (one directory per collection session):
    <root>/<session_id>/manifest.json              atomic JSON (temp file + rename), see manifest.py
    <root>/<session_id>/segment-000001.jsonl.gz    records, appended in batches
    <root>/<session_id>/segment-000002.jsonl.gz    ... rotated at segment_max_bytes (uncompressed)

Record line:  {"k": <kind>, "d": <data>, "c": <crc32 hex of canonical JSON [k, d]>}
kinds: raw (exact received text), event (normalized MarketEvent), failure (parse failure),
       gap, feed (health snapshot), note.

Why this format: stdlib only (no database or pyarrow), append-safe, compressed (~5-10x for JSON market
data), streamable replay, human-inspectable with `zcat`, and the RAW text is kept verbatim so
normalization can be recomputed later. Each flush writes ONE complete gzip member and fsyncs it, so a
crash can only truncate the last member; the reader reports that tail as corrupt and keeps everything
before it. Lines with a bad CRC or unknown kind are reported, never interpreted.
"""
import json
import os
import tempfile
import threading
import time
import zlib
from dataclasses import dataclass, field

KINDS = ("raw", "event", "failure", "gap", "feed", "note")
SEGMENT_PREFIX = "segment-"
SEGMENT_SUFFIX = ".jsonl.gz"


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False)


def crc(kind, data):
    return format(zlib.crc32(canonical([kind, data]).encode()), "08x")


def encode_line(kind, data):
    if kind not in KINDS:
        raise ValueError(f"unknown record kind {kind!r}")
    return canonical({"k": kind, "d": data, "c": crc(kind, data)})


def write_json_atomic(path, obj):
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(obj, f, indent=1, sort_keys=True, allow_nan=False, default=str)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


class EventStoreWriter:
    def __init__(self, session_dir, segment_max_bytes=64 << 20, flush_lines=500, fsync=True):
        self.dir = session_dir
        os.makedirs(session_dir, exist_ok=True)
        self.segment_max_bytes = segment_max_bytes
        self.flush_lines = flush_lines
        self.fsync = fsync
        self._lock = threading.Lock()
        self._buf = []
        self._seg_no = self._last_segment_number() or 1
        self._seg_uncompressed = 0
        self.records = 0
        self.bytes_compressed = 0
        self.flushes = 0
        self.flush_ms = []                       # disk-write latency per flush

    def _last_segment_number(self):
        nums = [int(n[len(SEGMENT_PREFIX):-len(SEGMENT_SUFFIX)]) for n in os.listdir(self.dir)
                if n.startswith(SEGMENT_PREFIX) and n.endswith(SEGMENT_SUFFIX)]
        return max(nums) + 1 if nums else None     # never append to a segment written by an earlier run

    def segment_path(self, n=None):
        return os.path.join(self.dir, f"{SEGMENT_PREFIX}{(n or self._seg_no):06d}{SEGMENT_SUFFIX}")

    def write(self, kind, data):
        line = encode_line(kind, data)
        with self._lock:
            self._buf.append(line)
            self.records += 1
            if len(self._buf) >= self.flush_lines:
                self._flush_locked()

    def flush(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if not self._buf:
            return
        t0 = time.perf_counter()
        payload = ("\n".join(self._buf) + "\n").encode("utf-8")
        member = zlib.compressobj(6, zlib.DEFLATED, 31)          # 31 = gzip container
        blob = member.compress(payload) + member.flush()
        with open(self.segment_path(), "ab") as f:
            f.write(blob)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        self.bytes_compressed += len(blob)
        self._seg_uncompressed += len(payload)
        self._buf = []
        self.flushes += 1
        self.flush_ms.append((time.perf_counter() - t0) * 1000.0)
        del self.flush_ms[:-1000]
        if self._seg_uncompressed >= self.segment_max_bytes:
            self._seg_no += 1
            self._seg_uncompressed = 0

    def close(self):
        self.flush()


@dataclass
class ReadResult:
    records: list = field(default_factory=list)          # [(kind, data)] in file order
    corrupt: list = field(default_factory=list)          # [(file, location, reason)]
    segments: list = field(default_factory=list)


def _members(blob):
    """Yield decompressed gzip members; a truncated/corrupt tail raises ValueError after good members."""
    data = blob
    while data:
        d = zlib.decompressobj(31)
        try:
            out = d.decompress(data)
        except zlib.error as e:
            raise ValueError(f"corrupt gzip member: {e}")
        if not d.eof:
            raise ValueError("truncated gzip member (incomplete write)")
        yield out
        data = d.unused_data


def segment_files(session_dir):
    return sorted(os.path.join(session_dir, n) for n in os.listdir(session_dir)
                  if n.startswith(SEGMENT_PREFIX) and n.endswith(SEGMENT_SUFFIX))


def read_session(session_dir):
    res = ReadResult()
    for path in segment_files(session_dir):
        res.segments.append(os.path.basename(path))
        with open(path, "rb") as f:
            blob = f.read()
        member_no = 0
        try:
            for text in _members(blob):
                member_no += 1
                for ln, line in enumerate(text.decode("utf-8", errors="replace").split("\n"), start=1):
                    if not line:
                        continue
                    where = f"member {member_no} line {ln}"
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        res.corrupt.append((os.path.basename(path), where, "invalid JSON"))
                        continue
                    k, d, c = rec.get("k"), rec.get("d"), rec.get("c")
                    if k not in KINDS:
                        res.corrupt.append((os.path.basename(path), where, f"unknown kind {k!r}"))
                    elif c != crc(k, d):
                        res.corrupt.append((os.path.basename(path), where, "CRC mismatch"))
                    else:
                        res.records.append((k, d))
        except ValueError as e:
            res.corrupt.append((os.path.basename(path), f"after member {member_no}", str(e)))
    return res
