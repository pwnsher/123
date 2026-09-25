"""
Storage controls for high-volume book data (Step 3's append-only gzip JSONL store, unchanged on disk: the same
reader, record kinds and CRC per line).

    MicroStoreWriter(dir, compression_level=6, segment_max_bytes=64 MiB, segment_max_s=None, ...)
        compression level 1..9 (Step 3 fixes 6), size- and time-based segment rotation. Every write is kept:
        nothing is sampled or dropped by the writer.
    prune_sessions(root, keep_days, dry_run=True)
        retention: lists (and, with dry_run=False, deletes) whole session directories older than keep_days by
        their manifest start time. Never touches a session without a manifest or one still RUNNING.
    storage_estimate(counts, seconds, bytes_compressed)
        bytes / hour and per-source share from an observed session (bench / manifest).
Overload: the collector never silently drops a message; if the writer cannot keep up the collector records a
BOOK_RESET (reason OVERLOAD) and a gap, which makes the affected books INVALID until a new snapshot.
"""
import json
import os
import shutil
import time
import zlib

from market_data.manifest import utc_iso
from market_data.storage import EventStoreWriter


class MicroStoreWriter(EventStoreWriter):
    def __init__(self, session_dir, compression_level=6, segment_max_bytes=64 << 20, segment_max_s=None, flush_lines=500,
                 fsync=True, clock=None):
        if not 1 <= int(compression_level) <= 9:
            raise ValueError("compression_level must be 1..9")
        super().__init__(session_dir, segment_max_bytes=segment_max_bytes, flush_lines=flush_lines, fsync=fsync)
        self.compression_level = int(compression_level)
        self.segment_max_s = segment_max_s
        self._clock = clock or time.monotonic
        self._seg_started = self._clock()
        self.bytes_uncompressed = 0
        self.rotations = 0

    def _flush_locked(self):
        if not self._buf:
            return
        t0 = time.perf_counter()
        payload = ("\n".join(self._buf) + "\n").encode("utf-8")
        member = zlib.compressobj(self.compression_level, zlib.DEFLATED, 31)
        blob = member.compress(payload) + member.flush()
        with open(self.segment_path(), "ab") as f:
            f.write(blob)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        self.bytes_compressed += len(blob)
        self.bytes_uncompressed += len(payload)
        self._seg_uncompressed += len(payload)
        self._buf = []
        self.flushes += 1
        self.flush_ms.append((time.perf_counter() - t0) * 1000.0)
        del self.flush_ms[:-1000]
        now = self._clock()
        if self._seg_uncompressed >= self.segment_max_bytes or \
                (self.segment_max_s is not None and now - self._seg_started >= self.segment_max_s):
            self._seg_no += 1
            self._seg_uncompressed = 0
            self._seg_started = now
            self.rotations += 1

    def pending(self):
        return len(self._buf)


def _manifest(d):
    for p in (os.path.join(d, "manifest.json"), os.path.join(d, "micro", "manifest.json")):
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            continue
    return None


def prune_sessions(root, keep_days, now_ms=None, dry_run=True):
    """Return [(session_dir, reason)] of sessions older than keep_days (deleted only when dry_run is False)."""
    import datetime as dt
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    out = []
    if not os.path.isdir(root):
        return out
    for name in sorted(os.listdir(root)):
        d = os.path.join(root, name)
        if not os.path.isdir(d):
            continue
        m = _manifest(d)
        if m is None or m.get("status") == "RUNNING" or not m.get("started_utc"):
            continue
        try:
            started = dt.datetime.fromisoformat(m["started_utc"].replace("Z", "+00:00"))
        except ValueError:
            continue
        age_days = (now_ms - started.timestamp() * 1000) / 86_400_000
        if age_days > keep_days:
            out.append((d, f"started {m['started_utc']} ({age_days:.1f} days > {keep_days})"))
            if not dry_run:
                shutil.rmtree(d)
    return out


def storage_estimate(bytes_compressed, bytes_uncompressed, seconds, by_source_counts=None):
    hours = max(seconds, 1e-9) / 3600.0
    est = {"seconds_observed": seconds, "bytes_compressed": bytes_compressed, "bytes_uncompressed": bytes_uncompressed,
           "compressed_bytes_per_hour": bytes_compressed / hours, "uncompressed_bytes_per_hour": bytes_uncompressed / hours,
           "compressed_gib_per_day": bytes_compressed / hours * 24 / (1 << 30),
           "compression_ratio": (bytes_uncompressed / bytes_compressed) if bytes_compressed else None,
           "estimated_utc": utc_iso()}
    if by_source_counts:
        tot = sum(by_source_counts.values()) or 1
        est["record_share_by_source"] = {k: v / tot for k, v in sorted(by_source_counts.items())}
    return est
