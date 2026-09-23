#!/usr/bin/env python3
"""Stage 5 regression tests. Run:  py test_stage5.py"""
import os
import kalshi_dashboard as k

k.WATCHER_STATE_FILE = "_t5_state"

def _clean():
    if os.path.exists(k.WATCHER_STATE_FILE):
        os.remove(k.WATCHER_STATE_FILE)

def run(name, fn):
    fn(); print(f"PASS  {name}")

# A pause must persist and survive a "reconnect" (re-reading state).
def test_pause_persists_across_reconnect():
    _clean()
    k.set_watcher_state(False)                 # user pauses
    assert not k.RUNNING.is_set()
    k.RUNNING.set()                            # simulate a stray reconnect flipping it on
    state = k.load_watcher_state()             # on_ready re-reads intended state
    assert state == "paused" and not k.RUNNING.is_set(), "reconnect must not resume a paused watcher"

# A running state also persists.
def test_running_persists():
    _clean()
    k.set_watcher_state(True)
    k.RUNNING.clear()
    state = k.load_watcher_state()
    assert state == "running" and k.RUNNING.is_set()

# No state file yet -> defaults to running.
def test_default_running():
    _clean()
    assert k.load_watcher_state() == "running"

# Real timezone hour conversion works (or falls back cleanly).
def test_local_hour():
    import datetime as dt
    ts = int(dt.datetime(2026, 7, 1, 18, 0, tzinfo=dt.timezone.utc).timestamp())  # 18:00 UTC
    h = k.local_hour(ts)
    assert 0 <= h <= 23
    if k.LOCAL_TZ is not None:
        assert h == 14, f"18:00 UTC should be 14:00 ET in July, got {h}"

if __name__ == "__main__":
    run("pause persists across reconnect", test_pause_persists_across_reconnect)
    run("running persists", test_running_persists)
    run("defaults to running", test_default_running)
    run("local timezone hour", test_local_hour)
    _clean()
    print("\nAll Stage 5 tests passed.")
