#!/usr/bin/env python3
"""Run every stage regression suite (1-13) EXACTLY ONCE, each in its own process.
Run:  py run_all_tests.py

KALSHI_MASTER_TEST_RUN=1 is set for the child processes so that the "all previous stages"
regression checks inside later stages do not re-run earlier suites recursively (they still do
when a stage is run on its own, e.g. py test_stage12.py)."""
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
failed = []
for i in range(1, 14):
    name = f"test_stage{i}.py"
    p = subprocess.run([sys.executable, name], cwd=here, capture_output=True, text=True, env=env)
    ok = p.returncode == 0
    print(f"{'PASS' if ok else 'FAIL'}  {name}", flush=True)
    if not ok:
        failed.append(name); print(p.stdout[-2000:], p.stderr[-2000:])
print("\nALL SUITES PASSED" if not failed else f"\nFAILED: {', '.join(failed)}")
sys.exit(1 if failed else 0)
