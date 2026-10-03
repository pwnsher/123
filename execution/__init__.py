"""
Execution architecture foundation (Step 6.5) - PAPER / SHADOW ONLY. ZERO LIVE ORDERS.

    OrderIntent -> state machine -> persistent journal -> paper adapter -> orders / fills / positions
                -> reconciliation -> settlement / closed position

This package is infrastructure only: nothing in production (kalshi_dashboard, kalshi_bot, run_local, kalshi_core,
the collectors, the research stack) imports it, and it imports no network library and contains no exchange endpoint.
The only adapter that can act is PaperExecutionAdapter (a deterministic, scripted, in-process simulation). The
future live adapter exists only as an interface stub whose every method refuses (kalshi_core.execution's LIVE
refusal is reused, unchanged). See docs/EXECUTION_ARCHITECTURE.md.
"""
EXECUTION_ARCHITECTURE_VERSION = "execution_foundation_v1"
