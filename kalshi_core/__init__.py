"""
kalshi_core — local-first architecture layer around the LEGACY strategy (Step 1 baseline).

This package WRAPS the existing strategy; it does not replace or modify it. The legacy
decision is still made by kalshi_dashboard.evaluate() and the legacy poller, byte-for-byte
unchanged (pinned by strategy_fingerprint.py, config/strategy_baseline.json and the
regression fixtures in regression/).

Modules
    no_call      machine-readable NO_CALL reasons, mapped 1:1 from the legacy reason codes
    data_health  lightweight feed-health representation (descriptive only, never gates)
    signal       SignalDecision: the standard, serialisable decision record
    adapter      legacy evaluate() output  ->  SignalDecision (read-only, no strategy logic)
    interfaces   Protocols for the future DATA / FEATURES / MODEL / CALIBRATION / SIGNAL /
                 RISK / EXECUTION / STORAGE layers (types only; nothing implemented)
    execution    the execution SAFETY BOUNDARY: no engine here can place an order
    baseline     strategy baseline manifest + extended strategy fingerprint
    config       infrastructure config from the environment; secret names (never values)
    logging_setup structured logging with secret redaction

Import rules (enforced by test_stage17.py):
    * nothing in kalshi_core imports discord, kalshi_bot or kalshi_api_learn;
    * signal / no_call / data_health / interfaces / execution import no network library;
    * prediction code (kalshi_dashboard, kalshi_backtest, perp_*) never imports
      kalshi_core.execution.
"""

__all__ = ["no_call", "data_health", "signal", "adapter", "interfaces", "execution",
           "baseline", "config", "logging_setup"]
