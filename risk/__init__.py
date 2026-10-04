"""
Step 6.6 risk manager foundation - PAPER / SHADOW ONLY, ZERO LIVE ORDERS.

The deterministic portfolio / trade risk layer that sits directly above the Step 6.5 execution subsystem:

    RiskCandidate -> RiskManager -> (RiskSnapshot + RiskPolicy) -> RiskDecision (APPROVE / REDUCE / VETO)
                  -> RiskApproval (APPROVE / REDUCE only) -> OrderIntent -> Step 6.5 ExecutionEngine (paper)

It generates no prediction, creates no CALL, never overrides NO_CALL and never places or cancels an order. It makes
no network request of any kind: every input (candidate, snapshot) is passed in. Nothing in production imports it.
See docs/RISK_ARCHITECTURE.md.
"""
RISK_ARCHITECTURE_VERSION = "risk_foundation_v1"
