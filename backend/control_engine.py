"""
Minimal control engine: maps analysis signals to actionable control states.

States are recommendations / effective policy inside Algo Sphere only (no live trading).
"""

from dataclasses import dataclass

CONTROL_STATES = frozenset({"MONITOR", "REDUCE", "BOOST", "KILL"})

# Actionable signal strings for external consumers (e.g. cTrader); no execution in this service.
RECOMMENDED_ACTION_BY_STATE: dict[str, str] = {
    "KILL": "STOP",
    "REDUCE": "LOWER_VOLUME",
    "BOOST": "INCREASE_VOLUME",
    "MONITOR": "NO_CHANGE",
}

# Safe caps for effective allocation multiplier (local-first, explainable)
BOOST_MULTIPLIER = 1.2
BOOST_MULTIPLIER_CAP = 1.25
REDUCE_MULTIPLIER = 0.5
MONITOR_MULTIPLIER = 1.0
KILL_MULTIPLIER = 0.0


@dataclass(frozen=True)
class ControlResult:
    state: str
    active: bool
    alloc_multiplier: float
    reason: str


def recommended_action_for_state(control_state: str) -> str:
    """Map stored control_state to a safe, stateless action hint for bots."""
    return RECOMMENDED_ACTION_BY_STATE.get(control_state, "NO_CHANGE")


def compute_control(decision: str, risk_level: str, score: float) -> ControlResult:
    """
    Derive control state from existing execution decision + risk + score.

    Rules (deterministic, easy to debug):
    - KILL: execution says PAUSE (e.g. HIGH risk) — bot inactive for control; no effective capital.
    - REDUCE: execution says REDUCE — lower effective allocation multiplier.
    - BOOST: execution says EXECUTE — raise effective allocation within cap (not above BOOST_MULTIPLIER_CAP).
    - MONITOR: execution says MONITOR — nominal multiplier, no change signal.
    """
    if decision == "PAUSE" or risk_level == "HIGH":
        return ControlResult(
            state="KILL",
            active=False,
            alloc_multiplier=KILL_MULTIPLIER,
            reason="HIGH risk or PAUSE: inactive for control purposes",
        )

    if decision == "REDUCE":
        return ControlResult(
            state="REDUCE",
            active=True,
            alloc_multiplier=REDUCE_MULTIPLIER,
            reason="Execution REDUCE: mark for reduced effective allocation",
        )

    if decision == "EXECUTE":
        mult = min(BOOST_MULTIPLIER, BOOST_MULTIPLIER_CAP)
        return ControlResult(
            state="BOOST",
            active=True,
            alloc_multiplier=mult,
            reason="Execution EXECUTE: safe boost within cap",
        )

    # MONITOR (and any other decision treated as watch)
    return ControlResult(
        state="MONITOR",
        active=True,
        alloc_multiplier=MONITOR_MULTIPLIER,
        reason="Execution MONITOR: keep active, no allocation change signal",
    )
