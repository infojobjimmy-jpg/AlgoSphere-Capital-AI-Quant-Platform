from __future__ import annotations

import hashlib
import json
from typing import Any


def strategy_fingerprint(*, guidance: dict[str, Any], goal_ids: tuple[int, ...]) -> str:
    """Stable id for the meta-strategy context applied on a tick."""
    payload = {
        "threshold_deltas": guidance.get("threshold_deltas") or {},
        "severity_lift_delta": guidance.get("severity_lift_delta"),
        "planner_profile": guidance.get("planner_profile"),
        "decision_confidence_boost": guidance.get("decision_confidence_boost"),
        "goals": list(goal_ids),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:28]
