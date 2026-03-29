from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

EXECUTOR_PENDING = "EXECUTOR_PENDING"
EXECUTOR_READY = "EXECUTOR_READY"
EXECUTOR_RUNNING = "EXECUTOR_RUNNING"
EXECUTOR_PAUSED = "EXECUTOR_PAUSED"
EXECUTOR_STOPPED = "EXECUTOR_STOPPED"
EXECUTOR_REJECTED = "EXECUTOR_REJECTED"

EXECUTOR_STATES = {
    EXECUTOR_PENDING,
    EXECUTOR_READY,
    EXECUTOR_RUNNING,
    EXECUTOR_PAUSED,
    EXECUTOR_STOPPED,
    EXECUTOR_REJECTED,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def is_executor_eligible(strategy: dict[str, Any]) -> tuple[bool, str]:
    demo_status = str(strategy.get("demo_status", ""))
    review_status = str(strategy.get("review_status", ""))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))
    live_status = str(strategy.get("status", ""))
    if demo_status != "DEMO_ASSIGNED":
        return False, "Demo item is not assigned yet."
    if review_status not in {"APPROVED_FOR_DEMO", "UNDER_REVIEW"}:
        return False, "Review status is not acceptable for executor preparation."
    if risk_profile == "HIGH":
        return False, "High-risk profile is blocked for executor preparation."
    if live_status == "LIVE_SAFE_REJECTED":
        return False, "Live-safe rejected candidates are blocked."
    return True, "Eligible for executor preparation."


def compute_executor_priority(strategy: dict[str, Any]) -> float:
    fitness = float(strategy.get("fitness_score", 0.0))
    promotion = float(strategy.get("promotion_score", 0.0))
    drawdown = float(strategy.get("expected_drawdown", 0.0))
    demo_priority = float(strategy.get("demo_priority", 0.0))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))
    risk_penalty = {"LOW": 0.0, "MEDIUM": 5.0, "HIGH": 15.0}.get(risk_profile, 7.0)
    score = (demo_priority * 0.35) + (fitness * 0.35) + (promotion * 0.3) - (drawdown * 0.12) - risk_penalty
    return round(_clamp(score, 0.0, 100.0), 2)


def build_executor_candidates(
    strategies: list[dict[str, Any]],
    feedback_results: list[dict[str, Any]],
    limit: int = 25,
) -> dict[str, Any]:
    fb_by_id = {str(x.get("strategy_id")): x for x in feedback_results}
    out: list[dict[str, Any]] = []
    for s in strategies:
        sid = str(s.get("strategy_id"))
        fb = fb_by_id.get(sid, {})
        s["promotion_score"] = float(fb.get("promotion_score", 0.0))
        eligible, reason = is_executor_eligible(s)
        status = str(s.get("executor_status", ""))
        include = eligible or status in EXECUTOR_STATES
        if not include:
            continue
        out.append(
            {
                "strategy_id": sid,
                "family": s.get("family"),
                "status": s.get("status"),
                "review_status": s.get("review_status", "PENDING_REVIEW"),
                "demo_status": s.get("demo_status", ""),
                "demo_assignee": s.get("demo_assignee", ""),
                "risk_profile": s.get("risk_profile", "MEDIUM"),
                "fitness_score": float(s.get("fitness_score", 0.0)),
                "promotion_score": float(s.get("promotion_score", 0.0)),
                "executor_status": status if status in EXECUTOR_STATES else "",
                "executor_note": str(s.get("executor_note", "")),
                "executor_target": str(s.get("executor_target", "")),
                "executor_assigned_at": s.get("executor_assigned_at"),
                "executor_priority": float(s.get("executor_priority", 0.0))
                if float(s.get("executor_priority", 0.0)) > 0
                else compute_executor_priority(s),
                "eligible": eligible,
                "eligibility_reason": reason,
            }
        )
    out.sort(
        key=lambda x: (
            float(x.get("executor_priority", 0.0)),
            float(x.get("fitness_score", 0.0)),
        ),
        reverse=True,
    )
    selected = out[: max(1, int(limit))]
    return {"count": len(selected), "candidates": selected}


def build_executor_status(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        EXECUTOR_PENDING: 0,
        EXECUTOR_READY: 0,
        EXECUTOR_RUNNING: 0,
        EXECUTOR_PAUSED: 0,
        EXECUTOR_STOPPED: 0,
        EXECUTOR_REJECTED: 0,
    }
    for c in candidates:
        st = str(c.get("executor_status", ""))
        if st in counts:
            counts[st] += 1
    active = [
        c
        for c in candidates
        if c.get("executor_status") in {EXECUTOR_READY, EXECUTOR_RUNNING, EXECUTOR_PAUSED}
    ]
    active.sort(key=lambda x: float(x.get("executor_priority", 0.0)), reverse=True)
    return {
        "counts": counts,
        "prepared_or_running": active[:10],
        "adapter_only": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
