from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

RUNNER_PENDING = "RUNNER_PENDING"
RUNNER_ACKNOWLEDGED = "RUNNER_ACKNOWLEDGED"
RUNNER_ACTIVE = "RUNNER_ACTIVE"
RUNNER_PAUSED = "RUNNER_PAUSED"
RUNNER_COMPLETED = "RUNNER_COMPLETED"
RUNNER_FAILED = "RUNNER_FAILED"

RUNNER_STATES = {
    RUNNER_PENDING,
    RUNNER_ACKNOWLEDGED,
    RUNNER_ACTIVE,
    RUNNER_PAUSED,
    RUNNER_COMPLETED,
    RUNNER_FAILED,
}


def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def is_runner_eligible(strategy: dict[str, Any]) -> tuple[bool, str]:
    if str(strategy.get("executor_status", "")) != "EXECUTOR_READY":
        return False, "Executor item is not ready."
    if str(strategy.get("demo_status", "")) not in {"DEMO_ASSIGNED", "DEMO_RUNNING", "DEMO_PAUSED"}:
        return False, "Demo state is not acceptable."
    if str(strategy.get("review_status", "")) not in {"APPROVED_FOR_DEMO", "UNDER_REVIEW"}:
        return False, "Review state is not acceptable."
    if str(strategy.get("risk_profile", "MEDIUM")) == "HIGH":
        return False, "High-risk profile is blocked."
    if str(strategy.get("status", "")) in {"LIVE_SAFE_REJECTED", "PAPER_REJECTED"}:
        return False, "Rejected lifecycle state is blocked."
    return True, "Eligible for runner job."


def compute_runner_priority(strategy: dict[str, Any]) -> float:
    executor_priority = float(strategy.get("executor_priority", 0.0))
    demo_priority = float(strategy.get("demo_priority", 0.0))
    fitness = float(strategy.get("fitness_score", 0.0))
    promotion = float(strategy.get("promotion_score", 0.0))
    score = (executor_priority * 0.45) + (demo_priority * 0.2) + (fitness * 0.2) + (promotion * 0.15)
    return round(_clamp(score, 0.0, 100.0), 2)


def build_runner_jobs(
    strategies: list[dict[str, Any]],
    feedback_results: list[dict[str, Any]],
    limit: int = 25,
) -> dict[str, Any]:
    fb_by_id = {str(x.get("strategy_id")): x for x in feedback_results}
    jobs: list[dict[str, Any]] = []
    for s in strategies:
        sid = str(s.get("strategy_id"))
        promotion = float(fb_by_id.get(sid, {}).get("promotion_score", 0.0))
        s["promotion_score"] = promotion
        eligible, reason = is_runner_eligible(s)
        state = str(s.get("runner_status", ""))
        if not eligible and state not in RUNNER_STATES:
            continue
        priority = float(s.get("runner_priority", 0.0))
        if priority <= 0:
            priority = compute_runner_priority(s)
        jobs.append(
            {
                "strategy_id": sid,
                "family": s.get("family"),
                "executor_status": s.get("executor_status", ""),
                "demo_status": s.get("demo_status", ""),
                "review_status": s.get("review_status", ""),
                "runner_status": state if state in RUNNER_STATES else "",
                "runner_note": str(s.get("runner_note", "")),
                "runner_id": str(s.get("runner_id", "")),
                "runner_started_at": s.get("runner_started_at"),
                "runner_completed_at": s.get("runner_completed_at"),
                "runner_priority": priority,
                "executor_target": str(s.get("executor_target", "")),
                "eligible": eligible,
                "eligibility_reason": reason,
            }
        )
    jobs.sort(key=lambda x: float(x.get("runner_priority", 0.0)), reverse=True)
    selected = jobs[: max(1, int(limit))]
    return {"count": len(selected), "jobs": selected}


def build_runner_status_payload(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        RUNNER_PENDING: 0,
        RUNNER_ACKNOWLEDGED: 0,
        RUNNER_ACTIVE: 0,
        RUNNER_PAUSED: 0,
        RUNNER_COMPLETED: 0,
        RUNNER_FAILED: 0,
    }
    for j in jobs:
        st = str(j.get("runner_status", ""))
        if st in counts:
            counts[st] += 1
    active = [j for j in jobs if j.get("runner_status") in {RUNNER_ACKNOWLEDGED, RUNNER_ACTIVE}]
    active.sort(key=lambda x: float(x.get("runner_priority", 0.0)), reverse=True)
    return {
        "counts": counts,
        "current_jobs": active[:10],
        "summary": {
            "total_jobs": len(jobs),
            "active_or_acknowledged": len(active),
            "completed": counts[RUNNER_COMPLETED],
            "failed": counts[RUNNER_FAILED],
        },
        "bridge_only": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
