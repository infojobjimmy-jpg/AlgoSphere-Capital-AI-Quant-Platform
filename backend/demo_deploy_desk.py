from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

DEMO_QUEUE = "DEMO_QUEUE"
DEMO_ASSIGNED = "DEMO_ASSIGNED"
DEMO_RUNNING = "DEMO_RUNNING"
DEMO_PAUSED = "DEMO_PAUSED"
DEMO_COMPLETED = "DEMO_COMPLETED"
DEMO_REJECTED = "DEMO_REJECTED"

DEMO_STATES = {
    DEMO_QUEUE,
    DEMO_ASSIGNED,
    DEMO_RUNNING,
    DEMO_PAUSED,
    DEMO_COMPLETED,
    DEMO_REJECTED,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def is_demo_eligible(
    strategy: dict[str, Any],
    promotion_score: float,
) -> tuple[bool, str]:
    review_status = str(strategy.get("review_status", ""))
    lifecycle_status = str(strategy.get("status", ""))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))
    fitness = float(strategy.get("fitness_score", 0.0))

    if review_status == "APPROVED_FOR_DEMO":
        return True, "Approved by Candidate Review Desk."

    strong_live_review = (
        lifecycle_status == "APPROVED_FOR_LIVE_REVIEW"
        and fitness >= 70.0
        and promotion_score >= 65.0
        and risk_profile == "LOW"
    )
    if strong_live_review:
        return True, "Approved-for-live-review with strong metrics and low risk."
    return False, "Not eligible for demo queue yet."


def compute_demo_priority(strategy: dict[str, Any], promotion_score: float) -> float:
    fitness = float(strategy.get("fitness_score", 0.0))
    drawdown = float(strategy.get("expected_drawdown", 0.0))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))
    review_status = str(strategy.get("review_status", ""))

    review_bonus = 18.0 if review_status == "APPROVED_FOR_DEMO" else 10.0
    risk_penalty = {"LOW": 0.0, "MEDIUM": 7.0, "HIGH": 14.0}.get(risk_profile, 8.0)
    score = (fitness * 0.5) + (promotion_score * 0.4) + review_bonus - (drawdown * 0.12) - risk_penalty
    return round(_clamp(score, 0.0, 100.0), 2)


def build_demo_candidates(
    strategies: list[dict[str, Any]],
    paper_bots: list[dict[str, Any]],
    feedback_results: list[dict[str, Any]],
    limit: int = 25,
) -> dict[str, Any]:
    paper_by_id = {str(p.get("strategy_id")): p for p in paper_bots}
    feedback_by_id = {str(f.get("strategy_id")): f for f in feedback_results}
    out: list[dict[str, Any]] = []

    for s in strategies:
        sid = str(s.get("strategy_id"))
        fb = feedback_by_id.get(sid, {})
        promotion_score = float(fb.get("promotion_score", 0.0))
        eligible, reason = is_demo_eligible(s, promotion_score)
        demo_state = str(s.get("demo_status", ""))
        include = eligible or demo_state in DEMO_STATES
        if not include:
            continue
        paper = paper_by_id.get(sid, {})
        priority = compute_demo_priority(s, promotion_score)
        out.append(
            {
                "strategy_id": sid,
                "family": s.get("family"),
                "status": s.get("status"),
                "review_status": s.get("review_status", "PENDING_REVIEW"),
                "fitness_score": float(s.get("fitness_score", 0.0)),
                "risk_profile": s.get("risk_profile", "MEDIUM"),
                "paper_profit": float(paper.get("paper_profit", 0.0)),
                "paper_drawdown": float(paper.get("paper_drawdown", 0.0)),
                "paper_win_rate": float(paper.get("paper_win_rate", 0.0)),
                "paper_trades": int(paper.get("paper_trades", 0)),
                "promotion_score": promotion_score,
                "demo_status": demo_state if demo_state in DEMO_STATES else "",
                "demo_note": str(s.get("demo_note", "")),
                "demo_assignee": str(s.get("demo_assignee", "")),
                "demo_assigned_at": s.get("demo_assigned_at"),
                "demo_priority": priority if float(s.get("demo_priority", 0.0)) <= 0 else float(s.get("demo_priority")),
                "eligible": eligible,
                "eligibility_reason": reason,
            }
        )

    out.sort(
        key=lambda x: (
            float(x.get("demo_priority", 0.0)),
            float(x.get("fitness_score", 0.0)),
            float(x.get("promotion_score", 0.0)),
        ),
        reverse=True,
    )
    selected = out[: max(1, int(limit))]
    return {"count": len(selected), "candidates": selected}


def build_demo_status(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        DEMO_QUEUE: 0,
        DEMO_ASSIGNED: 0,
        DEMO_RUNNING: 0,
        DEMO_PAUSED: 0,
        DEMO_COMPLETED: 0,
        DEMO_REJECTED: 0,
    }
    for c in candidates:
        state = str(c.get("demo_status", ""))
        if state in counts:
            counts[state] += 1

    top = sorted(
        [x for x in candidates if x.get("demo_status") in {DEMO_QUEUE, DEMO_ASSIGNED, DEMO_RUNNING}],
        key=lambda x: float(x.get("demo_priority", 0.0)),
        reverse=True,
    )[:5]
    return {
        "counts": counts,
        "top_priority": top,
        "queue_only": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
