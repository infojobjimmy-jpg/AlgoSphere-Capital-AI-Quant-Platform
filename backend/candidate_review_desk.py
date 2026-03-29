from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REVIEW_PENDING = "PENDING_REVIEW"
REVIEW_UNDER = "UNDER_REVIEW"
REVIEW_APPROVED = "APPROVED_FOR_DEMO"
REVIEW_REJECTED = "REJECTED_BY_REVIEW"
REVIEW_NEEDS_TESTING = "NEEDS_MORE_TESTING"

REVIEW_STATUSES = {
    REVIEW_PENDING,
    REVIEW_UNDER,
    REVIEW_APPROVED,
    REVIEW_REJECTED,
    REVIEW_NEEDS_TESTING,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def compute_review_priority(
    strategy: dict[str, Any], paper: dict[str, Any] | None, feedback: dict[str, Any] | None
) -> float:
    status = str(strategy.get("status", ""))
    fitness = float(strategy.get("fitness_score", 0.0))
    drawdown = float(strategy.get("expected_drawdown", 0.0))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))

    # Prioritize live-safe and review-facing states first.
    state_bonus = 0.0
    if status == "LIVE_SAFE_CANDIDATE":
        state_bonus = 45.0
    elif status == "APPROVED_FOR_LIVE_REVIEW":
        state_bonus = 40.0
    elif status == "LIVE_SAFE_READY":
        state_bonus = 35.0
    elif status == "PAPER_SUCCESS":
        state_bonus = 25.0

    promotion = 0.0
    if feedback is not None:
        promotion = float(feedback.get("promotion_score", 0.0))
    elif paper is not None:
        # Fallback approximation from paper metrics when feedback preview is missing.
        promotion = _clamp(
            float(paper.get("paper_win_rate", 0.0)) * 100.0
            - float(paper.get("paper_drawdown", 0.0)) * 0.5
            + float(paper.get("paper_profit", 0.0)) * 0.05,
            0.0,
            100.0,
        )

    risk_penalty = {"LOW": 0.0, "MEDIUM": 7.0, "HIGH": 14.0}.get(risk_profile, 8.0)
    score = state_bonus + (fitness * 0.35) + (promotion * 0.45) - (drawdown * 0.15) - risk_penalty
    return round(_clamp(score, 0.0, 100.0), 2)


def build_review_candidates(
    strategies: list[dict[str, Any]],
    paper_bots: list[dict[str, Any]],
    feedback_results: list[dict[str, Any]],
    limit: int = 25,
) -> dict[str, Any]:
    paper_by_id = {str(p.get("strategy_id")): p for p in paper_bots}
    feedback_by_id = {str(f.get("strategy_id")): f for f in feedback_results}

    rows: list[dict[str, Any]] = []
    for s in strategies:
        sid = str(s.get("strategy_id"))
        paper = paper_by_id.get(sid)
        feedback = feedback_by_id.get(sid)
        priority = compute_review_priority(s, paper, feedback)

        review_status = str(s.get("review_status", REVIEW_PENDING))
        if review_status not in REVIEW_STATUSES:
            review_status = REVIEW_PENDING

        rows.append(
            {
                "strategy_id": sid,
                "family": s.get("family"),
                "status": s.get("status"),
                "fitness_score": float(s.get("fitness_score", 0.0)),
                "risk_profile": s.get("risk_profile"),
                "paper_profit": float((paper or {}).get("paper_profit", 0.0)),
                "paper_drawdown": float((paper or {}).get("paper_drawdown", 0.0)),
                "paper_win_rate": float((paper or {}).get("paper_win_rate", 0.0)),
                "paper_trades": int((paper or {}).get("paper_trades", 0)),
                "promotion_score": float((feedback or {}).get("promotion_score", 0.0)),
                "review_status": review_status,
                "review_note": str(s.get("review_note", "")),
                "reviewer": str(s.get("reviewer", "")),
                "reviewed_at": s.get("reviewed_at"),
                "review_priority": priority,
            }
        )

    rows.sort(
        key=lambda x: (
            float(x.get("review_priority", 0.0)),
            float(x.get("fitness_score", 0.0)),
            float(x.get("promotion_score", 0.0)),
        ),
        reverse=True,
    )
    selected = rows[: max(1, int(limit))]
    return {"count": len(selected), "candidates": selected}


def build_review_status_payload(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        REVIEW_PENDING: 0,
        REVIEW_UNDER: 0,
        REVIEW_APPROVED: 0,
        REVIEW_REJECTED: 0,
        REVIEW_NEEDS_TESTING: 0,
    }
    for c in candidates:
        state = str(c.get("review_status", REVIEW_PENDING))
        if state in counts:
            counts[state] += 1

    top_priority = sorted(
        candidates,
        key=lambda x: float(x.get("review_priority", 0.0)),
        reverse=True,
    )[:5]
    return {
        "counts": counts,
        "top_priority_candidates": top_priority,
        "review_only": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
