from __future__ import annotations

from typing import Any

from .feedback_engine import evaluate_paper_feedback


def evaluate_live_safe_candidate(
    strategy: dict[str, Any], paper: dict[str, Any]
) -> dict[str, Any]:
    """
    Review-only live-safe promotion gate.
    No trading or deployment actions are triggered here.
    """
    feedback = evaluate_paper_feedback(paper)
    trades = int(paper.get("paper_trades", 0))
    drawdown = float(paper.get("paper_drawdown", 0.0))
    win_rate = float(paper.get("paper_win_rate", 0.0))
    paper_status = str(paper.get("status", ""))
    risk_profile = str(strategy.get("risk_profile", "MEDIUM"))
    promotion_score = float(feedback.get("promotion_score", 0.0))

    reject = (
        paper_status == "PAPER_REJECTED"
        or drawdown >= 320
        or win_rate < 0.45
        or risk_profile == "HIGH"
    )

    qualifies = (
        paper_status == "PAPER_SUCCESS"
        and trades >= 30
        and drawdown < 220
        and win_rate > 0.55
        and promotion_score >= 60
        and risk_profile != "HIGH"
    )

    if reject:
        target_status = "LIVE_SAFE_REJECTED"
    elif qualifies and promotion_score >= 75:
        target_status = "LIVE_SAFE_READY"
    elif qualifies:
        target_status = "APPROVED_FOR_LIVE_REVIEW"
    else:
        target_status = "LIVE_SAFE_CANDIDATE"

    return {
        "strategy_id": strategy.get("strategy_id"),
        "family": strategy.get("family"),
        "paper_status": paper_status,
        "paper_profit": float(paper.get("paper_profit", 0.0)),
        "paper_drawdown": drawdown,
        "paper_win_rate": win_rate,
        "paper_trades": trades,
        "risk_profile": risk_profile,
        "feedback_score": float(feedback.get("feedback_score", 0.0)),
        "promotion_score": promotion_score,
        "rejection_flag": bool(feedback.get("rejection_flag", False) or reject),
        "target_status": target_status,
        "reasoning": feedback.get("reasoning", ""),
    }
