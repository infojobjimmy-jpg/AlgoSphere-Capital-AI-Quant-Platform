from __future__ import annotations

from typing import Any


def evaluate_paper_feedback(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert paper trading metrics into feedback signals.
    This is advisory + status update only; no live deployment.
    """
    profit = float(item.get("paper_profit", 0.0))
    drawdown = float(item.get("paper_drawdown", 0.0))
    win_rate = float(item.get("paper_win_rate", 0.0))
    trades = int(item.get("paper_trades", 0))

    profit_component = max(-1.0, min(1.0, profit / 200.0)) * 40.0
    drawdown_component = max(0.0, min(1.0, 1.0 - (drawdown / 400.0))) * 35.0
    win_component = max(0.0, min(1.0, win_rate)) * 25.0
    feedback_score = round(max(0.0, min(100.0, profit_component + drawdown_component + win_component)), 2)

    trade_bonus = max(0.0, min(10.0, trades / 10.0))
    promotion_score = round(max(0.0, min(100.0, feedback_score + trade_bonus)), 2)

    promote = (
        profit > 0
        and drawdown <= 220
        and win_rate > 0.55
        and trades >= 20
    )
    reject = (
        drawdown >= 320
        or win_rate < 0.45
        or (profit < -120 and trades >= 10)
    )

    if promote:
        target_status = "PAPER_SUCCESS"
        action = "PROMOTE"
    elif reject:
        target_status = "PAPER_REJECTED"
        action = "REJECT"
    else:
        target_status = "EVOLVE_AGAIN"
        action = "EVOLVE_AGAIN"

    return {
        "feedback_score": feedback_score,
        "promotion_score": promotion_score,
        "rejection_flag": bool(reject),
        "target_status": target_status,
        "action": action,
        "reasoning": (
            f"profit={profit}, drawdown={drawdown}, win_rate={win_rate}, trades={trades}"
        ),
    }
