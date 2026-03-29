"""
Portfolio-level FUND MODE evaluation.

This module stays read-only/action-signal only: no trade execution.
"""

from typing import Any


PORTFOLIO_ACTION_BY_STATE: dict[str, str] = {
    "NORMAL": "KEEP_RUNNING",
    "CAUTION": "TIGHTEN_RISK",
    "DEFENSIVE": "REDUCE_ALL",
    "LOCKDOWN": "STOP_NEW_ENTRIES",
}


def _bot_brief(bot: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": bot.get("name"),
        "score": float(bot.get("score") or 0.0),
        "control_state": bot.get("control_state", "MONITOR"),
        "effective_capital": float(bot.get("effective_capital") or 0.0),
    }


def evaluate_fund_status(bots: list[dict[str, Any]]) -> dict[str, Any]:
    bot_count = len(bots)
    total_profit = round(sum(float(b.get("profit") or 0.0) for b in bots), 2)
    total_effective_capital = round(
        sum(float(b.get("effective_capital") or 0.0) for b in bots), 2
    )
    active_bot_count = sum(1 for b in bots if bool(b.get("control_active", True)))

    kill_bot_count = sum(1 for b in bots if b.get("control_state") == "KILL")
    reduce_bot_count = sum(1 for b in bots if b.get("control_state") == "REDUCE")
    monitor_bot_count = sum(1 for b in bots if b.get("control_state") == "MONITOR")
    boost_bot_count = sum(1 for b in bots if b.get("control_state") == "BOOST")

    average_score = (
        round(sum(float(b.get("score") or 0.0) for b in bots) / bot_count, 2)
        if bot_count
        else 0.0
    )

    sorted_bots = sorted(bots, key=lambda b: float(b.get("score") or 0.0), reverse=True)
    top_bots = [_bot_brief(b) for b in sorted_bots[:3]]
    worst_bots = [_bot_brief(b) for b in sorted_bots[-3:]][::-1]

    # Safe and explainable portfolio rules
    if bot_count == 0:
        portfolio_state = "NORMAL"
        reasoning = "No bots yet; keep system in normal monitoring mode."
    elif active_bot_count == 0 or kill_bot_count >= max(2, (bot_count + 1) // 2):
        portfolio_state = "LOCKDOWN"
        reasoning = "Too many KILL states or no active bots; stop new entries at portfolio level."
    elif reduce_bot_count >= max(2, (bot_count + 1) // 2) or average_score < 40:
        portfolio_state = "DEFENSIVE"
        reasoning = "Many REDUCE states or low average score; reduce aggregate risk."
    elif kill_bot_count >= 1 or reduce_bot_count >= 1 or average_score < 55:
        portfolio_state = "CAUTION"
        reasoning = "Mild deterioration detected; tighten risk and monitor closely."
    else:
        portfolio_state = "NORMAL"
        reasoning = "Portfolio signals are healthy and balanced."

    return {
        "portfolio_state": portfolio_state,
        "recommended_portfolio_action": PORTFOLIO_ACTION_BY_STATE[portfolio_state],
        "summary": {
            "total_profit": total_profit,
            "total_effective_capital": total_effective_capital,
            "bot_count": bot_count,
            "active_bot_count": active_bot_count,
            "average_score": average_score,
        },
        "state_counts": {
            "kill_bot_count": kill_bot_count,
            "reduce_bot_count": reduce_bot_count,
            "monitor_bot_count": monitor_bot_count,
            "boost_bot_count": boost_bot_count,
        },
        "top_bots": top_bots,
        "worst_bots": worst_bots,
        "reasoning": reasoning,
    }
