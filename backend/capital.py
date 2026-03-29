from .utils import safe_div


def allocate_capital(total_balance: float, bot_scores: dict[int, float], risk_limit: float) -> dict[int, float]:
    """
    Allocate a bounded portion of account balance to bots based on score.
    """
    active_capital = total_balance * (1 - risk_limit)
    total_score = sum(max(score, 0) for score in bot_scores.values())

    allocations: dict[int, float] = {}
    for bot_id, score in bot_scores.items():
        weight = safe_div(max(score, 0), total_score)
        allocations[bot_id] = round(active_capital * weight, 2)
    return allocations
