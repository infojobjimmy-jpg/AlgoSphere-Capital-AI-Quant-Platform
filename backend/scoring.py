from .utils import clamp


def compute_bot_score(profit: float, drawdown: float, win_rate: float, trades: int) -> float:
    """
    Compute a stable score in range [0, 100].
    """
    profit_component = clamp(profit / 1000, -1, 2) * 35
    risk_component = clamp(1 - (drawdown / 1000), 0, 1) * 35
    consistency_component = clamp(win_rate, 0, 1) * 20
    activity_component = clamp(trades / 100, 0, 1) * 10
    return round(clamp(profit_component + risk_component + consistency_component + activity_component, 0, 100), 2)
