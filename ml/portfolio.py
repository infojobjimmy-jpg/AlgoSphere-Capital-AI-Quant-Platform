def compute_portfolio_weights(scores: dict[str, float]) -> dict[str, float]:
    """
    Convert strategy scores into normalized long-only weights.
    """
    positive = {name: max(value, 0.0) for name, value in scores.items()}
    total = sum(positive.values())
    if total == 0:
        return {name: 0.0 for name in scores}
    return {name: round(value / total, 6) for name, value in positive.items()}
