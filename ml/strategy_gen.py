from typing import Sequence


def generate_strategy_tag(score: float, recent_scores: Sequence[float]) -> str:
    if not recent_scores:
        return "neutral"

    avg_score = sum(recent_scores) / len(recent_scores)
    if score > avg_score + 0.1:
        return "momentum"
    if score < avg_score - 0.1:
        return "defensive"
    return "neutral"
