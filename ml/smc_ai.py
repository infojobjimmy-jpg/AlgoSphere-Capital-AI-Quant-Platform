from typing import Sequence


def detect_market_bias(price_changes: Sequence[float]) -> str:
    if not price_changes:
        return "neutral"
    total_move = sum(price_changes)
    if total_move > 0:
        return "bullish"
    if total_move < 0:
        return "bearish"
    return "neutral"
