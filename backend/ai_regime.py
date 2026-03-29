from typing import Sequence


def detect_regime(profits: Sequence[float], avg_win_rate: float) -> tuple[str, str]:
    if not profits:
        return ("NEUTRAL", "No bot activity yet.")

    avg_profit = sum(profits) / len(profits)
    if avg_profit > 100 and avg_win_rate >= 0.55:
        return ("RISK_ON", "Favorable performance environment.")
    if avg_profit < 0 or avg_win_rate < 0.45:
        return ("RISK_OFF", "Defensive regime due to weak metrics.")
    return ("NEUTRAL", "Mixed conditions. Keep positions balanced.")
