def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
