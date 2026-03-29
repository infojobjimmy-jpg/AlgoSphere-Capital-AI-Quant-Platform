def evaluate_risk(drawdown: float, win_rate: float) -> str:
    if drawdown > 500 or win_rate < 0.4:
        return "HIGH"
    if drawdown > 250 or win_rate < 0.5:
        return "MEDIUM"
    return "LOW"
