def get_execution_decision(score: float, risk_level: str) -> str:
    if risk_level == "HIGH":
        return "PAUSE"
    if score >= 70 and risk_level == "LOW":
        return "EXECUTE"
    if score >= 50 and risk_level in {"LOW", "MEDIUM"}:
        return "MONITOR"
    return "REDUCE"
