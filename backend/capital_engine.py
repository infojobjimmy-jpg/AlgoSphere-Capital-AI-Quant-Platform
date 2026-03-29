from __future__ import annotations

from typing import Any


def build_capital_status(
    account: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    fund_status: dict[str, Any],
) -> dict[str, Any]:
    """
    Simulation-only capital status.
    No real money execution; decision layer output only.
    """
    total_capital = float(account.get("balance", 0.0))
    alloc_percent = float(portfolio_allocation.get("total_allocated_percent", 0.0))
    alloc_percent = max(0.0, min(100.0, alloc_percent))
    allocated_capital = round(total_capital * (alloc_percent / 100.0), 2)
    free_capital = round(max(0.0, total_capital - allocated_capital), 2)
    risk_usage = round(allocated_capital / total_capital, 4) if total_capital > 0 else 0.0

    total_profit = float(fund_status.get("summary", {}).get("total_profit", 0.0))
    growth_rate = round((total_profit / total_capital) if total_capital > 0 else 0.0, 4)

    return {
        "total_capital": round(total_capital, 2),
        "allocated": allocated_capital,
        "free": free_capital,
        "risk_usage": risk_usage,
        "growth_rate": growth_rate,
    }
