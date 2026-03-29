"""
Simulated fund allocation across top-performing factory strategies.
Analytics / decision support only: no broker, no real capital, no trade execution.
"""

from __future__ import annotations

from typing import Any

DEFAULT_TOTAL_CAPITAL = 100_000.0
PERFORMANCE_SCORE_FLOOR = 0.70
MAX_ALLOCATION_PCT = 0.10
MIN_ALLOCATION_PCT = 0.02
# Keep a slice of simulated capital unallocated (dry powder).
MAX_TOTAL_DEPLOY_PCT = 0.90


def _stability(row: dict[str, Any]) -> float:
    tr = max(1, int(row.get("total_runs", 0) or 0))
    fc = int(row.get("fail_count", 0) or 0)
    return max(0.0, 1.0 - fc / tr)


def _composite(row: dict[str, Any]) -> float:
    return float(row.get("performance_score", 0.0) or 0.0) * _stability(row)


def _paper_metrics(
    strategy_id: str,
    paper_by_id: dict[str, dict[str, Any]],
) -> tuple[float, float]:
    p = paper_by_id.get(strategy_id) or {}
    wr = float(p.get("paper_win_rate", 0.0) or 0.0)
    dd = float(p.get("paper_drawdown", 0.0) or 0.0)
    return wr, dd


def select_eligible_strategies(perf_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = [r for r in perf_rows if float(r.get("performance_score", 0.0) or 0.0) > PERFORMANCE_SCORE_FLOOR]
    out.sort(key=lambda r: _composite(r), reverse=True)
    return out


def compute_allocation_percents(eligible: list[dict[str, Any]]) -> list[tuple[dict[str, Any], float]]:
    """
    Returns (row, allocation_percent) where percents are fractions of total capital (0-1).
    """
    if not eligible:
        return []
    selected: list[dict[str, Any]] = []
    for r in eligible:
        if (len(selected) + 1) * MIN_ALLOCATION_PCT <= MAX_TOTAL_DEPLOY_PCT + 1e-12:
            selected.append(r)
        else:
            break
    if not selected:
        return []
    n = len(selected)
    base = MIN_ALLOCATION_PCT
    surplus = MAX_TOTAL_DEPLOY_PCT - n * base
    comps = [_composite(r) for r in selected]
    total_c = sum(comps) or 1.0
    extras = [surplus * (c / total_c) for c in comps]
    pcts: list[float] = []
    for i in range(n):
        p = min(MAX_ALLOCATION_PCT, base + extras[i])
        pcts.append(p)
    s = sum(pcts)
    if s > MAX_TOTAL_DEPLOY_PCT and s > 0:
        scale = MAX_TOTAL_DEPLOY_PCT / s
        pcts = [p * scale for p in pcts]
    return [(selected[i], pcts[i]) for i in range(n)]


def _herfindahl(pcts: list[float]) -> float:
    return sum(p * p for p in pcts)


def build_fund_simulation(
    *,
    perf_rows: list[dict[str, Any]],
    paper_items: list[dict[str, Any]],
    total_capital: float = DEFAULT_TOTAL_CAPITAL,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """
    Returns (status_dict, portfolio_strategies, meta).
    """
    paper_by_id = {str(p.get("strategy_id")): p for p in paper_items}
    eligible = select_eligible_strategies(perf_rows)
    pairs = compute_allocation_percents(eligible)
    pcts = [p for _, p in pairs]

    allocated_capital = round(sum(total_capital * p for _, p in pairs), 2)
    free_capital = round(total_capital - allocated_capital, 2)

    # Toy portfolio_return: weighted paper win rate scaled to a small % return number
    port_ret = 0.0
    dd_weighted = 0.0
    strategies_out: list[dict[str, Any]] = []
    if pairs:
        for row, pct in pairs:
            sid = str(row["strategy_id"])
            wr, dd = _paper_metrics(sid, paper_by_id)
            strategies_out.append(
                {
                    "strategy_id": sid,
                    "allocation_percent": round(pct, 6),
                    "allocation_amount": round(total_capital * pct, 2),
                    "performance_score": float(row.get("performance_score", 0.0) or 0.0),
                }
            )
            port_ret += pct * wr
            dd_weighted += pct * dd
        port_ret = round(port_ret * 0.01, 6)
        dd_weighted = round(min(1.0, dd_weighted / 10_000.0), 6)

    risk_score = round(min(1.0, _herfindahl(pcts)), 6) if pcts else 0.0

    status = {
        "total_capital": float(total_capital),
        "allocated_capital": allocated_capital,
        "free_capital": free_capital,
        "portfolio_return": port_ret,
        "risk_score": risk_score,
        "drawdown": dd_weighted,
    }
    meta = {
        "eligible_count": len(eligible),
        "allocated_slots": len(pairs),
        "max_deploy_pct": MAX_TOTAL_DEPLOY_PCT,
    }
    return status, strategies_out, meta
