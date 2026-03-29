from __future__ import annotations

import os
from datetime import datetime, timezone
from random import Random
from typing import Any


MAX_PAPER_BOTS = 5


def select_paper_candidates(
    strategies: list[dict[str, Any]],
    running_strategy_ids: set[str],
    max_bots: int = MAX_PAPER_BOTS,
) -> list[dict[str, Any]]:
    """
    Select top candidates for paper mode only.
    """
    eligible = [
        s
        for s in strategies
        if s.get("status") in {"APPROVED_FOR_REVIEW", "CANDIDATE", "APPROVED_FOR_PAPER"}
        and s.get("strategy_id") not in running_strategy_ids
    ]
    slots = max(0, max_bots - len(running_strategy_ids))
    return eligible[:slots]


def simulate_paper_metrics(strategy_id: str, fitness_score: float) -> dict[str, Any]:
    """
    Deterministic simulation keyed by strategy id.
    Fixed-risk simulation only; no capital sizing or live actions.
    """
    seed = int(strategy_id[:8], 16)
    rng = Random(seed)

    fit = max(0.0, min(100.0, float(fitness_score)))
    win_rate = round(max(0.2, min(0.85, 0.35 + (fit / 200.0) + rng.uniform(-0.08, 0.08))), 3)
    trades = int(max(5, min(120, 20 + fit + rng.randint(-10, 10))))
    drawdown = round(max(20.0, min(500.0, 260 - (fit * 1.6) + rng.uniform(-30, 30))), 2)
    profit = round(((win_rate - 0.5) * 2.0 * trades * 3.0) - (drawdown * 0.08), 2)

    status = "PAPER_RUNNING"
    if trades >= 20 and win_rate >= 0.58 and drawdown <= 180 and profit > 0:
        status = "PAPER_SUCCESS"
    elif drawdown >= 320 or win_rate <= 0.4 or profit < -120:
        status = "PAPER_REJECTED"

    now = datetime.now(timezone.utc).isoformat()
    note = "paper simulation only; no live trading"
    if os.environ.get("ALGO_SPHERE_LIVE_TESTING", "").strip().lower() in ("1", "true", "yes", "on"):
        note += "; live_testing_mode uses incoming candles for research — execution remains paper-only"
    return {
        "status": status,
        "paper_profit": profit,
        "paper_drawdown": drawdown,
        "paper_win_rate": win_rate,
        "paper_trades": trades,
        "last_updated": now,
        "sim_note": note,
    }
