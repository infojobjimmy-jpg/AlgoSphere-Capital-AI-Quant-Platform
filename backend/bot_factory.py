from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

STRATEGY_FAMILIES = (
    "EMA_CROSS",
    "MOMENTUM",
    "MEAN_REVERSION",
    "SESSION_BREAKOUT",
)

STRATEGY_STATES = (
    "GENERATED",
    "TESTING",
    "CANDIDATE",
    "REJECTED",
    "APPROVED_FOR_REVIEW",
    "APPROVED_FOR_PAPER",
    "PAPER_RUNNING",
    "PAPER_REJECTED",
    "PAPER_SUCCESS",
    "EVOLVE_AGAIN",
    "APPROVED_FOR_LIVE_REVIEW",
    "LIVE_SAFE_CANDIDATE",
    "LIVE_SAFE_REJECTED",
    "LIVE_SAFE_READY",
)


def _pick_status(fitness_score: float, expected_drawdown: float) -> str:
    if fitness_score >= 80 and expected_drawdown <= 180:
        return "APPROVED_FOR_REVIEW"
    if fitness_score >= 65:
        return "CANDIDATE"
    if fitness_score >= 45:
        return "TESTING"
    return "REJECTED"


def _pick_risk_profile(expected_drawdown: float) -> str:
    if expected_drawdown >= 320:
        return "HIGH"
    if expected_drawdown >= 180:
        return "MEDIUM"
    return "LOW"


def _build_parameters(rng: random.Random, family: str) -> dict[str, Any]:
    params: dict[str, Any] = {
        "risk_multiplier": round(rng.uniform(0.5, 1.5), 2),
        "tp_sl_ratio": round(rng.uniform(1.1, 3.0), 2),
        "session_filter": rng.choice(["ASIA", "LONDON", "NY", "ALL"]),
        "volatility_filter": rng.choice(["LOW", "MEDIUM", "HIGH"]),
    }
    if family == "EMA_CROSS":
        fast = rng.randint(5, 40)
        slow = rng.randint(fast + 5, fast + 80)
        params["ema_fast"] = fast
        params["ema_slow"] = slow
    elif family == "MOMENTUM":
        params["lookback"] = rng.randint(5, 60)
        params["threshold"] = round(rng.uniform(0.2, 2.0), 2)
    elif family == "MEAN_REVERSION":
        params["zscore_window"] = rng.randint(10, 80)
        params["reversion_band"] = round(rng.uniform(0.8, 3.0), 2)
    elif family == "SESSION_BREAKOUT":
        params["breakout_window"] = rng.randint(5, 30)
        params["confirmation_bars"] = rng.randint(1, 5)
    return params


def _fitness_from_metrics(win_rate: float, drawdown: float, risk_multiplier: float) -> float:
    base = (win_rate * 100) - (drawdown / 10)
    adjusted = base + (10 * (1.2 - abs(1.0 - risk_multiplier)))
    return round(max(0.0, min(100.0, adjusted)), 2)


def generate_candidates(count: int = 12, seed: int | None = None) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    created_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    n = max(1, min(count, 200))
    for _ in range(n):
        family = rng.choice(STRATEGY_FAMILIES)
        parameters = _build_parameters(rng, family)
        expected_drawdown = round(rng.uniform(60, 420), 2)
        expected_win_rate = round(rng.uniform(0.35, 0.75), 3)
        fitness_score = _fitness_from_metrics(
            win_rate=expected_win_rate,
            drawdown=expected_drawdown,
            risk_multiplier=float(parameters["risk_multiplier"]),
        )
        status = _pick_status(fitness_score, expected_drawdown)
        out.append(
            {
                "strategy_id": uuid4().hex,
                "family": family,
                "parameters": parameters,
                "fitness_score": fitness_score,
                "expected_drawdown": expected_drawdown,
                "expected_win_rate": expected_win_rate,
                "risk_profile": _pick_risk_profile(expected_drawdown),
                "status": status,
                "created_at": created_at,
                "parent_strategy_id": None,
                "generation": 0,
                "mutation_note": "seed candidate",
                "origin_type": "GENERATED",
            }
        )
    return out


def rank_candidates(candidates: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    sorted_items = sorted(
        candidates,
        key=lambda c: (
            float(c.get("fitness_score", 0.0)),
            float(c.get("expected_win_rate", 0.0)),
            -float(c.get("expected_drawdown", 9999.0)),
        ),
        reverse=True,
    )
    return sorted_items[: max(1, min(limit, 100))]
