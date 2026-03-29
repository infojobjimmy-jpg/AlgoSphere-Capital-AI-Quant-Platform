from __future__ import annotations

import copy
import random
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .bot_factory import rank_candidates


def _bounded(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _mutate_param(name: str, value: Any, rng: random.Random) -> Any:
    if isinstance(value, (int, float)):
        if "risk_multiplier" in name:
            return round(_bounded(float(value) + rng.uniform(-0.15, 0.15), 0.5, 1.5), 2)
        if "tp_sl_ratio" in name:
            return round(_bounded(float(value) + rng.uniform(-0.25, 0.25), 1.1, 3.0), 2)
        if "threshold" in name:
            return round(_bounded(float(value) + rng.uniform(-0.2, 0.2), 0.2, 2.0), 2)
        if "reversion_band" in name:
            return round(_bounded(float(value) + rng.uniform(-0.3, 0.3), 0.8, 3.0), 2)
        if isinstance(value, int):
            step = rng.choice([-2, -1, 1, 2])
            return max(1, int(value) + step)
        return round(float(value) + rng.uniform(-0.1, 0.1), 3)
    if isinstance(value, str):
        if "session_filter" in name:
            return rng.choice(["ASIA", "LONDON", "NY", "ALL"])
        if "volatility_filter" in name:
            return rng.choice(["LOW", "MEDIUM", "HIGH"])
    return value


def _score_candidate(c: dict[str, Any]) -> None:
    wr = float(c.get("expected_win_rate", 0.5))
    dd = float(c.get("expected_drawdown", 250))
    rm = float(c.get("parameters", {}).get("risk_multiplier", 1.0))
    base = (wr * 100) - (dd / 10)
    adjusted = base + (10 * (1.2 - abs(1.0 - rm)))
    fitness = round(max(0.0, min(100.0, adjusted)), 2)
    c["fitness_score"] = fitness
    if dd >= 320:
        c["risk_profile"] = "HIGH"
    elif dd >= 180:
        c["risk_profile"] = "MEDIUM"
    else:
        c["risk_profile"] = "LOW"
    if fitness >= 80 and dd <= 180:
        c["status"] = "APPROVED_FOR_REVIEW"
    elif fitness >= 65:
        c["status"] = "CANDIDATE"
    elif fitness >= 45:
        c["status"] = "TESTING"
    else:
        c["status"] = "REJECTED"


def evolve_candidates(
    existing: list[dict[str, Any]],
    top_n: int = 5,
    children_per_parent: int = 2,
    crossover_rate: float = 0.3,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """
    Create mutated/crossed strategy children from top performers.
    Candidates only; no deployment.
    """
    if not existing:
        return []
    rng = random.Random(seed)
    parents = rank_candidates(existing, limit=max(1, top_n))
    now = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []

    for p in parents:
        p_params = p.get("parameters", {})
        p_gen = int(p.get("generation", 0))
        for _ in range(max(1, children_per_parent)):
            child = copy.deepcopy(p)
            child["strategy_id"] = uuid4().hex
            child["parent_strategy_id"] = p["strategy_id"]
            child["generation"] = p_gen + 1
            child["origin_type"] = "MUTATED"
            child["created_at"] = now

            params = copy.deepcopy(p_params)
            keys = list(params.keys())
            if keys:
                mutate_keys = rng.sample(keys, k=max(1, min(3, len(keys))))
                notes: list[str] = []
                for k in mutate_keys:
                    old = params[k]
                    params[k] = _mutate_param(k, old, rng)
                    if params[k] != old:
                        notes.append(k)
                child["mutation_note"] = (
                    f"mutated: {', '.join(notes)}" if notes else "mutated: none"
                )
            else:
                child["mutation_note"] = "mutated: no parameters"
            child["parameters"] = params

            # Keep expected metrics near parent with bounded noise
            child["expected_win_rate"] = round(
                _bounded(
                    float(p.get("expected_win_rate", 0.5)) + rng.uniform(-0.03, 0.03),
                    0.25,
                    0.9,
                ),
                3,
            )
            child["expected_drawdown"] = round(
                _bounded(
                    float(p.get("expected_drawdown", 200)) + rng.uniform(-35, 35),
                    40,
                    500,
                ),
                2,
            )
            _score_candidate(child)
            out.append(child)

        # Optional crossover with another top parent
        if len(parents) > 1 and rng.random() < _bounded(crossover_rate, 0.0, 1.0):
            mate = rng.choice([x for x in parents if x["strategy_id"] != p["strategy_id"]])
            cross = copy.deepcopy(p)
            cross["strategy_id"] = uuid4().hex
            cross["parent_strategy_id"] = p["strategy_id"]
            cross["generation"] = p_gen + 1
            cross["origin_type"] = "CROSSED"
            cross["created_at"] = now
            cross_params = copy.deepcopy(p_params)
            mate_params = mate.get("parameters", {})
            swapped: list[str] = []
            for k in cross_params.keys():
                if k in mate_params and rng.random() < 0.5:
                    cross_params[k] = mate_params[k]
                    swapped.append(k)
            cross["parameters"] = cross_params
            cross["expected_win_rate"] = round(
                _bounded(
                    (float(p.get("expected_win_rate", 0.5)) + float(mate.get("expected_win_rate", 0.5))) / 2
                    + rng.uniform(-0.02, 0.02),
                    0.25,
                    0.9,
                ),
                3,
            )
            cross["expected_drawdown"] = round(
                _bounded(
                    (float(p.get("expected_drawdown", 200)) + float(mate.get("expected_drawdown", 200))) / 2
                    + rng.uniform(-30, 30),
                    40,
                    500,
                ),
                2,
            )
            cross["mutation_note"] = (
                f"crossed with {mate['strategy_id'][:8]}: {', '.join(swapped) or 'no swap'}"
            )
            _score_candidate(cross)
            out.append(cross)

    return out
