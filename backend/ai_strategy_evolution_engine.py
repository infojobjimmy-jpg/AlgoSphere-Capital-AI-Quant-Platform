"""
AI Strategy Evolution Engine: performance-driven, deterministic variant generation.
Simulation / factory records only — no broker, no execution, no capital deployment.
Does not UPDATE parent strategies; only INSERTs new rows via insert_factory_strategies.
"""

from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

# Classification thresholds (aligned with spec)
WEAK_PERFORMANCE_MAX = 0.45
WEAK_MIN_TOTAL_RUNS = 3
STRONG_PERFORMANCE_MIN = 0.70
STRONG_SUCCESS_RATE_MIN = 0.60

MAX_PARENT_GENERATION = 12
MUTATION_NOTE_PREFIX = "mutation_type"

TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"]
SESSION_ORDER = ["ASIA", "LONDON", "NY", "ALL"]
VOL_ORDER = ["LOW", "MEDIUM", "HIGH"]


def _sign(parent_id: str, salt: str, seed: int) -> int:
    h = hashlib.sha256(f"{parent_id}:{salt}:{seed}".encode()).hexdigest()
    return 1 if int(h[:8], 16) % 2 == 0 else -1


def _perturb_int(value: int, sign: int, pct: float = 0.10) -> int:
    return max(1, int(round(float(value) * (1.0 + sign * pct))))


def _perturb_float(value: float, sign: int, low: float, high: float, pct: float = 0.10) -> float:
    nv = float(value) * (1.0 + sign * pct)
    return round(max(low, min(high, nv)), 3)


def _next_in_cycle(items: list[str], current: str | None, steps: int) -> str:
    if not items:
        return ""
    try:
        idx = items.index(str(current)) if current in items else 0
    except ValueError:
        idx = 0
    return items[(idx + steps) % len(items)]


def is_weak_strategy(perf: dict[str, Any]) -> bool:
    ps = float(perf.get("performance_score", 0.0) or 0.0)
    tr = int(perf.get("total_runs", 0) or 0)
    return ps < WEAK_PERFORMANCE_MAX and tr >= WEAK_MIN_TOTAL_RUNS


def is_strong_strategy(perf: dict[str, Any]) -> bool:
    ps = float(perf.get("performance_score", 0.0) or 0.0)
    sr = float(perf.get("success_rate", 0.0) or 0.0)
    return ps > STRONG_PERFORMANCE_MIN and sr > STRONG_SUCCESS_RATE_MIN


def _perf_by_id(perf_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(r["strategy_id"]): r for r in perf_rows if r.get("strategy_id")}


def build_evolution_candidates_payload(
    factory_strategies: list[dict[str, Any]],
    perf_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    perf_map = _perf_by_id(perf_rows)
    weak: list[dict[str, Any]] = []
    strong: list[dict[str, Any]] = []
    for s in factory_strategies:
        sid = str(s.get("strategy_id", ""))
        if not sid:
            continue
        p = perf_map.get(sid)
        if p is None:
            continue
        base = {
            "strategy_id": sid,
            "family": s.get("family"),
            "performance_score": p.get("performance_score"),
            "total_runs": p.get("total_runs"),
            "success_rate": p.get("success_rate"),
            "generation": int(s.get("generation", 0) or 0),
        }
        if is_weak_strategy(p):
            weak.append(
                {
                    **base,
                    "classify_reason": f"performance_score<{WEAK_PERFORMANCE_MAX} and total_runs>={WEAK_MIN_TOTAL_RUNS}",
                }
            )
        if is_strong_strategy(p):
            strong.append(
                {
                    **base,
                    "classify_reason": f"performance_score>{STRONG_PERFORMANCE_MIN} and success_rate>{STRONG_SUCCESS_RATE_MIN}",
                }
            )
    weak.sort(key=lambda x: (float(x.get("performance_score", 0.0)), x["strategy_id"]))
    strong.sort(key=lambda x: (-float(x.get("performance_score", 0.0)), x["strategy_id"]))
    return {
        "weak_strategies": weak,
        "strong_strategies": strong,
        "evolution_only": True,
        "demo_simulation_only": True,
        "thresholds": {
            "weak_max_performance_score": WEAK_PERFORMANCE_MAX,
            "weak_min_total_runs": WEAK_MIN_TOTAL_RUNS,
            "strong_min_performance_score": STRONG_PERFORMANCE_MIN,
            "strong_min_success_rate": STRONG_SUCCESS_RATE_MIN,
        },
    }


def _format_mutation_note(mutation_type: str, details: list[str]) -> str:
    body = "; ".join(details) if details else mutation_type
    return f"{MUTATION_NOTE_PREFIX}={mutation_type}|{body}"


def extract_mutation_type(mutation_note: str) -> str:
    note = str(mutation_note or "")
    if f"{MUTATION_NOTE_PREFIX}=" in note:
        part = note.split("|", 1)[0]
        return part.split("=", 1)[-1].strip() or "UNKNOWN"
    return note[:64] if note else "UNKNOWN"


def _fresh_pipeline_defaults() -> dict[str, Any]:
    return {
        "review_status": "PENDING_REVIEW",
        "review_note": "",
        "reviewer": "",
        "reviewed_at": None,
        "review_priority": 0.0,
        "demo_status": "",
        "demo_note": "",
        "demo_assignee": "",
        "demo_assigned_at": None,
        "demo_priority": 0.0,
        "executor_status": "",
        "executor_note": "",
        "executor_target": "",
        "executor_assigned_at": None,
        "executor_priority": 0.0,
        "runner_status": "",
        "runner_note": "",
        "runner_id": "",
        "runner_started_at": None,
        "runner_completed_at": None,
        "runner_priority": 0.0,
    }


def evolve_weak_variant(parent: dict[str, Any], *, seed: int) -> tuple[dict[str, Any], list[str]]:
    """
    Deterministic weak-strategy recovery: EMA/ATR-like/risk/timeframe/entry filters.
    Returns (new_strategy_dict, explain_lines).
    """
    pid = str(parent["strategy_id"])
    child = copy.deepcopy(parent)
    params = copy.deepcopy(parent.get("parameters") or {})
    details: list[str] = []
    fam = str(parent.get("family", ""))

    if "ema_fast" in params:
        sgn = _sign(pid, "ema_fast", seed)
        old = int(params["ema_fast"])
        params["ema_fast"] = _perturb_int(old, sgn, 0.10)
        details.append(f"ema_fast {old}->{params['ema_fast']} ({sgn:+d} 10%)")
    if "ema_slow" in params:
        sgn = _sign(pid, "ema_slow", seed)
        old = int(params["ema_slow"])
        params["ema_slow"] = _perturb_int(old, sgn, 0.10)
        if int(params["ema_slow"]) <= int(params.get("ema_fast", 1)):
            params["ema_slow"] = int(params.get("ema_fast", 1)) + 5
        details.append(f"ema_slow adjusted->{params['ema_slow']}")

    if "tp_sl_ratio" in params:
        sgn = _sign(pid, "tp_sl", seed)
        old = float(params["tp_sl_ratio"])
        params["tp_sl_ratio"] = _perturb_float(old, sgn, 1.05, 3.5, 0.10)
        details.append(f"tp_sl_ratio(ATR-like) {old}->{params['tp_sl_ratio']}")

    if "risk_multiplier" in params:
        sgn = _sign(pid, "risk", seed)
        old = float(params["risk_multiplier"])
        params["risk_multiplier"] = _perturb_float(old, sgn, 0.5, 1.5, 0.10)
        details.append(f"risk_multiplier {old}->{params['risk_multiplier']}")

    if "session_filter" in params:
        step = 1 if _sign(pid, "sess", seed) > 0 else -1
        old = str(params["session_filter"])
        params["session_filter"] = _next_in_cycle(SESSION_ORDER, old, step)
        details.append(f"session_filter(entry) {old}->{params['session_filter']}")

    if "volatility_filter" in params:
        step = 1 if _sign(pid, "vol", seed) > 0 else -1
        old = str(params["volatility_filter"])
        params["volatility_filter"] = _next_in_cycle(VOL_ORDER, old, step)
        details.append(f"volatility_filter(entry) {old}->{params['volatility_filter']}")

    tf_step = 1 if _sign(pid, "tf", seed) > 0 else -1
    prev_tf = params.get("timeframe")
    params["timeframe"] = _next_in_cycle(TIMEFRAMES, str(prev_tf) if prev_tf else None, tf_step)
    details.append(f"timeframe {prev_tf}->{params['timeframe']}")

    if fam == "MOMENTUM" and "lookback" in params:
        sgn = _sign(pid, "lb", seed)
        old = int(params["lookback"])
        params["lookback"] = _perturb_int(old, sgn, 0.10)
        details.append(f"lookback {old}->{params['lookback']}")
    if fam == "MEAN_REVERSION" and "zscore_window" in params:
        sgn = _sign(pid, "zw", seed)
        old = int(params["zscore_window"])
        params["zscore_window"] = _perturb_int(old, sgn, 0.10)
        details.append(f"zscore_window {old}->{params['zscore_window']}")

    now = datetime.now(timezone.utc).isoformat()
    child["strategy_id"] = uuid4().hex
    child["parent_strategy_id"] = pid
    child["generation"] = int(parent.get("generation", 0) or 0) + 1
    child["parameters"] = params
    child["mutation_note"] = _format_mutation_note("WEAK_PARAM_TIME_FILTER_VARIANT", details)
    child["origin_type"] = "AI_EVOLVED_WEAK"
    child["created_at"] = now
    child["status"] = "TESTING"
    # Expected metrics: nudge toward exploration (bounded)
    child["expected_win_rate"] = round(
        max(0.25, min(0.9, float(parent.get("expected_win_rate", 0.5)) + 0.02 * _sign(pid, "ewr", seed))),
        3,
    )
    child["expected_drawdown"] = round(
        max(40.0, min(450.0, float(parent.get("expected_drawdown", 200.0)) - 10.0 * _sign(pid, "edd", seed))),
        2,
    )
    ff = float(parent.get("fitness_score", 50.0) or 50.0)
    child["fitness_score"] = round(max(0.0, min(100.0, ff - 3.0)), 2)
    child.update(_fresh_pipeline_defaults())
    return child, details


def evolve_strong_variant(parent: dict[str, Any], *, seed: int) -> tuple[dict[str, Any], list[str]]:
    """Small deterministic clone / fine-tune for strong strategies."""
    pid = str(parent["strategy_id"])
    child = copy.deepcopy(parent)
    params = copy.deepcopy(parent.get("parameters") or {})
    numeric_keys = [k for k, v in params.items() if isinstance(v, (int, float))]
    details: list[str] = []
    if numeric_keys:
        numeric_keys.sort()
        pick_idx = int(hashlib.sha256(f"{pid}:pick:{seed}".encode()).hexdigest(), 16) % len(
            numeric_keys
        )
        key = numeric_keys[pick_idx]
        val = params[key]
        sgn = _sign(pid, f"fine:{key}", seed)
        if isinstance(val, int):
            old = int(val)
            params[key] = max(1, int(round(old * (1.0 + sgn * 0.05))))
            details.append(f"{key} {old}->{params[key]} (±5%)")
        else:
            old = float(val)
            params[key] = round(old * (1.0 + sgn * 0.05), 3)
            details.append(f"{key} {old}->{params[key]} (±5%)")
    else:
        details.append("no_numeric_params: structural_clone_only")

    now = datetime.now(timezone.utc).isoformat()
    child["strategy_id"] = uuid4().hex
    child["parent_strategy_id"] = pid
    child["generation"] = int(parent.get("generation", 0) or 0) + 1
    child["parameters"] = params
    child["mutation_note"] = _format_mutation_note("STRONG_CLONE_OPTIMIZED_VARIANT", details)
    child["origin_type"] = "AI_EVOLVED_STRONG"
    child["created_at"] = now
    child["status"] = "CANDIDATE"
    child["expected_win_rate"] = round(
        max(0.25, min(0.9, float(parent.get("expected_win_rate", 0.5)) + 0.01 * _sign(pid, "swr", seed))),
        3,
    )
    child["expected_drawdown"] = round(
        max(40.0, min(450.0, float(parent.get("expected_drawdown", 200.0)) + 5.0 * _sign(pid, "sdd", seed))),
        2,
    )
    ff = float(parent.get("fitness_score", 50.0) or 50.0)
    child["fitness_score"] = round(max(0.0, min(100.0, ff + 1.0)), 2)
    child.update(_fresh_pipeline_defaults())
    return child, details


def run_evolution_batch(
    factory_strategies: list[dict[str, Any]],
    perf_rows: list[dict[str, Any]],
    *,
    seed: int = 42,
    max_weak: int = 5,
    max_strong: int = 5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build new strategy rows (insert-only). Parents are never mutated here.
    """
    by_id = {str(s["strategy_id"]): s for s in factory_strategies if s.get("strategy_id")}
    payload = build_evolution_candidates_payload(factory_strategies, perf_rows)
    weak_list = payload["weak_strategies"][: max(0, int(max_weak))]
    strong_list = payload["strong_strategies"][: max(0, int(max_strong))]

    created: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    def try_add(entry: dict[str, Any], *, strong: bool) -> None:
        sid = entry["strategy_id"]
        parent = by_id.get(sid)
        if parent is None:
            skipped.append({"strategy_id": sid, "reason": "parent_strategy_not_found"})
            return
        gen = int(parent.get("generation", 0) or 0)
        if gen >= MAX_PARENT_GENERATION:
            skipped.append(
                {
                    "strategy_id": sid,
                    "reason": f"parent_generation_cap>={MAX_PARENT_GENERATION}",
                }
            )
            return
        if strong:
            child, _ = evolve_strong_variant(parent, seed=seed)
        else:
            child, _ = evolve_weak_variant(parent, seed=seed)
        created.append(child)

    for w in weak_list:
        try_add(w, strong=False)
    for s in strong_list:
        try_add(s, strong=True)

    return created, skipped


def variant_summary_row(s: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": s.get("strategy_id"),
        "parent_strategy_id": s.get("parent_strategy_id"),
        "generation": int(s.get("generation", 0) or 0),
        "mutation_type": extract_mutation_type(str(s.get("mutation_note", ""))),
        "family": s.get("family"),
        "origin_type": s.get("origin_type"),
        "created_at": s.get("created_at"),
    }


def build_lineage_payload(factory_strategies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for s in factory_strategies:
        pid = s.get("parent_strategy_id")
        if not pid:
            continue
        edges.append(
            {
                "parent": str(pid),
                "child": str(s.get("strategy_id", "")),
                "generation": int(s.get("generation", 0) or 0),
                "mutation": extract_mutation_type(str(s.get("mutation_note", ""))),
                "created_at": s.get("created_at"),
            }
        )
    edges.sort(key=lambda e: (e["generation"], e["parent"], e["child"]))
    return edges
