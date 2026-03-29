"""
Long Term Memory Engine: persistent learning memory from read-only system signals.
No trading, broker execution, or capital deployment — aggregation and storage only.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .bot_factory import STRATEGY_FAMILIES
from .database import get_alert_engine_state, get_connection, set_alert_engine_state
from .market_regime_engine import ALL_REGIMES

STATE_KEY = "long_term_memory_state"
MAX_STRATEGY_OBS = 48
MAX_REGIME_OBS = 120
MAX_RISK_SNAPS = 60
MAX_EVOLUTION_SNAPS = 40

ST_HEALTH_GOOD = "GOOD"
ST_HEALTH_SEEDING = "SEEDING"
ST_HEALTH_STALE = "STALE"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "last_update": None,
        "update_count": 0,
        "strategy_observations": {},  # strategy_id -> list of observations
        "regime_observations": [],
        "risk_snapshots": [],
        "evolution_snapshots": [],
    }


def load_memory_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, STATE_KEY)
    if not raw:
        return _empty_state()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_state()
    if not isinstance(data, dict):
        return _empty_state()
    if int(data.get("version", 0) or 0) != 1:
        return _empty_state()
    # Ensure keys
    base = _empty_state()
    base.update(data)
    base.setdefault("strategy_observations", {})
    base.setdefault("regime_observations", [])
    base.setdefault("risk_snapshots", [])
    base.setdefault("evolution_snapshots", [])
    return base


def save_memory_state(state: dict[str, Any]) -> None:
    payload = json.dumps(state, separators=(",", ":"))
    with get_connection() as conn:
        set_alert_engine_state(conn, STATE_KEY, payload)


def _trim_list(lst: list[Any], cap: int) -> None:
    while len(lst) > cap:
        lst.pop(0)


def _append_strategy_obs(
    state: dict[str, Any],
    strategy_id: str,
    obs: dict[str, Any],
) -> None:
    bucket = state["strategy_observations"].setdefault(strategy_id, [])
    bucket.append(obs)
    _trim_list(bucket, MAX_STRATEGY_OBS)


def evolution_success_rate_for_strategy(
    strategy_id: str,
    factory_strategies: list[dict[str, Any]],
) -> float:
    sid = str(strategy_id)
    children = [
        s
        for s in factory_strategies
        if str(s.get("parent_strategy_id", "") or "") == sid and int(s.get("generation", 0) or 0) > 0
    ]
    if not children:
        return 0.0
    wins = sum(1 for s in children if _f(s.get("fitness_score"), 0.0) >= 52.0)
    return round(wins / len(children), 4)


def aggregate_strategy_memory(
    state: dict[str, Any],
    factory_strategies: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fam_by_sid = {str(s.get("strategy_id")): str(s.get("family", "")) for s in factory_strategies}
    out: list[dict[str, Any]] = []
    for sid, obs in (state.get("strategy_observations") or {}).items():
        if not obs:
            continue
        scores = [_f(o.get("performance_score"), 0.0) for o in obs]
        stabs = [_f(o.get("success_rate"), 0.0) for o in obs]
        regimes = [str(o.get("regime", "TRANSITIONAL")) for o in obs]
        # best / worst regime by mean score when label seen
        by_reg: dict[str, list[float]] = {}
        for o in obs:
            r = str(o.get("regime", "TRANSITIONAL"))
            by_reg.setdefault(r, []).append(_f(o.get("performance_score"), 0.0))
        reg_means = {r: sum(v) / len(v) for r, v in by_reg.items() if v}
        best_r = max(reg_means, key=reg_means.get) if reg_means else "TRANSITIONAL"
        worst_r = min(reg_means, key=reg_means.get) if reg_means else "TRANSITIONAL"
        evo_sr = evolution_success_rate_for_strategy(sid, factory_strategies)
        out.append(
            {
                "strategy_id": sid,
                "family": fam_by_sid.get(sid, ""),
                "avg_performance": round(sum(scores) / len(scores), 4) if scores else 0.0,
                "stability": round(sum(stabs) / len(stabs), 4) if stabs else 0.0,
                "best_regime": best_r,
                "worst_regime": worst_r,
                "evolution_success_rate": evo_sr,
                "observation_count": len(obs),
            }
        )
    out.sort(key=lambda x: (-x["avg_performance"], x["strategy_id"]))
    return out


def aggregate_family_memory(
    strategy_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_f: dict[str, list[dict[str, Any]]] = {f: [] for f in STRATEGY_FAMILIES}
    for row in strategy_rows:
        fam = str(row.get("family", "") or "")
        if fam in by_f:
            by_f[fam].append(row)
    out: list[dict[str, Any]] = []
    for fam in STRATEGY_FAMILIES:
        rows = by_f[fam]
        if not rows:
            out.append(
                {
                    "family": fam,
                    "avg_performance": 0.0,
                    "stability": 0.0,
                    "best_regime": "TRANSITIONAL",
                    "sample_strategies": 0,
                }
            )
            continue
        ap = sum(_f(r.get("avg_performance"), 0.0) for r in rows) / len(rows)
        st = sum(_f(r.get("stability"), 0.0) for r in rows) / len(rows)
        # most common best_regime among strategies in family
        regimes = [str(r.get("best_regime", "TRANSITIONAL")) for r in rows]
        best_reg = max(set(regimes), key=regimes.count)
        out.append(
            {
                "family": fam,
                "avg_performance": round(ap, 4),
                "stability": round(st, 4),
                "best_regime": best_reg,
                "sample_strategies": len(rows),
            }
        )
    return out


def aggregate_regime_memory(
    state: dict[str, Any],
    strategy_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fam_perf: dict[str, float] = {}
    for row in strategy_rows:
        fam = str(row.get("family", "") or "")
        if fam:
            fam_perf[fam] = _f(row.get("avg_performance"), 0.0)

    obs = state.get("regime_observations") or []
    by_reg: dict[str, list[float]] = {r: [] for r in ALL_REGIMES}
    for o in obs:
        r = str(o.get("regime", ""))
        if r in by_reg:
            by_reg[r].append(_f(o.get("mean_strategy_performance", 0.0)))

    out: list[dict[str, Any]] = []
    for regime in ALL_REGIMES:
        scores = by_reg.get(regime) or []
        avg_p = sum(scores) / len(scores) if scores else 0.0
        # correlate family strength with regime from current snapshot of strategy rows
        ranked = sorted(fam_perf.items(), key=lambda x: -x[1])
        best_f = [a for a, _ in ranked[:2]] if ranked else []
        if len(ranked) >= 2:
            worst_f = [a for a, _ in ranked[-2:]]
        elif ranked:
            worst_f = [ranked[-1][0]]
        else:
            worst_f = []
        out.append(
            {
                "regime": regime,
                "avg_performance": round(avg_p, 4),
                "best_families": best_f,
                "worst_families": worst_f,
                "snapshot_count": len(scores),
            }
        )
    return out


def aggregate_evolution_memory(state: dict[str, Any]) -> dict[str, Any]:
    snaps = state.get("evolution_snapshots") or []
    if not snaps:
        return {
            "total_snapshots": 0,
            "avg_weak_pool": 0.0,
            "avg_strong_pool": 0.0,
            "last_loops_completed": 0,
        }
    last = snaps[-1]
    weak = [_f(s.get("weak_count"), 0.0) for s in snaps]
    strong = [_f(s.get("strong_count"), 0.0) for s in snaps]
    return {
        "total_snapshots": len(snaps),
        "avg_weak_pool": round(sum(weak) / len(weak), 2),
        "avg_strong_pool": round(sum(strong) / len(strong), 2),
        "last_loops_completed": int(last.get("loops_completed", 0) or 0),
    }


def aggregate_risk_memory(state: dict[str, Any]) -> dict[str, Any]:
    snaps = state.get("risk_snapshots") or []
    if not snaps:
        return {"latest_global_risk_score": None, "samples": 0, "avg_global_risk_score": 0.0}
    scores = [_f(s.get("global_risk_score"), 0.0) for s in snaps]
    return {
        "latest_global_risk_score": round(_f(snaps[-1].get("global_risk_score"), 0.0), 4),
        "samples": len(snaps),
        "avg_global_risk_score": round(sum(scores) / len(scores), 4),
    }


def build_learning_insights(
    strategy_rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
    evolution: dict[str, Any],
    risk: dict[str, Any],
) -> list[str]:
    insights: list[str] = []
    if strategy_rows:
        top = strategy_rows[0]
        insights.append(
            f"Top remembered strategy {str(top.get('strategy_id', ''))[:12]}… "
            f"avg_performance={top.get('avg_performance')} stability={top.get('stability')}."
        )
    best_fam = max(family_rows, key=lambda r: _f(r.get("avg_performance"), 0.0), default=None)
    if best_fam and _f(best_fam.get("avg_performance"), 0.0) > 0:
        insights.append(
            f"Family {best_fam.get('family')} shows strongest remembered avg performance vs others."
        )
    best_reg = max(regime_rows, key=lambda r: _f(r.get("avg_performance"), 0.0), default=None)
    if best_reg and _f(best_reg.get("avg_performance"), 0.0) > 0:
        insights.append(
            f"Regime {best_reg.get('regime')} has the highest mean remembered cohort performance."
        )
    if evolution.get("total_snapshots", 0) > 0:
        insights.append(
            f"Evolution memory: avg weak pool ~{evolution.get('avg_weak_pool')} "
            f"strong ~{evolution.get('avg_strong_pool')} (snapshots={evolution.get('total_snapshots')})."
        )
    if risk.get("samples", 0) > 0:
        insights.append(
            f"Risk memory: avg global risk score {risk.get('avg_global_risk_score')} "
            f"over {risk.get('samples')} samples."
        )
    if not insights:
        insights.append("Memory is seeding — run POST /memory/update after the system has activity.")
    return insights[:12]


def count_memory_entries(state: dict[str, Any]) -> int:
    n = 0
    for _sid, obs in (state.get("strategy_observations") or {}).items():
        n += len(obs)
    n += len(state.get("regime_observations") or [])
    n += len(state.get("risk_snapshots") or [])
    n += len(state.get("evolution_snapshots") or [])
    return n


def memory_health(state: dict[str, Any]) -> str:
    lu = state.get("last_update")
    uc = int(state.get("update_count") or 0)
    if not lu or uc < 1:
        return ST_HEALTH_SEEDING
    try:
        raw = str(lu).replace("Z", "+00:00")
        t = datetime.fromisoformat(raw)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - t).total_seconds()
        if age > 7 * 24 * 3600:
            return ST_HEALTH_STALE
    except ValueError:
        return ST_HEALTH_SEEDING
    return ST_HEALTH_GOOD


def run_memory_update_cycle(bundle: dict[str, Any]) -> dict[str, Any]:
    """
    bundle keys:
      regime_status, global_risk_full, strategies_performance (list),
      factory_strategies, evolution_candidates, evolution_lineage,
      continuous_evolution_status, auto_loop_status
    """
    state = load_memory_state()
    now = _iso_now()
    regime = bundle.get("regime_status") or {}
    current_regime = str(regime.get("current_regime", "TRANSITIONAL"))
    grs = _f((bundle.get("global_risk_full") or {}).get("global_risk_score"), 0.0)
    risk_level = str((bundle.get("global_risk_full") or {}).get("risk_level", ""))

    perfs = bundle.get("strategies_performance") or []
    factory = bundle.get("factory_strategies") or []

    mean_sp = 0.0
    if perfs:
        mean_sp = sum(_f(p.get("performance_score"), 0.0) for p in perfs) / len(perfs)

    for row in perfs:
        sid = str(row.get("strategy_id", "") or "")
        if not sid:
            continue
        _append_strategy_obs(
            state,
            sid,
            {
                "at": now,
                "regime": current_regime,
                "performance_score": _f(row.get("performance_score"), 0.0),
                "success_rate": _f(row.get("success_rate"), 0.0),
            },
        )

    state["regime_observations"].append(
        {
            "at": now,
            "regime": current_regime,
            "confidence": _f(regime.get("confidence_score"), 0.0),
            "global_risk_score": grs,
            "mean_strategy_performance": round(mean_sp, 4),
            "favored_families": list(regime.get("favored_strategy_families") or []),
            "reduced_families": list(regime.get("reduced_strategy_families") or []),
        }
    )
    _trim_list(state["regime_observations"], MAX_REGIME_OBS)

    comps = (bundle.get("global_risk_full") or {}).get("components") or {}
    state["risk_snapshots"].append(
        {
            "at": now,
            "global_risk_score": grs,
            "risk_level": risk_level,
            "components": {k: round(_f(v, 0.0), 4) for k, v in comps.items()},
        }
    )
    _trim_list(state["risk_snapshots"], MAX_RISK_SNAPS)

    ev_c = bundle.get("evolution_candidates") or {}
    weak = ev_c.get("weak_strategies") or []
    strong = ev_c.get("strong_strategies") or []
    lineage = (bundle.get("evolution_lineage") or {}).get("lineage") or []
    cel = bundle.get("continuous_evolution_status") or {}
    auto = bundle.get("auto_loop_status") or {}
    state["evolution_snapshots"].append(
        {
            "at": now,
            "weak_count": len(weak),
            "strong_count": len(strong),
            "lineage_nodes": len(lineage),
            "loops_completed": int(cel.get("loops_completed", 0) or auto.get("loops_completed", 0) or 0),
        }
    )
    _trim_list(state["evolution_snapshots"], MAX_EVOLUTION_SNAPS)

    state["last_update"] = now
    state["update_count"] = int(state.get("update_count") or 0) + 1
    save_memory_state(state)

    return {
        "ok": True,
        "last_update": now,
        "memory_entries": count_memory_entries(state),
        "memory_health": memory_health(state),
        "learning_memory_only": True,
        "decision_layer_only": True,
    }


def build_memory_status_payload(
    state: dict[str, Any],
    factory_strategies: list[dict[str, Any]],
) -> dict[str, Any]:
    strat = aggregate_strategy_memory(state, factory_strategies)
    fam = aggregate_family_memory(strat)
    reg = aggregate_regime_memory(state, strat)
    evo = aggregate_evolution_memory(state)
    risk = aggregate_risk_memory(state)
    insights = build_learning_insights(strat, fam, reg, evo, risk)
    return {
        "memory_entries": count_memory_entries(state),
        "last_update": state.get("last_update"),
        "memory_health": memory_health(state),
        "learning_insights": insights,
        "evolution_memory": evo,
        "risk_memory_summary": risk,
        "update_count": int(state.get("update_count") or 0),
        "decision_layer_only": True,
        "learning_memory_only": True,
    }


def memory_hint_for_regime_engine(state: dict[str, Any]) -> str | None:
    """Short advisory string for regime payload enrichment (read-only)."""
    strat = aggregate_strategy_memory(state, [])
    if not strat:
        return None
    top = strat[0]
    return (
        f"Memory: strategy {str(top.get('strategy_id', ''))[:10]}… "
        f"best under {top.get('best_regime')} (avg_perf={top.get('avg_performance')})."
    )


def memory_hints_for_meta(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "memory_health": memory_health(state),
        "memory_entries": count_memory_entries(state),
        "last_update": state.get("last_update"),
    }
