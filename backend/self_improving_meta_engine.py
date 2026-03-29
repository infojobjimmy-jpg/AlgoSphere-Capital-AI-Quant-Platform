"""
Self-Improving Meta AI engine.
Persistent orchestration learning only; no trading, broker execution, or capital deployment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .database import get_alert_engine_state, get_connection, set_alert_engine_state

STATE_KEY = "meta_ai_learning_state"
MAX_HISTORY = 1000


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "last_update": None,
        "meta_history": [],
    }


def load_learning_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, STATE_KEY)
    if not raw:
        return _empty_state()
    try:
        state = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_state()
    if not isinstance(state, dict):
        return _empty_state()
    out = _empty_state()
    out.update(state)
    if not isinstance(out.get("meta_history"), list):
        out["meta_history"] = []
    return out


def save_learning_state(state: dict[str, Any]) -> None:
    payload = json.dumps(state, separators=(",", ":"))
    with get_connection() as conn:
        set_alert_engine_state(conn, STATE_KEY, payload)


def compute_outcome_score(
    *,
    prev_entry: dict[str, Any] | None,
    performance_score: float,
    risk_score: float,
    drawdown: float,
) -> float:
    """
    Positive when performance improves and risk/drawdown ease.
    Deterministic and bounded to [-1, 1].
    """
    perf_n = _clamp(performance_score, 0.0, 1.0)
    risk_n = _clamp(risk_score, 0.0, 1.0)
    dd_n = _clamp(drawdown, 0.0, 1.0)
    base = 0.55 * perf_n + 0.25 * (1.0 - risk_n) + 0.20 * (1.0 - dd_n)
    if prev_entry is None:
        return round(_clamp((base - 0.5) * 1.4, -1.0, 1.0), 4)
    p0 = _clamp(_f(prev_entry.get("performance_score")), 0.0, 1.0)
    r0 = _clamp(_f(prev_entry.get("risk_score")), 0.0, 1.0)
    d0 = _clamp(_f(prev_entry.get("drawdown")), 0.0, 1.0)
    delta = 0.60 * (perf_n - p0) + 0.25 * (r0 - risk_n) + 0.15 * (d0 - dd_n)
    return round(_clamp(delta, -1.0, 1.0), 4)


def build_learning_entry(
    *,
    control_status: dict[str, Any],
    global_risk_status: dict[str, Any],
    regime_status: dict[str, Any],
    memory_status: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    fund_status: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    prev_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    posture = str(control_status.get("system_posture", "BALANCED"))
    confidence = _f(control_status.get("confidence"), 0.0)
    risk_level = str(global_risk_status.get("risk_level", "MODERATE"))
    risk_score = _f(global_risk_status.get("global_risk_score"), 0.5)
    regime = str(regime_status.get("current_regime", "TRANSITIONAL"))
    memory_health = str(memory_status.get("memory_health", "SEEDING"))
    performance_score = _clamp(
        0.6 * _f(performance_system.get("runner_success_rate"), 0.0)
        + 0.4 * _f(performance_system.get("pipeline_throughput"), 0.0),
        0.0,
        1.0,
    )
    fs = multi_runner_status.get("fleet_summary") or {}
    rc = max(1, int(fs.get("runner_count", 0) or 0))
    bad = int(fs.get("degraded_count", 0) or 0) + int(fs.get("offline_count", 0) or 0)
    runner_health = round(_clamp(1.0 - (bad / float(rc)), 0.0, 1.0), 4)
    portfolio_state = str(fund_status.get("portfolio_state", "NORMAL"))
    drawdown = _clamp(_f(fund_allocation_status.get("drawdown"), 0.0), 0.0, 1.0)
    outcome_score = compute_outcome_score(
        prev_entry=prev_entry,
        performance_score=performance_score,
        risk_score=risk_score,
        drawdown=drawdown,
    )
    return {
        "timestamp": _now_iso(),
        "posture": posture,
        "confidence": round(confidence, 4),
        "risk_level": risk_level,
        "risk_score": round(risk_score, 4),
        "regime": regime,
        "memory_health": memory_health,
        "performance_score": round(performance_score, 4),
        "runner_health": runner_health,
        "portfolio_state": portfolio_state,
        "drawdown": round(drawdown, 4),
        "outcome_score": outcome_score,
    }


def update_learning_state(state: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    hist = state.setdefault("meta_history", [])
    hist.append(entry)
    if len(hist) > MAX_HISTORY:
        del hist[: len(hist) - MAX_HISTORY]
    state["last_update"] = entry.get("timestamp")
    return state


def _posture_avg_outcomes(history: list[dict[str, Any]]) -> dict[str, float]:
    sums: dict[str, float] = {}
    ns: dict[str, int] = {}
    for row in history:
        p = str(row.get("posture", ""))
        if not p:
            continue
        sums[p] = sums.get(p, 0.0) + _f(row.get("outcome_score"), 0.0)
        ns[p] = ns.get(p, 0) + 1
    return {k: round(sums[k] / ns[k], 4) for k in sums if ns.get(k, 0) > 0}


def build_learning_status_payload(state: dict[str, Any]) -> dict[str, Any]:
    hist = state.get("meta_history") or []
    n = len(hist)
    health = "SEEDING" if n < 10 else ("LEARNING" if n < 100 else "MATURE")
    avgs = _posture_avg_outcomes(hist)
    best = sorted(
        [{"posture": k, "avg_outcome_score": v} for k, v in avgs.items()],
        key=lambda x: -float(x["avg_outcome_score"]),
    )[:5]
    return {
        "learning_entries": n,
        "learning_health": health,
        "best_postures": best,
        "last_update": state.get("last_update"),
        "decision_layer_only": True,
        "self_learning_only": True,
    }


def build_learning_insights_payload(state: dict[str, Any]) -> dict[str, Any]:
    hist = state.get("meta_history") or []
    top_patterns: list[str] = []
    best_posture_by_regime: dict[str, str] = {}
    risk_posture_patterns: dict[str, str] = {}
    confidence_adjustments: dict[str, float] = {}

    if hist:
        # regime -> posture avg outcome
        rp: dict[tuple[str, str], list[float]] = {}
        rl: dict[tuple[str, str], list[float]] = {}
        conf_by_posture: dict[str, list[float]] = {}
        for row in hist:
            reg = str(row.get("regime", "TRANSITIONAL"))
            pos = str(row.get("posture", "BALANCED"))
            out = _f(row.get("outcome_score"), 0.0)
            risk = str(row.get("risk_level", "MODERATE"))
            rp.setdefault((reg, pos), []).append(out)
            rl.setdefault((risk, pos), []).append(out)
            conf_by_posture.setdefault(pos, []).append(_f(row.get("confidence"), 0.0))
        regs = sorted({k[0] for k in rp.keys()})
        for reg in regs:
            rows = []
            for (r, p), vals in rp.items():
                if r == reg and vals:
                    rows.append((p, sum(vals) / len(vals)))
            if rows:
                rows.sort(key=lambda x: -x[1])
                best_posture_by_regime[reg] = rows[0][0]
        risks = sorted({k[0] for k in rl.keys()})
        for risk in risks:
            rows = []
            for (r, p), vals in rl.items():
                if r == risk and vals:
                    rows.append((p, sum(vals) / len(vals)))
            if rows:
                rows.sort(key=lambda x: -x[1])
                risk_posture_patterns[risk] = rows[0][0]

        posture_avgs = _posture_avg_outcomes(hist)
        if posture_avgs:
            best_p = max(posture_avgs.keys(), key=lambda k: posture_avgs[k])
            top_patterns.append(
                f"Best average outcome posture so far: {best_p} ({posture_avgs[best_p]:+.3f})."
            )
        if best_posture_by_regime:
            sample_reg = sorted(best_posture_by_regime.keys())[0]
            top_patterns.append(
                f"Regime pattern: {sample_reg} tends to favor {best_posture_by_regime[sample_reg]}."
            )
        for p, vals in conf_by_posture.items():
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            confidence_adjustments[p] = round(_clamp((avg - 0.5) * 0.2, -0.08, 0.08), 4)
    if not top_patterns:
        top_patterns.append("Learning state is seeding; run more autonomous cycles to build patterns.")

    return {
        "top_patterns": top_patterns[:8],
        "best_posture_by_regime": best_posture_by_regime,
        "risk_posture_patterns": risk_posture_patterns,
        "confidence_adjustments": confidence_adjustments,
        "decision_layer_only": True,
        "self_learning_only": True,
    }


def run_learning_update_cycle(
    *,
    control_status: dict[str, Any],
    global_risk_status: dict[str, Any],
    regime_status: dict[str, Any],
    memory_status: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    fund_status: dict[str, Any],
    fund_allocation_status: dict[str, Any],
) -> dict[str, Any]:
    state = load_learning_state()
    hist = state.get("meta_history") or []
    prev = hist[-1] if hist else None
    entry = build_learning_entry(
        control_status=control_status,
        global_risk_status=global_risk_status,
        regime_status=regime_status,
        memory_status=memory_status,
        performance_system=performance_system,
        multi_runner_status=multi_runner_status,
        fund_status=fund_status,
        fund_allocation_status=fund_allocation_status,
        prev_entry=prev,
    )
    state = update_learning_state(state, entry)
    save_learning_state(state)
    status = build_learning_status_payload(state)
    return {
        "ok": True,
        "entry": entry,
        "learning_entries": status["learning_entries"],
        "learning_health": status["learning_health"],
        "last_update": status["last_update"],
        "decision_layer_only": True,
        "self_learning_only": True,
    }
