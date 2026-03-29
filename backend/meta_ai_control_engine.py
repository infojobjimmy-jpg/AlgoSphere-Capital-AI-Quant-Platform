"""
Meta AI Control Engine: top-level orchestration over read-only engine outputs.
Decision layer only — no trading, broker execution, or capital deployment.
"""

from __future__ import annotations

from typing import Any

POSTURE_AGGRESSIVE = "AGGRESSIVE"
POSTURE_BALANCED = "BALANCED"
POSTURE_DEFENSIVE = "DEFENSIVE"
POSTURE_CAPITAL_PRESERVATION = "CAPITAL_PRESERVATION"
POSTURE_CHAOTIC_SAFE_MODE = "CHAOTIC_SAFE_MODE"

def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def classify_system_posture(
    *,
    global_risk_full: dict[str, Any],
    regime_status: dict[str, Any],
    fund_status: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    learning_insights: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    """Return (posture, reasoning_lines)."""
    grs = _f(global_risk_full.get("global_risk_score"), 0.5)
    rl = str(global_risk_full.get("risk_level", "MODERATE") or "MODERATE")
    reg = str(regime_status.get("current_regime", "TRANSITIONAL") or "TRANSITIONAL")
    portfolio_state = str(fund_status.get("portfolio_state", "NORMAL") or "NORMAL")
    fund_dd = _f(fund_allocation_status.get("drawdown"), 0.0)

    reasoning: list[str] = []

    if rl == "CRITICAL" or grs >= 0.75:
        reasoning.append("Global risk is CRITICAL or score is very high — freeze aggressive tilts.")
        return POSTURE_CHAOTIC_SAFE_MODE, reasoning

    if reg == "CHAOTIC" and grs >= 0.52:
        reasoning.append("Chaotic regime with elevated composite risk — use chaotic-safe posture.")
        return POSTURE_CHAOTIC_SAFE_MODE, reasoning

    if portfolio_state in {"DEFENSIVE", "LOCKDOWN"} or fund_dd >= 0.58:
        reasoning.append("Fund simulation is defensive or drawdown pressure is high — preserve capital.")
        return POSTURE_CAPITAL_PRESERVATION, reasoning

    if rl == "HIGH" or (reg == "CHAOTIC" and grs >= 0.50):
        reasoning.append("Risk is HIGH or chaotic regime persists — favor defensive coordination.")
        return POSTURE_DEFENSIVE, reasoning

    if reg == "VOLATILE" and grs >= 0.45:
        reasoning.append("Volatile regime with meaningful global risk — reduce concentration risk.")
        return POSTURE_DEFENSIVE, reasoning

    if rl == "LOW" and reg == "TRENDING" and grs < 0.38:
        reasoning.append("Low risk and trending regime proxy — room for aggressive research posture.")
        return POSTURE_AGGRESSIVE, reasoning

    if rl in {"LOW", "MODERATE"} and grs < 0.48:
        reasoning.append("Risk is contained — balanced exploration across families.")
        posture = POSTURE_BALANCED
    else:
        reasoning.append("Mixed signals — default to balanced orchestration.")
        posture = POSTURE_BALANCED

    # Self-improving overlay: only applies when core safety guards are not triggered above.
    if learning_insights:
        reg = str(regime_status.get("current_regime", "TRANSITIONAL"))
        by_reg = learning_insights.get("best_posture_by_regime") or {}
        hint = str(by_reg.get(reg, "") or "")
        if hint in {
            POSTURE_AGGRESSIVE,
            POSTURE_BALANCED,
            POSTURE_DEFENSIVE,
            POSTURE_CAPITAL_PRESERVATION,
            POSTURE_CHAOTIC_SAFE_MODE,
        } and hint != posture:
            # Keep self-learning conservative: allow only one-step nudges unless risk is LOW.
            if rl == "LOW" or {posture, hint} <= {POSTURE_BALANCED, POSTURE_DEFENSIVE}:
                reasoning.append(f"Learning insight: in {reg}, {hint} historically performs better.")
                posture = hint
    return posture, reasoning


def compute_control_confidence(
    *,
    global_risk_full: dict[str, Any],
    memory_payload: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    learning_insights: dict[str, Any] | None = None,
) -> tuple[float, list[str]]:
    """Return confidence 0–1 and notes used in reasoning."""
    notes: list[str] = []
    comps = global_risk_full.get("components") or {}
    data_n = sum(1 for k in ("concentration_risk", "correlation_risk", "drawdown_risk", "runner_risk", "pipeline_risk", "capital_risk") if k in comps)
    data_score = _clamp(data_n / 6.0, 0.0, 1.0)
    if data_n >= 5:
        notes.append("Risk decomposition is well populated.")
    else:
        notes.append("Partial risk components — confidence discounted.")

    mem_entries = int(memory_payload.get("memory_entries") or 0)
    mem_score = _clamp(mem_entries / 400.0, 0.0, 1.0)
    if mem_entries < 50:
        notes.append("Long-term memory is still seeding.")

    r_fail = _f(performance_system.get("runner_fail_rate"), 0.0)
    pipe = _f(performance_system.get("pipeline_throughput"), 0.5)
    n_jobs = int(performance_system.get("total_jobs") or performance_system.get("total_runner_jobs") or 0)
    perf_score = _clamp(0.55 * (1.0 - r_fail) + 0.45 * pipe, 0.0, 1.0)
    if n_jobs < 5:
        perf_score *= 0.65
        notes.append("Limited runner history — performance confidence reduced.")

    fs = multi_runner_status.get("fleet_summary") or {}
    rc = max(1, int(fs.get("runner_count", 0) or 0))
    bad = int(fs.get("degraded_count", 0) or 0) + int(fs.get("offline_count", 0) or 0)
    runner_score = _clamp(1.0 - (bad / float(rc)), 0.0, 1.0)
    if rc <= 1:
        runner_score = max(0.35, runner_score * 0.8)
        notes.append("Runner fleet is minimal — fleet health confidence capped.")

    conf = 0.22 * data_score + 0.22 * mem_score + 0.28 * perf_score + 0.28 * runner_score
    if learning_insights:
        adj = learning_insights.get("confidence_adjustments") or {}
        if isinstance(adj, dict):
            # Gentle global confidence nudge from learned confidence calibration.
            if adj:
                avg_adj = sum(_f(v, 0.0) for v in adj.values()) / max(1, len(adj))
                conf += _clamp(avg_adj, -0.05, 0.05)
                notes.append("Confidence calibrated with self-learning history.")
    return round(_clamp(conf, 0.12, 0.97), 4), notes


def build_control_recommendations(
    posture: str,
    regime_status: dict[str, Any],
    global_risk_full: dict[str, Any],
    legacy_meta: dict[str, Any],
    learning_insights: dict[str, Any] | None = None,
) -> list[str]:
    out: list[str] = []
    reg = str(regime_status.get("current_regime", ""))
    fav = regime_status.get("favored_strategy_families") or []
    red = regime_status.get("reduced_strategy_families") or []

    if posture == POSTURE_AGGRESSIVE:
        out.append("Increase diversification across favored families while monitoring correlation.")
        out.append("Allow faster evolution cadence with paper-only validation gates.")
    elif posture == POSTURE_BALANCED:
        out.append("Maintain balanced family weights; avoid heavy single-family concentration.")
        out.append("Keep steady evolution rate and review promotion quality frequently.")
    elif posture == POSTURE_DEFENSIVE:
        out.append("Reduce momentum-style tilts when regime and risk jointly stress the book.")
        out.append("Favor mean reversion or session breakout only where memory supports stability.")
        out.append("Slow evolution rate until runner and paper success rates improve.")
    elif posture == POSTURE_CAPITAL_PRESERVATION:
        out.append("Prioritize capital defense: trim simulated allocation headroom and widen review gates.")
        out.append("Increase paper testing coverage before promoting candidates.")
    else:  # CHAOTIC_SAFE
        out.append("Chaotic-safe mode: pause aggressive families and reduce all style risk.")
        out.append("Increase paper testing; defer promotion until global risk mean-reverts.")

    if fav:
        out.append(f"Regime overlay: favor {', '.join(str(x) for x in fav[:4])}.")
    if red:
        out.append(f"Regime overlay: reduce {', '.join(str(x) for x in red[:4])}.")

    grs = _f(global_risk_full.get("global_risk_score"), 0.0)
    if grs >= 0.55:
        out.append("Increase diversification because composite global risk is elevated.")

    # Blend a few legacy meta hints (already advisory strings)
    leg = legacy_meta.get("recommendations") or []
    for line in leg[:3]:
        s = str(line).strip()
        if s and s not in out:
            out.append(s)
    if learning_insights:
        pats = learning_insights.get("top_patterns") or []
        if pats:
            out.append(f"Self-learning insight: {str(pats[0])}")
    return out[:14]


def build_diagnostics_block(
    *,
    global_risk_full: dict[str, Any],
    regime_status: dict[str, Any],
    memory_payload: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    capital_status: dict[str, Any],
    portfolio_allocation: dict[str, Any],
) -> dict[str, Any]:
    brain = portfolio_allocation.get("brain") or {}
    fam_stress = sum(
        1 for r in (brain.get("family_concentration") or []) if not r.get("within_target", True)
    )
    return {
        "risk": {
            "global_risk_score": _f(global_risk_full.get("global_risk_score"), 0.0),
            "risk_level": str(global_risk_full.get("risk_level", "")),
            "components": global_risk_full.get("components") or {},
        },
        "regime": {
            "current_regime": regime_status.get("current_regime"),
            "confidence_score": _f(regime_status.get("confidence_score"), 0.0),
            "favored_strategy_families": regime_status.get("favored_strategy_families") or [],
            "reduced_strategy_families": regime_status.get("reduced_strategy_families") or [],
        },
        "memory": {
            "memory_health": memory_payload.get("memory_health"),
            "memory_entries": int(memory_payload.get("memory_entries") or 0),
            "update_count": int(memory_payload.get("update_count") or 0),
        },
        "performance": {
            "runner_success_rate": _f(performance_system.get("runner_success_rate"), 0.0),
            "runner_fail_rate": _f(performance_system.get("runner_fail_rate"), 0.0),
            "pipeline_throughput": _f(performance_system.get("pipeline_throughput"), 0.0),
            "total_jobs": int(performance_system.get("total_jobs") or performance_system.get("total_runner_jobs") or 0),
        },
        "runner": {
            "fleet_summary": multi_runner_status.get("fleet_summary") or {},
        },
        "capital": {
            "total_capital": _f(capital_status.get("total_capital"), 0.0),
            "allocated": _f(capital_status.get("allocated"), 0.0),
            "free": _f(capital_status.get("free"), 0.0),
            "risk_usage": _f(capital_status.get("risk_usage"), 0.0),
        },
        "portfolio_brain": {
            "rotate_in_n": len(brain.get("rotate_in") or []),
            "rotate_out_n": len(brain.get("rotate_out") or []),
            "family_concentration_stress": fam_stress,
        },
    }


def build_meta_ai_control_status(
    *,
    global_risk_full: dict[str, Any],
    regime_status: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    memory_payload: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    capital_status: dict[str, Any],
    fund_status: dict[str, Any],
    legacy_meta: dict[str, Any],
    learning_insights: dict[str, Any] | None = None,
) -> dict[str, Any]:
    posture, reasoning_posture = classify_system_posture(
        global_risk_full=global_risk_full,
        regime_status=regime_status,
        fund_status=fund_status,
        fund_allocation_status=fund_allocation_status,
        learning_insights=learning_insights,
    )
    conf, conf_notes = compute_control_confidence(
        global_risk_full=global_risk_full,
        memory_payload=memory_payload,
        performance_system=performance_system,
        multi_runner_status=multi_runner_status,
        learning_insights=learning_insights,
    )
    reasoning = reasoning_posture + conf_notes
    recs = build_control_recommendations(
        posture,
        regime_status=regime_status,
        global_risk_full=global_risk_full,
        legacy_meta=legacy_meta,
        learning_insights=learning_insights,
    )
    diagnostics = build_diagnostics_block(
        global_risk_full=global_risk_full,
        regime_status=regime_status,
        memory_payload=memory_payload,
        performance_system=performance_system,
        multi_runner_status=multi_runner_status,
        capital_status=capital_status,
        portfolio_allocation=portfolio_allocation,
    )
    return {
        "system_posture": posture,
        "confidence": conf,
        "reasoning": reasoning[:16],
        "recommendations": recs,
        "diagnostics": diagnostics,
        "learning_insights_used": bool(learning_insights),
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


def build_meta_recommendations_payload(control: dict[str, Any]) -> dict[str, Any]:
    return {
        "recommendations": control.get("recommendations") or [],
        "system_posture": control.get("system_posture"),
        "confidence": control.get("confidence"),
        "reasoning": control.get("reasoning") or [],
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }
