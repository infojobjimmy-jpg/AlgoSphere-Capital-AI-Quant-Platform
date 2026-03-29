"""
Global Risk Engine: portfolio-level risk view from existing signals only.
Decision layer — no trading, broker execution, or capital deployment.
"""

from __future__ import annotations

from typing import Any

RISK_LOW = "LOW"
RISK_MODERATE = "MODERATE"
RISK_HIGH = "HIGH"
RISK_CRITICAL = "CRITICAL"

# Component weights (sum = 1.0)
W_CONCENTRATION = 0.18
W_CORRELATION = 0.15
W_DRAWDOWN = 0.18
W_RUNNER = 0.17
W_PIPELINE = 0.17
W_CAPITAL = 0.15


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def risk_level_from_score(score: float) -> str:
    s = float(score)
    if s < 0.30:
        return RISK_LOW
    if s < 0.50:
        return RISK_MODERATE
    if s < 0.70:
        return RISK_HIGH
    return RISK_CRITICAL


def compute_concentration_risk(
    portfolio_allocation: dict[str, Any],
    fund_allocation_status: dict[str, Any],
) -> float:
    fund_hhi = float(fund_allocation_status.get("risk_score", 0.0) or 0.0)
    brain = portfolio_allocation.get("brain") or {}
    fam_rows = brain.get("family_concentration") or []
    fam_stress = 0.0
    for row in fam_rows:
        sh = float(row.get("weight_share", 0.0) or 0.0)
        if not row.get("within_target", True):
            fam_stress = max(fam_stress, _clamp((sh - 0.28) / 0.55, 0.0, 1.0))
        else:
            fam_stress = max(fam_stress, _clamp((sh - 0.33) * 1.8, 0.0, 1.0))
    return round(_clamp(0.55 * fund_hhi + 0.45 * fam_stress, 0.0, 1.0), 4)


def compute_correlation_risk(portfolio_allocation: dict[str, Any]) -> float:
    allocs = portfolio_allocation.get("allocations") or []
    if not allocs:
        return 0.12
    vals = [float(a.get("avg_correlation", 0.0) or 0.0) for a in allocs]
    avg_c = sum(vals) / len(vals)
    # Map typical proxy band ~0.15–0.95 into 0–1 risk
    return round(_clamp((avg_c - 0.14) / 0.86, 0.0, 1.0), 4)


def compute_drawdown_risk(
    fund_allocation_status: dict[str, Any],
    paper_status: dict[str, Any],
) -> float:
    fund_dd = float(fund_allocation_status.get("drawdown", 0.0) or 0.0)
    summary = paper_status.get("summary") or {}
    paper_dd_sum = float(summary.get("drawdown", 0.0) or 0.0)
    paper_r = _clamp(paper_dd_sum / 2500.0, 0.0, 1.0)
    return round(_clamp(0.65 * fund_dd + 0.35 * paper_r, 0.0, 1.0), 4)


def compute_runner_risk(
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
) -> float:
    fail_r = float(performance_system.get("runner_fail_rate", 0.0) or 0.0)
    fs = multi_runner_status.get("fleet_summary") or {}
    rc = max(1, int(fs.get("runner_count", 0) or 0))
    bad = int(fs.get("degraded_count", 0) or 0) + int(fs.get("offline_count", 0) or 0)
    fleet_r = min(1.0, bad / float(rc))
    succ_penalty = max(0.0, 0.35 - float(performance_system.get("runner_success_rate", 0.0) or 0.0))
    mix = 0.5 * fail_r + 0.35 * fleet_r + 0.15 * succ_penalty
    return round(_clamp(mix, 0.0, 1.0), 4)


def compute_pipeline_risk(
    performance_system: dict[str, Any],
    review_status: dict[str, Any],
    *,
    factory_candidate_count: int,
    recovery_status: dict[str, Any],
) -> float:
    pt = float(performance_system.get("pipeline_throughput", 0.5) or 0.5)
    flow_risk = 1.0 - pt
    counts = review_status.get("counts") or {}
    pending = int(counts.get("PENDING_REVIEW", 0) or 0)
    backlog_r = min(1.0, pending / 160.0)
    overflow = min(1.0, max(0, factory_candidate_count - 600) / 2000.0)
    rec = str(recovery_status.get("recovery_state", "") or "")
    rec_boost = 0.22 if rec == "RECOVERY_FAILED" else 0.0
    if rec == "RECOVERY_RUNNING":
        rec_boost = max(rec_boost, 0.08)
    mix = 0.42 * flow_risk + 0.30 * backlog_r + 0.20 * overflow + rec_boost
    return round(_clamp(mix, 0.0, 1.0), 4)


def compute_capital_risk(capital_status: dict[str, Any]) -> float:
    ru = float(capital_status.get("risk_usage", 0.0) or 0.0)
    alloc_pct = float(capital_status.get("allocated", 0.0) or 0.0)
    total = float(capital_status.get("total_capital", 0.0) or 0.0)
    if total > 0 and ru < 0.05:
        ru = max(ru, min(1.0, alloc_pct / total))
    return round(_clamp(ru, 0.0, 1.0), 4)


def _build_recommendations(
    components: dict[str, float],
    portfolio_allocation: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    if components["concentration_risk"] >= 0.55:
        out.append(
            "Reduce allocation: portfolio concentration elevated — trim top weights and rebalance."
        )
    if components["correlation_risk"] >= 0.55:
        out.append(
            "Rotate strategies: correlation proxy is high — prefer less overlapping cohorts."
        )
    if components["drawdown_risk"] >= 0.55:
        out.append(
            "Pause strategies: drawdown signals (fund sim + paper) are stressed — freeze increases until recovery."
        )
    if components["runner_risk"] >= 0.55:
        out.append(
            "Runner risk: failures or degraded/offline fleet — reduce concurrent runner pressure and verify fleet health."
        )
    if components["pipeline_risk"] >= 0.55:
        out.append(
            "Pipeline risk: throughput or review backlog strained — clear review/demo bottlenecks."
        )
    if components["capital_risk"] >= 0.78:
        out.append(
            "Capital usage risk: simulated allocation usage high — reduce allocation headroom."
        )
    brain = portfolio_allocation.get("brain") or {}
    for row in brain.get("family_concentration") or []:
        if not row.get("within_target", True):
            out.append(
                f"Reduce family exposure: {row.get('family')} above balance target — increase diversification."
            )
            break
    if not out:
        out.append("Maintain current diversification; continue monitoring global risk components.")
    return out


def _build_alerts(
    components: dict[str, float],
    global_risk_score: float,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    level = risk_level_from_score(global_risk_score)

    def add(sev: str, comp: str, msg: str) -> None:
        alerts.append({"severity": sev, "component": comp, "message": msg})

    for name, val in components.items():
        if val >= 0.72:
            add("CRITICAL", name, f"{name} at {val:.2f} — immediate review recommended (decision layer).")
        elif val >= 0.52:
            add("WARNING", name, f"{name} elevated ({val:.2f}); consider protective actions.")

    if level == RISK_CRITICAL:
        add("CRITICAL", "global", f"Global risk score {global_risk_score:.2f} in CRITICAL band.")
    elif level == RISK_HIGH:
        add("WARNING", "global", f"Global risk score {global_risk_score:.2f} in HIGH band.")

    return alerts


def build_global_risk_assessment(
    *,
    portfolio_allocation: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    performance_system: dict[str, Any],
    multi_runner_status: dict[str, Any],
    recovery_status: dict[str, Any],
    capital_status: dict[str, Any],
    review_status: dict[str, Any],
    paper_status: dict[str, Any],
    factory_candidate_count: int,
) -> dict[str, Any]:
    components = {
        "concentration_risk": compute_concentration_risk(
            portfolio_allocation, fund_allocation_status
        ),
        "correlation_risk": compute_correlation_risk(portfolio_allocation),
        "drawdown_risk": compute_drawdown_risk(fund_allocation_status, paper_status),
        "runner_risk": compute_runner_risk(performance_system, multi_runner_status),
        "pipeline_risk": compute_pipeline_risk(
            performance_system,
            review_status,
            factory_candidate_count=factory_candidate_count,
            recovery_status=recovery_status,
        ),
        "capital_risk": compute_capital_risk(capital_status),
    }
    global_risk_score = (
        W_CONCENTRATION * components["concentration_risk"]
        + W_CORRELATION * components["correlation_risk"]
        + W_DRAWDOWN * components["drawdown_risk"]
        + W_RUNNER * components["runner_risk"]
        + W_PIPELINE * components["pipeline_risk"]
        + W_CAPITAL * components["capital_risk"]
    )
    global_risk_score = round(_clamp(global_risk_score, 0.0, 1.0), 4)
    risk_level = risk_level_from_score(global_risk_score)
    recommendations = _build_recommendations(components, portfolio_allocation)
    alerts = _build_alerts(components, global_risk_score)

    return {
        "global_risk_score": global_risk_score,
        "risk_level": risk_level,
        "components": components,
        "recommendations": recommendations,
        "alerts": alerts,
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


def build_global_risk_status_payload(full: dict[str, Any]) -> dict[str, Any]:
    return {
        "global_risk_score": full["global_risk_score"],
        "risk_level": full["risk_level"],
        "components": full["components"],
        "recommendations": full["recommendations"],
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


def build_global_risk_alerts_payload(full: dict[str, Any]) -> dict[str, Any]:
    return {"alerts": full.get("alerts", []), "decision_layer_only": True}
