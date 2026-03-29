"""
Market Regime Engine: read-only regime classification from existing signals.
Decision layer only — no trading, broker execution, capital deployment, or strategy mutation.
"""

from __future__ import annotations

from typing import Any

from .bot_factory import STRATEGY_FAMILIES

REGIME_TRENDING = "TRENDING"
REGIME_RANGING = "RANGING"
REGIME_VOLATILE = "VOLATILE"
REGIME_CHAOTIC = "CHAOTIC"
REGIME_TRANSITIONAL = "TRANSITIONAL"

ALL_REGIMES = (
    REGIME_TRENDING,
    REGIME_RANGING,
    REGIME_VOLATILE,
    REGIME_CHAOTIC,
    REGIME_TRANSITIONAL,
)

FAMILIES_SET = frozenset(STRATEGY_FAMILIES)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _paper_success_rate(paper_status: dict[str, Any]) -> float:
    items = paper_status.get("running_paper_bots") or []
    n = len(items)
    if not n:
        return 0.0
    ok = sum(1 for x in items if str(x.get("status", "")) == "PAPER_SUCCESS")
    return ok / n


def _family_metric_avgs(
    factory_strategies: list[dict[str, Any]],
    strategies_performance: list[dict[str, Any]],
    key: str,
) -> dict[str, float]:
    by_sid: dict[str, float] = {}
    for row in strategies_performance:
        sid = str(row.get("strategy_id", "") or "")
        if sid:
            by_sid[sid] = _f(row.get(key), 0.0)
    sums: dict[str, list[float]] = {f: [] for f in STRATEGY_FAMILIES}
    for s in factory_strategies:
        sid = str(s.get("strategy_id", "") or "")
        fam = str(s.get("family", "") or "")
        if fam not in FAMILIES_SET or sid not in by_sid:
            continue
        sums[fam].append(by_sid[sid])
    return {f: (sum(v) / len(v)) for f, v in sums.items() if v}


def _weakest_family_by_metric(
    fam_avgs: dict[str, float],
) -> str | None:
    if not fam_avgs:
        return None
    return min(fam_avgs.keys(), key=lambda k: fam_avgs[k])


def _score_regime_candidates(features: dict[str, float], fam_edges: dict[str, float]) -> dict[str, float]:
    grs = features["global_risk_score"]
    dd_c = features["drawdown_risk"]
    run_c = features["runner_risk"]
    corr_c = features["correlation_risk"]
    conc_c = features["concentration_risk"]
    pipe_c = features["pipeline_risk"]
    pret = features["portfolio_return_norm"]
    pret_flat = features["portfolio_return_flatness"]
    fund_dd = features["fund_drawdown"]
    r_fail = features["runner_fail_rate"]
    pipe = features["pipeline_throughput"]
    stressful = features["stressful_component_count"]

    mom_edge = fam_edges.get("momentum_minus_mr", 0.0)
    mr_edge = -mom_edge

    chaotic = (
        2.4 * grs
        + 1.85 * features["critical_risk_flag"]
        + 0.55 * stressful
        + 0.45 * _clamp(dd_c + run_c, 0.0, 2.0)
    )

    volatile = (
        1.35 * dd_c
        + 1.15 * run_c
        + 1.05 * r_fail
        + 0.75 * (1.0 - pipe)
        + 0.35 * pipe_c
        - 0.85 * features["critical_risk_flag"]
    )

    trending = (
        1.25 * pret
        + 0.95 * (1.0 - corr_c)
        + 0.65 * (1.0 - fund_dd)
        + 0.55 * pipe
        + 0.45 * _clamp(0.5 + mom_edge, 0.0, 1.0)
        + 0.25 * (1.0 - grs)
    )

    ranging = (
        1.05 * corr_c
        + 0.95 * pret_flat
        + 0.40 * (1.0 - pipe)
        + 0.50 * _clamp(0.5 + mr_edge, 0.0, 1.0)
        + 0.20 * (1.0 - dd_c)
    )

    transitional = 0.72 + 1.15 * features["signal_ambiguity"] + 0.55 * _clamp(1.0 - abs(grs - 0.5) * 2.0, 0.0, 1.0)

    return {
        REGIME_CHAOTIC: max(0.0, chaotic),
        REGIME_VOLATILE: max(0.0, volatile),
        REGIME_TRENDING: max(0.0, trending),
        REGIME_RANGING: max(0.0, ranging),
        REGIME_TRANSITIONAL: max(0.0, transitional),
    }


def _compute_features(
    *,
    performance_system: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    paper_status: dict[str, Any],
    global_risk_assessment: dict[str, Any],
    meta_status: dict[str, Any],
) -> dict[str, float]:
    comps = global_risk_assessment.get("components") or {}
    grs = _f(global_risk_assessment.get("global_risk_score"), 0.0)
    risk_level = str(global_risk_assessment.get("risk_level", "") or "")
    critical = 1.0 if risk_level == "CRITICAL" else 0.0

    dd_c = _f(comps.get("drawdown_risk"), 0.0)
    run_c = _f(comps.get("runner_risk"), 0.0)
    corr_c = _f(comps.get("correlation_risk"), 0.0)
    conc_c = _f(comps.get("concentration_risk"), 0.0)
    pipe_c = _f(comps.get("pipeline_risk"), 0.0)

    stressful = sum(1 for k in ("drawdown_risk", "runner_risk", "correlation_risk", "concentration_risk", "pipeline_risk") if _f(comps.get(k), 0.0) >= 0.68)

    pret_raw = _f(fund_allocation_status.get("portfolio_return"), 0.0)
    pret = _clamp((pret_raw + 0.02) / 0.06, 0.0, 1.0)
    pret_flat = 1.0 - _clamp(abs(pret_raw) / 0.025, 0.0, 1.0)

    fund_dd = _f(fund_allocation_status.get("drawdown"), 0.0)

    r_fail = _f(performance_system.get("runner_fail_rate"), 0.0)
    pipe = _f(performance_system.get("pipeline_throughput"), 0.5)

    paper_sr = _paper_success_rate(paper_status)
    ambiguity_piece = 1.0 - abs(paper_sr - 0.5) * 2.0
    signal_ambiguity = _clamp(0.35 * ambiguity_piece + 0.25 * (1.0 - min(1.0, grs + 0.15)), 0.0, 1.0)

    return {
        "global_risk_score": grs,
        "critical_risk_flag": critical,
        "drawdown_risk": dd_c,
        "runner_risk": run_c,
        "correlation_risk": corr_c,
        "concentration_risk": conc_c,
        "pipeline_risk": pipe_c,
        "stressful_component_count": float(stressful),
        "portfolio_return_norm": pret,
        "portfolio_return_flatness": pret_flat,
        "fund_drawdown": fund_dd,
        "runner_fail_rate": r_fail,
        "pipeline_throughput": pipe,
        "paper_success_rate": paper_sr,
        "signal_ambiguity": signal_ambiguity,
    }


def _data_quality(
    performance_system: dict[str, Any],
    paper_status: dict[str, Any],
    factory_strategies: list[dict[str, Any]],
    strategies_performance: list[dict[str, Any]],
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    n_jobs = int(performance_system.get("total_jobs") or performance_system.get("total_runner_jobs") or 0)
    paper_n = len(paper_status.get("running_paper_bots") or [])
    fac_n = len(factory_strategies)
    perf_n = len(strategies_performance)

    score = 0.0
    if n_jobs > 0:
        score += 0.35
    else:
        reasons.append("No runner job history yet; runner-based signals are defaulting.")
    if paper_n > 0:
        score += 0.30
    else:
        reasons.append("No paper bots loaded; paper outcome signals are limited.")
    if fac_n > 0:
        score += 0.20
    if perf_n > 0:
        score += 0.15
    return _clamp(score, 0.25, 1.0), reasons


def _select_regime(
    scores: dict[str, float],
    features: dict[str, float],
    risk_level: str,
    global_risk_score: float,
) -> tuple[str, float, list[str]]:
    reasoning: list[str] = []
    ordered = sorted(scores.items(), key=lambda x: -x[1])
    best_name, best_s = ordered[0]
    second_s = ordered[1][1] if len(ordered) > 1 else 0.0
    gap = best_s - second_s

    if risk_level == "CRITICAL" or global_risk_score >= 0.72:
        regime = REGIME_CHAOTIC
        reasoning.append("Global risk is CRITICAL or composite score is very high — capital-defense posture.")
    elif global_risk_score >= 0.62 and scores[REGIME_CHAOTIC] >= scores[REGIME_TRENDING]:
        regime = REGIME_CHAOTIC
        reasoning.append("Elevated global risk with chaotic stress profile — prioritize safety over style bets.")
    elif gap < 0.09 and best_name != REGIME_CHAOTIC:
        regime = REGIME_TRANSITIONAL
        reasoning.append("Competing regime scores are close — treat environment as transitional.")
    else:
        regime = best_name

    if regime == REGIME_TRENDING:
        reasoning.append("Fund sim return and flow proxies favor directional persistence.")
    elif regime == REGIME_RANGING:
        reasoning.append("Correlation and flat return proxies favor mean-reversion conditions.")
    elif regime == REGIME_VOLATILE:
        reasoning.append("Drawdown and runner stress elevated while systemic risk is not maximal.")
    elif regime == REGIME_CHAOTIC:
        reasoning.append("Multiple risk channels stressed — reduce exposure to aggressive style risk.")
    else:
        reasoning.append("Mixed or thin signals — prefer balanced family weights and avoid concentration.")

    conf = _clamp(0.28 + 0.62 * (gap / (gap + 0.22)), 0.0, 1.0)
    return regime, conf, reasoning


def _family_lists(
    regime: str,
    brain: dict[str, Any],
    fam_perf: dict[str, float],
    fam_succ: dict[str, float],
) -> tuple[list[str], list[str], list[str]]:
    favored: list[str] = []
    reduced: list[str] = []
    paused: list[str] = []

    over_conc = [str(r.get("family")) for r in (brain.get("family_concentration") or []) if not r.get("within_target", True)]
    over_conc = [f for f in over_conc if f in FAMILIES_SET]

    fragile = _weakest_family_by_metric(fam_succ) or _weakest_family_by_metric(fam_perf)

    if regime == REGIME_TRENDING:
        favored = ["MOMENTUM", "EMA_CROSS"]
        reduced = ["MEAN_REVERSION"] + [f for f in over_conc if f not in favored]
    elif regime == REGIME_RANGING:
        favored = ["MEAN_REVERSION"]
        reduced = ["MOMENTUM"]
        if "SESSION_BREAKOUT" not in favored:
            reduced.append("SESSION_BREAKOUT")
        reduced.extend(f for f in over_conc if f not in reduced and f not in favored)
    elif regime == REGIME_VOLATILE:
        favored = ["SESSION_BREAKOUT"]
        reduced = ["MEAN_REVERSION"]
        if fragile and fragile != "SESSION_BREAKOUT":
            reduced.append(fragile)
        reduced = list(dict.fromkeys(reduced))
    elif regime == REGIME_CHAOTIC:
        paused = ["MOMENTUM", "SESSION_BREAKOUT"]
        reduced = ["EMA_CROSS", "MEAN_REVERSION"]
    else:
        reduced = list(dict.fromkeys(over_conc))

    def _clean(lst: list[str]) -> list[str]:
        out: list[str] = []
        for x in lst:
            if x in FAMILIES_SET and x not in out:
                out.append(x)
        return out

    favored = _clean(favored)
    reduced = _clean([x for x in reduced if x not in favored and x not in paused])
    paused = _clean([x for x in paused if x not in favored])
    return favored, reduced, paused


def build_market_regime_payload(
    *,
    performance_system: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    fund_allocation_status: dict[str, Any],
    paper_status: dict[str, Any],
    global_risk_assessment: dict[str, Any],
    meta_status: dict[str, Any],
    factory_strategies: list[dict[str, Any]],
    strategies_performance: list[dict[str, Any]],
) -> dict[str, Any]:
    brain = portfolio_allocation.get("brain") or {}
    fam_perf = _family_metric_avgs(factory_strategies, strategies_performance, "performance_score")
    fam_succ = _family_metric_avgs(factory_strategies, strategies_performance, "success_rate")
    mom = fam_perf.get("MOMENTUM")
    mr = fam_perf.get("MEAN_REVERSION")
    mom_edge = (mom - mr) if mom is not None and mr is not None else 0.0

    features = _compute_features(
        performance_system=performance_system,
        portfolio_allocation=portfolio_allocation,
        fund_allocation_status=fund_allocation_status,
        paper_status=paper_status,
        global_risk_assessment=global_risk_assessment,
        meta_status=meta_status,
    )
    scores = _score_regime_candidates(features, {"momentum_minus_mr": mom_edge})

    risk_level = str(global_risk_assessment.get("risk_level", "") or "")
    grs = features["global_risk_score"]
    regime, base_conf, regime_reasoning = _select_regime(scores, features, risk_level, grs)

    dq, dq_reasons = _data_quality(performance_system, paper_status, factory_strategies, strategies_performance)
    confidence = round(_clamp(base_conf * dq, 0.15, 0.97), 4)

    if dq < 0.7:
        regime_reasoning = dq_reasons[:2] + regime_reasoning

    if str(meta_status.get("system_health")) == "CRITICAL" and regime != REGIME_CHAOTIC:
        regime_reasoning.insert(0, "Meta AI system_health is CRITICAL — align sizing with defensive playbook.")

    favored, reduced, paused = _family_lists(regime, brain, fam_perf, fam_succ)

    return {
        "current_regime": regime,
        "confidence_score": confidence,
        "regime_reasoning": regime_reasoning[:8],
        "favored_strategy_families": favored,
        "reduced_strategy_families": reduced,
        "paused_strategy_families": paused,
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


def build_regime_recommendations_response(status_public: dict[str, Any]) -> dict[str, Any]:
    regime = str(status_public.get("current_regime", REGIME_TRANSITIONAL))
    recs: list[dict[str, str]] = []

    def add(fam: str, action: str, reason: str) -> None:
        recs.append({"family": fam, "action": action, "reason": reason})

    for fam in status_public.get("favored_strategy_families") or []:
        add(str(fam), "FAVOR", f"{regime} regime — style alignment.")
    for fam in status_public.get("reduced_strategy_families") or []:
        add(str(fam), "REDUCE", f"{regime} regime — trim exposure.")
    for fam in status_public.get("paused_strategy_families") or []:
        add(str(fam), "PAUSE", f"{regime} regime — avoid aggressive deployment.")

    return {"recommendations": recs, "decision_layer_only": True, "demo_simulation_only": True}


def meta_regime_diagnostic(status_public: dict[str, Any]) -> dict[str, Any]:
    return {
        "current_regime": status_public.get("current_regime"),
        "confidence_score": status_public.get("confidence_score"),
    }


def advisory_line_for_meta(status_public: dict[str, Any]) -> str:
    r = status_public.get("current_regime", "")
    c = status_public.get("confidence_score", 0.0)
    return f"Market regime (advisory): {r} — confidence {float(c):.2f}."
