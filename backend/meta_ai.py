from __future__ import annotations

from typing import Any


def build_meta_status(
    fund_status: dict[str, Any],
    factory_strategies: list[dict[str, Any]],
    paper_status: dict[str, Any],
    auto_status: dict[str, Any],
    portfolio_brain: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = fund_status.get("summary", {})
    total_profit = float(summary.get("total_profit", 0.0))
    average_score = float(summary.get("average_score", 0.0))

    paper_items = paper_status.get("running_paper_bots", [])
    paper_count = len(paper_items)
    success_count = sum(1 for x in paper_items if x.get("status") == "PAPER_SUCCESS")
    paper_success_rate = (success_count / paper_count) if paper_count else 0.0
    avg_paper_drawdown = (
        sum(float(x.get("paper_drawdown", 0.0)) for x in paper_items) / paper_count
        if paper_count
        else 0.0
    )

    gen0 = [float(s.get("fitness_score", 0.0)) for s in factory_strategies if int(s.get("generation", 0)) == 0]
    genN = [float(s.get("fitness_score", 0.0)) for s in factory_strategies if int(s.get("generation", 0)) > 0]
    g0_avg = (sum(gen0) / len(gen0)) if gen0 else 0.0
    gN_avg = (sum(genN) / len(genN)) if genN else 0.0
    evolution_rate = round(gN_avg - g0_avg, 2)

    if avg_paper_drawdown >= 250 or fund_status.get("portfolio_state") in {"DEFENSIVE", "LOCKDOWN"}:
        risk_mode = "DEFENSIVE"
    else:
        risk_mode = "NORMAL"

    if total_profit < 0 or average_score < 40 or paper_success_rate < 0.35:
        generation_speed = "SLOW"
    elif total_profit > 100 and average_score > 60 and paper_success_rate >= 0.55 and evolution_rate > 0:
        generation_speed = "FAST"
    else:
        generation_speed = "NORMAL"

    if average_score >= 65 and paper_success_rate >= 0.55:
        portfolio_quality = "HIGH"
    elif average_score >= 45:
        portfolio_quality = "MEDIUM"
    else:
        portfolio_quality = "LOW"

    if risk_mode == "DEFENSIVE" and portfolio_quality == "LOW":
        system_health = "CRITICAL"
    elif portfolio_quality == "HIGH" and risk_mode == "NORMAL":
        system_health = "GOOD"
    else:
        system_health = "WARNING"

    recommendations: list[str] = []
    if generation_speed == "SLOW":
        recommendations.append("Slow candidate generation; prioritize quality and risk cleanup.")
    elif generation_speed == "FAST":
        recommendations.append("Accelerate evolution cycles while keeping paper-only safeguards.")
    else:
        recommendations.append("Keep steady generation cadence and monitor quality.")

    if risk_mode == "DEFENSIVE":
        recommendations.append("Maintain defensive risk mode until drawdown improves.")
    if evolution_rate <= 0:
        recommendations.append("Evolution quality is flat/negative; increase mutation diversity.")
    if paper_success_rate < 0.5:
        recommendations.append("Paper success rate is weak; tighten promotion gates.")

    brain_diag: dict[str, Any] = {}
    if portfolio_brain:
        brain_diag["rotate_in_n"] = len(portfolio_brain.get("rotate_in") or [])
        brain_diag["rotate_out_n"] = len(portfolio_brain.get("rotate_out") or [])
        brain_diag["shift_pairs"] = len(portfolio_brain.get("capital_shift_recommendations") or [])
        for row in (portfolio_brain.get("top_priorities") or [])[:2]:
            act = str(row.get("brain_action", ""))
            sid = str(row.get("strategy_id", ""))[:12]
            if act and sid:
                recommendations.append(f"Portfolio brain [{act}] {sid}… — {str(row.get('brain_reason', ''))[:100]}")
        for fam_row in portfolio_brain.get("family_concentration") or []:
            if not fam_row.get("within_target", True):
                recommendations.append(
                    f"Family {fam_row.get('family')} over-concentrated (~{float(fam_row.get('weight_share', 0)):.0%}); "
                    "consider rebalancing per portfolio brain."
                )

    return {
        "system_health": system_health,
        "risk_mode": risk_mode,
        "generation_speed": generation_speed,
        "evolution_rate": evolution_rate,
        "portfolio_quality": portfolio_quality,
        "recommendations": recommendations,
        "diagnostics": {
            "paper_success_rate": round(paper_success_rate, 3),
            "average_paper_drawdown": round(avg_paper_drawdown, 2),
            "average_strategy_score": round(average_score, 2),
            "total_profit": round(total_profit, 2),
            "loops_completed": int(auto_status.get("loops_completed", 0)),
            "portfolio_brain": brain_diag,
        },
    }
