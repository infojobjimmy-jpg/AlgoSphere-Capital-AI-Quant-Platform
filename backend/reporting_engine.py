from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_report_summary(
    meta_status: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    capital_status: dict[str, Any],
    fund_status: dict[str, Any],
    live_safe_status: dict[str, Any],
    factory_top: dict[str, Any],
    paper_status: dict[str, Any],
    feedback_preview: dict[str, Any],
    total_strategies: int,
) -> dict[str, Any]:
    paper_items = paper_status.get("running_paper_bots", [])
    paper_running = sum(1 for x in paper_items if x.get("status") == "PAPER_RUNNING")
    paper_success = sum(1 for x in paper_items if x.get("status") == "PAPER_SUCCESS")

    warnings: list[str] = []
    if str(meta_status.get("system_health")) == "CRITICAL":
        warnings.append("Meta AI marks system health as CRITICAL.")
    if str(meta_status.get("risk_mode")) == "DEFENSIVE":
        warnings.append("Risk mode is DEFENSIVE.")
    if float(capital_status.get("risk_usage", 0.0)) > 0.8:
        warnings.append("Capital risk usage is high.")
    if int(live_safe_status.get("state_counts", {}).get("live_safe_rejected", 0)) > 0:
        warnings.append("Some live-safe candidates are rejected.")
    if paper_success == 0 and len(paper_items) > 0:
        warnings.append("No paper strategy currently in success state.")

    top_5 = []
    for x in factory_top.get("top", [])[:5]:
        top_5.append(
            {
                "strategy_id": x.get("strategy_id"),
                "family": x.get("family"),
                "fitness_score": x.get("fitness_score"),
                "status": x.get("status"),
                "generation": x.get("generation"),
                "origin_type": x.get("origin_type"),
            }
        )

    return {
        "system_health": meta_status.get("system_health", "WARNING"),
        "risk_mode": meta_status.get("risk_mode", "NORMAL"),
        "portfolio_state": fund_status.get("portfolio_state", "NORMAL"),
        "recommended_portfolio_action": fund_status.get(
            "recommended_portfolio_action", "KEEP_RUNNING"
        ),
        "total_capital": capital_status.get("total_capital", 0.0),
        "allocated": capital_status.get("allocated", 0.0),
        "free": capital_status.get("free", 0.0),
        "growth_rate": capital_status.get("growth_rate", 0.0),
        "total_strategies": total_strategies,
        "live_safe_candidates": live_safe_status.get("state_counts", {}).get(
            "live_safe_candidate", 0
        )
        + live_safe_status.get("state_counts", {}).get("approved_for_live_review", 0)
        + live_safe_status.get("state_counts", {}).get("live_safe_ready", 0),
        "paper_running": paper_running,
        "paper_success": paper_success,
        "top_5_strategies": top_5,
        "warnings": warnings,
        "recommendations": meta_status.get("recommendations", []),
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_report_daily(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "system_health": summary.get("system_health"),
        "risk_mode": summary.get("risk_mode"),
        "portfolio_state": summary.get("portfolio_state"),
        "capital": {
            "total": summary.get("total_capital"),
            "allocated": summary.get("allocated"),
            "free": summary.get("free"),
            "growth_rate": summary.get("growth_rate"),
        },
        "strategy_counts": {
            "total_strategies": summary.get("total_strategies"),
            "live_safe_candidates": summary.get("live_safe_candidates"),
            "paper_running": summary.get("paper_running"),
            "paper_success": summary.get("paper_success"),
        },
        "top_3": summary.get("top_5_strategies", [])[:3],
        "warnings": summary.get("warnings", []),
    }
