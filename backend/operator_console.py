from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_operator_console_status(
    meta_status: dict[str, Any],
    auto_status: dict[str, Any],
    report_summary: dict[str, Any],
    factory_strategies: dict[str, Any],
    paper_status: dict[str, Any],
    live_safe_status: dict[str, Any],
    review_status: dict[str, Any],
    demo_status: dict[str, Any],
    capital_status: dict[str, Any],
    portfolio_allocation: dict[str, Any],
) -> dict[str, Any]:
    review_counts = review_status.get("counts", {})
    demo_counts = demo_status.get("counts", {})
    paper_items = paper_status.get("running_paper_bots", [])
    paper_running = sum(1 for p in paper_items if p.get("status") == "PAPER_RUNNING")
    paper_success = sum(1 for p in paper_items if p.get("status") == "PAPER_SUCCESS")

    pipeline = {
        "total_candidates": int(factory_strategies.get("count", 0)),
        "paper_running": paper_running,
        "paper_success": paper_success,
        "live_safe_ready": int(live_safe_status.get("state_counts", {}).get("live_safe_ready", 0)),
        "review_pending": int(review_counts.get("PENDING_REVIEW", 0)),
        "demo_queued": int(demo_counts.get("DEMO_QUEUE", 0)),
        "demo_running": int(demo_counts.get("DEMO_RUNNING", 0)),
    }

    allocations = portfolio_allocation.get("allocations", [])
    top_allocations = sorted(
        allocations,
        key=lambda x: float(x.get("capital_percent", 0.0)),
        reverse=True,
    )[:5]
    brain = portfolio_allocation.get("brain") or {}
    portfolio = {
        "allocation_count": len(allocations),
        "total_allocated_percent": float(portfolio_allocation.get("total_allocated_percent", 0.0)),
        "top_allocations": top_allocations,
        "brain_top_priorities": (brain.get("top_priorities") or [])[:5],
        "brain_rotate_in": brain.get("rotate_in") or [],
        "brain_rotate_out": brain.get("rotate_out") or [],
        "brain_family_concentration": brain.get("family_concentration") or [],
        "brain_capital_shifts": (brain.get("capital_shift_recommendations") or [])[:4],
    }

    capital = {
        "total_capital": float(capital_status.get("total_capital", 0.0)),
        "allocated": float(capital_status.get("allocated", 0.0)),
        "free": float(capital_status.get("free", 0.0)),
        "risk_usage": float(capital_status.get("risk_usage", 0.0)),
        "growth_rate": float(capital_status.get("growth_rate", 0.0)),
    }

    risk_flags: list[str] = []
    if float(capital["risk_usage"]) >= 0.8:
        risk_flags.append("Capital risk rising")
    if capital["allocated"] >= capital["total_capital"] and capital["total_capital"] > 0:
        risk_flags.append("Capital fully allocated")
    if pipeline["paper_success"] == 0 and int(paper_status.get("count", 0)) > 0:
        risk_flags.append("No paper success")
    if pipeline["total_candidates"] > 1000:
        risk_flags.append("Too many candidates")
    if "high drawdown" in " ".join(str(x).lower() for x in report_summary.get("warnings", [])):
        risk_flags.append("High drawdown detected")

    return {
        "system_health": meta_status.get("system_health", "WARNING"),
        "risk_mode": meta_status.get("risk_mode", "NORMAL"),
        "generation_speed": meta_status.get("generation_speed", "NORMAL"),
        "loops_completed": int(auto_status.get("loops_completed", 0)),
        "last_cycle_at": auto_status.get("last_cycle_at"),
        "pipeline": pipeline,
        "capital": capital,
        "portfolio": portfolio,
        "risk_flags": risk_flags,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
