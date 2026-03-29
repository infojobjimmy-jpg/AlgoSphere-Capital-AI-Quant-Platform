"""
Read-only snapshot of the latest client research JSON for investor presentation.

Does not execute strategies, trades, or evolution — filesystem read only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import DATA_DIR


def _latest_matching(directory: Path, pattern: str) -> Path | None:
    if not directory.is_dir():
        return None
    paths = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[0] if paths else None


def get_investor_research_snapshot() -> dict[str, Any]:
    """
    Load the newest ``client_research_*.json`` under ``data/research_reports/`` if present.
    Returns a trimmed, presentation-safe dict (no full strategy parameter dumps).
    """
    rr = DATA_DIR / "research_reports"
    path = _latest_matching(rr, "client_research_*.json")
    if path is None or not path.is_file():
        return {"available": False, "message": "No client research report found. Run scripts/client_research_report.py."}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return {"available": False, "message": f"Could not read research file: {exc}"}

    pres = data.get("presentation") or {}
    risk = data.get("risk_profile") or {}
    return {
        "available": True,
        "source_path": str(path),
        "timestamp_utc": data.get("timestamp_utc"),
        "ranking_mode": data.get("ranking_mode"),
        "evaluated_count": data.get("evaluated_count"),
        "bars_used": data.get("bars_used"),
        "history_source": data.get("history_source"),
        "history_meta": data.get("history_meta"),
        "demo_only": data.get("demo_only"),
        "no_live_trading": data.get("no_live_trading"),
        "research_limitations": data.get("research_limitations", []),
        "diversified_portfolio": data.get("diversified_portfolio"),
        "risk_profile": {
            "diversification_score": risk.get("diversification_score"),
            "avg_pairwise_abs_correlation": risk.get("avg_pairwise_abs_correlation"),
            "aggregate_expected_drawdown_mean": risk.get("aggregate_expected_drawdown_mean"),
            "family_mix": risk.get("family_mix"),
            "quotas_met": risk.get("quotas_met"),
            "disclaimer": risk.get("disclaimer"),
        },
        "portfolio_selection": data.get("portfolio_selection"),
        "presentation": {
            "growth_weighted_return_proxy": pres.get("growth_weighted_return_proxy"),
            "demo_safe_weighted_return_proxy": pres.get("demo_safe_weighted_return_proxy"),
            "demo_safe_diversification_score": pres.get("demo_safe_diversification_score"),
            "growth_portfolio": pres.get("growth_portfolio"),
            "demo_safe_portfolio": pres.get("demo_safe_portfolio"),
            "top_5_safest_candidates": pres.get("top_5_safest_candidates"),
            "top_5_strongest_growth_candidates": pres.get("top_5_strongest_growth_candidates"),
            "client_demo_verdict": pres.get("client_demo_verdict"),
            "family_mix_evaluated": pres.get("family_mix_evaluated"),
            "growth_risk_profile": pres.get("growth_risk_profile"),
            "demo_safe_risk_profile": pres.get("demo_safe_risk_profile"),
        },
    }
