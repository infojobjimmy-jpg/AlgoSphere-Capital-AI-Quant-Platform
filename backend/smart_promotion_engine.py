"""
Smart Promotion Engine: performance-based pipeline recommendations and safe desk actions.
No live trading, no broker execution, no capital deployment.
Does not start runner jobs, does not call executor start, and does not auto-queue demo (decision desk actions only).
"""

from __future__ import annotations

from typing import Any

PROMOTE_TO_REVIEW = "PROMOTE_TO_REVIEW"
PROMOTE_TO_DEMO = "PROMOTE_TO_DEMO"
PROMOTE_TO_EXECUTOR = "PROMOTE_TO_EXECUTOR"
PROMOTE_TO_RUNNER = "PROMOTE_TO_RUNNER"

TIER_ORDER = (
    PROMOTE_TO_REVIEW,
    PROMOTE_TO_DEMO,
    PROMOTE_TO_EXECUTOR,
    PROMOTE_TO_RUNNER,
)

TIER_RANK = {t: i for i, t in enumerate(TIER_ORDER)}

PROMOTION_THRESHOLDS: dict[str, dict[str, float | int]] = {
    PROMOTE_TO_REVIEW: {"performance_score": 0.60, "total_runs_min": 3},
    PROMOTE_TO_DEMO: {"performance_score": 0.70, "success_rate": 0.60},
    PROMOTE_TO_EXECUTOR: {"performance_score": 0.75, "success_rate": 0.65},
    PROMOTE_TO_RUNNER: {"performance_score": 0.80, "success_rate": 0.70},
}


def _row_success_rate(row: dict[str, Any]) -> float:
    return float(row.get("success_rate", 0.0) or 0.0)


def _row_perf_score(row: dict[str, Any]) -> float:
    return float(row.get("performance_score", 0.0) or 0.0)


def _row_total_runs(row: dict[str, Any]) -> int:
    return int(row.get("total_runs", 0) or 0)


def stability_score_for_row(row: dict[str, Any]) -> float:
    tr = _row_total_runs(row)
    if tr <= 0:
        return 0.0
    fc = int(row.get("fail_count", 0) or 0)
    return round(max(0.0, 1.0 - fc / tr), 4)


def activity_score_for_row(row: dict[str, Any]) -> float:
    return round(min(1.0, _row_total_runs(row) / 20.0), 4)


def qualifies_tier(row: dict[str, Any], tier: str) -> bool:
    th = PROMOTION_THRESHOLDS[tier]
    ps = _row_perf_score(row)
    sr = _row_success_rate(row)
    tr = _row_total_runs(row)
    if tier == PROMOTE_TO_REVIEW:
        return ps > float(th["performance_score"]) and tr >= int(th["total_runs_min"])
    return ps > float(th["performance_score"]) and sr > float(th["success_rate"])


def highest_tier_for_row(row: dict[str, Any]) -> str | None:
    for tier in reversed(TIER_ORDER):
        if qualifies_tier(row, tier):
            return tier
    return None


def cumulative_tier_lists(
    factory_strategies: list[dict[str, Any]],
    perf_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    perf_by_id = {str(r["strategy_id"]): r for r in perf_rows}
    review_l: list[dict[str, Any]] = []
    demo_l: list[dict[str, Any]] = []
    ex_l: list[dict[str, Any]] = []
    rn_l: list[dict[str, Any]] = []

    for s in factory_strategies:
        sid = str(s.get("strategy_id", ""))
        if not sid:
            continue
        row = perf_by_id.get(sid)
        if row is None:
            continue
        tier = highest_tier_for_row(row)
        if tier is None:
            continue
        rec = {
            "strategy_id": sid,
            "family": s.get("family"),
            "target_tier": tier,
            "performance_score": _row_perf_score(row),
            "success_rate": _row_success_rate(row),
            "stability_score": stability_score_for_row(row),
            "activity_score": activity_score_for_row(row),
            "total_runs": _row_total_runs(row),
            "review_status": str(s.get("review_status", "")),
            "demo_status": str(s.get("demo_status", "")),
            "executor_status": str(s.get("executor_status", "")),
            "runner_status": str(s.get("runner_status", "")),
        }
        if qualifies_tier(row, PROMOTE_TO_REVIEW):
            review_l.append(rec)
        if qualifies_tier(row, PROMOTE_TO_DEMO):
            demo_l.append(rec)
        if qualifies_tier(row, PROMOTE_TO_EXECUTOR):
            ex_l.append(rec)
        if qualifies_tier(row, PROMOTE_TO_RUNNER):
            rn_l.append(rec)

    return {
        "review_candidates": review_l,
        "demo_candidates": demo_l,
        "executor_candidates": ex_l,
        "runner_candidates": rn_l,
    }


def build_promotion_candidates_response(
    factory_strategies: list[dict[str, Any]],
    perf_rows: list[dict[str, Any]],
    *,
    recent_history: list[dict[str, Any]],
) -> dict[str, Any]:
    lists = cumulative_tier_lists(factory_strategies, perf_rows)
    return {
        **lists,
        "thresholds": PROMOTION_THRESHOLDS,
        "recent_promotion_history": recent_history,
    }


def zero_perf_row(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "total_runs": 0,
        "success_count": 0,
        "fail_count": 0,
        "success_rate": 0.0,
        "avg_duration": 0.0,
        "performance_score": 0.0,
        "last_run": None,
    }
