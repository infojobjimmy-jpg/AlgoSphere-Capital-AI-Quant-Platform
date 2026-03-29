"""
Performance analytics: read-only aggregation from run logs, paper rows, operator, recovery.
No strategy mutation, no trading, no capital deployment.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_FAIL = "FAIL"
SOURCE_RUNNER = "runner"
SOURCE_PAPER = "paper"


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        raw = str(s).replace("Z", "+00:00")
        t = datetime.fromisoformat(raw)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t
    except ValueError:
        return None


def duration_sec_between(started_at: str | None, ended_at: str | None) -> float:
    a = _parse_iso(started_at)
    b = _parse_iso(ended_at)
    if a is None or b is None:
        return 0.0
    return max(0.0, (b - a).total_seconds())


def compute_performance_score(
    success_rate: float,
    stability_score: float,
    activity_score: float,
) -> float:
    return round(
        float(success_rate) * 0.5 + float(stability_score) * 0.3 + float(activity_score) * 0.2,
        4,
    )


def _empty_metric() -> dict[str, Any]:
    return {
        "total_runs": 0,
        "success_count": 0,
        "fail_count": 0,
        "durations:sum": 0.0,
        "durations:n": 0,
        "last_run": None,
    }


def aggregate_run_logs(run_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_sid: dict[str, dict[str, Any]] = {}
    for row in run_rows:
        sid = str(row.get("strategy_id", ""))
        if not sid:
            continue
        m = by_sid.setdefault(sid, _empty_metric())
        m["total_runs"] += 1
        oc = str(row.get("outcome", "")).upper()
        if oc == OUTCOME_SUCCESS:
            m["success_count"] += 1
        elif oc == OUTCOME_FAIL:
            m["fail_count"] += 1
        src = str(row.get("source", ""))
        if src == SOURCE_RUNNER:
            d = float(row.get("duration_sec", 0.0) or 0.0)
            m["durations:sum"] += d
            m["durations:n"] += 1
        ended = str(row.get("run_ended_at", ""))
        if ended and (m["last_run"] is None or ended > str(m["last_run"])):
            m["last_run"] = ended
    return by_sid


def merge_paper_snapshots(
    by_sid: dict[str, dict[str, Any]],
    paper_items: list[dict[str, Any]],
) -> None:
    """When a strategy has no logged runs yet, infer one snapshot from paper terminal status."""
    for p in paper_items:
        sid = str(p.get("strategy_id", ""))
        if not sid:
            continue
        st = str(p.get("status", ""))
        if st not in {"PAPER_SUCCESS", "PAPER_REJECTED"}:
            continue
        m = by_sid.setdefault(sid, _empty_metric())
        if m["total_runs"] > 0:
            continue
        m["total_runs"] = 1
        if st == "PAPER_SUCCESS":
            m["success_count"] = 1
        else:
            m["fail_count"] = 1
        lu = p.get("last_updated")
        if lu:
            m["last_run"] = str(lu)


def finalize_strategy_row(strategy_id: str, m: dict[str, Any]) -> dict[str, Any]:
    tr = max(0, int(m["total_runs"]))
    sc = int(m["success_count"])
    fc = int(m["fail_count"])
    success_rate = sc / tr if tr else 0.0
    fail_rate = fc / tr if tr else 0.0
    n_dur = int(m["durations:n"])
    avg_dur = (m["durations:sum"] / n_dur) if n_dur else 0.0
    activity_score = min(1.0, tr / 20.0)
    stability_score = max(0.0, 1.0 - fail_rate)
    score = compute_performance_score(success_rate, stability_score, activity_score)
    return {
        "strategy_id": strategy_id,
        "total_runs": tr,
        "success_count": sc,
        "fail_count": fc,
        "success_rate": round(success_rate, 4),
        "avg_duration": round(avg_dur, 3),
        "performance_score": score,
        "last_run": m["last_run"],
    }


def build_strategies_performance(
    factory_strategies: list[dict[str, Any]],
    run_rows: list[dict[str, Any]],
    paper_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_sid = aggregate_run_logs(run_rows)
    merge_paper_snapshots(by_sid, paper_items)
    out: list[dict[str, Any]] = []
    for s in factory_strategies:
        sid = str(s.get("strategy_id", ""))
        if not sid:
            continue
        m = by_sid.get(sid) or _empty_metric()
        out.append(finalize_strategy_row(sid, m))
    out.sort(key=lambda x: (-float(x["performance_score"]), -int(x["total_runs"])))
    return out


def build_top_strategies(strategies_perf: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    slim = [
        {
            "strategy_id": x["strategy_id"],
            "total_runs": x["total_runs"],
            "success_rate": x["success_rate"],
            "avg_duration": x["avg_duration"],
            "performance_score": x["performance_score"],
            "last_run": x["last_run"],
        }
        for x in strategies_perf[:limit]
        if float(x.get("performance_score", 0.0)) > 0 or int(x.get("total_runs", 0)) > 0
    ]
    return slim[:limit]


def recovery_rate_from_history(recovery_history_json: str | None) -> float:
    if not recovery_history_json:
        return 0.0
    try:
        hist = json.loads(recovery_history_json)
    except json.JSONDecodeError:
        return 0.0
    if not isinstance(hist, list) or not hist:
        return 0.0
    ok = sum(1 for x in hist if isinstance(x, dict) and x.get("recovery_state") == "RECOVERY_SUCCESS")
    return round(ok / len(hist), 4)


def pipeline_throughput_score(operator_pipeline: dict[str, Any]) -> float:
    total = max(1, int(operator_pipeline.get("total_candidates", 0)))
    flow = (
        int(operator_pipeline.get("paper_success", 0))
        + int(operator_pipeline.get("demo_queued", 0))
        + int(operator_pipeline.get("live_safe_ready", 0))
        + int(operator_pipeline.get("paper_running", 0))
    )
    return round(min(1.0, flow / max(1, total)), 4)


def build_performance_trends(run_rows: list[dict[str, Any]], *, max_days: int = 14) -> list[dict[str, Any]]:
    """Daily run counts from log (UTC date of run_ended_at)."""
    by_day: dict[str, dict[str, int]] = defaultdict(lambda: {"runs": 0, "successes": 0})
    for row in run_rows:
        ended = str(row.get("run_ended_at", ""))
        if len(ended) < 10:
            continue
        day = ended[:10]
        by_day[day]["runs"] += 1
        if str(row.get("outcome", "")).upper() == OUTCOME_SUCCESS:
            by_day[day]["successes"] += 1
    days_sorted = sorted(by_day.keys(), reverse=True)[:max_days]
    days_sorted.reverse()
    return [{"date": d, **by_day[d]} for d in days_sorted]


def build_system_performance(
    *,
    run_rows: list[dict[str, Any]],
    operator_pipeline: dict[str, Any],
    recovery_history_json: str | None,
    factory_strategies: list[dict[str, Any]],
) -> dict[str, Any]:
    runner_rows = [r for r in run_rows if str(r.get("source", "")) == SOURCE_RUNNER]
    n = len(runner_rows)
    succ = sum(1 for r in runner_rows if str(r.get("outcome", "")).upper() == OUTCOME_SUCCESS)
    fail = sum(1 for r in runner_rows if str(r.get("outcome", "")).upper() == OUTCOME_FAIL)
    runner_success_rate = round(succ / n, 4) if n else 0.0
    runner_fail_rate = round(fail / n, 4) if n else 0.0
    dur_sum = sum(float(r.get("duration_sec", 0.0) or 0.0) for r in runner_rows)
    avg_runner_duration = round(dur_sum / n, 3) if n else 0.0

    ex_ready = sum(1 for s in factory_strategies if str(s.get("executor_status", "")) == "EXECUTOR_READY")
    ex_run = sum(1 for s in factory_strategies if str(s.get("executor_status", "")) == "EXECUTOR_RUNNING")

    trends = build_performance_trends(run_rows, max_days=14)

    return {
        "runner_success_rate": runner_success_rate,
        "runner_fail_rate": runner_fail_rate,
        "avg_runner_duration": avg_runner_duration,
        "pipeline_throughput": pipeline_throughput_score(operator_pipeline),
        "recovery_rate": recovery_rate_from_history(recovery_history_json),
        "total_jobs": n,
        "total_runner_jobs": n,
        "executor_ready_count": ex_ready,
        "executor_running_count": ex_run,
        "performance_trends": trends,
    }
