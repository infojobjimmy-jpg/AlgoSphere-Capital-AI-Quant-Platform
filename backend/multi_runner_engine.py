"""
Multi-runner coordination for demo/simulation only.
Assignment and fleet visibility — no broker, no live trading.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .demo_runner_bridge import (
    RUNNER_ACKNOWLEDGED,
    RUNNER_ACTIVE,
    RUNNER_PAUSED,
    RUNNER_PENDING,
)

RUNNER_ONLINE = "RUNNER_ONLINE"  # reserved / docs; fleet uses IDLE when healthy with zero load
RUNNER_BUSY = "RUNNER_BUSY"
RUNNER_IDLE = "RUNNER_IDLE"
RUNNER_OFFLINE = "RUNNER_OFFLINE"
RUNNER_DEGRADED = "RUNNER_DEGRADED"

COUNTS_TOWARD_CAPACITY = {
    RUNNER_PENDING,
    RUNNER_ACKNOWLEDGED,
    RUNNER_ACTIVE,
    RUNNER_PAUSED,
}

# Heartbeat older than this is treated as degraded for scheduling (deterministic).
STALE_SECONDS = 120


def _parse_ts(s: str | None) -> datetime | None:
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


def seconds_since_last_seen(last_seen_at: str | None, now: datetime) -> float | None:
    t = _parse_ts(last_seen_at)
    if t is None:
        return None
    return max(0.0, (now - t).total_seconds())


def effective_runner_status(
    row: dict[str, Any],
    *,
    now: datetime,
    assigned_jobs_count: int,
) -> tuple[str, str]:
    """
    Returns (display_status, explain_tag).
    """
    db_st = str(row.get("runner_status", RUNNER_OFFLINE))
    if db_st == RUNNER_OFFLINE:
        return RUNNER_OFFLINE, "fleet_marked_offline"

    age = seconds_since_last_seen(str(row.get("last_seen_at") or ""), now)
    if age is None or age > STALE_SECONDS:
        return RUNNER_DEGRADED, "heartbeat_stale_or_missing"

    cap = int(row.get("runner_capacity", 1) or 1)
    load_reported = int(row.get("current_load", 0) or 0)
    used = max(load_reported, int(assigned_jobs_count))
    if used <= 0:
        return RUNNER_IDLE, "online_zero_load"
    if used >= cap:
        return RUNNER_BUSY, "at_capacity"
    return RUNNER_BUSY, "carrying_load"


def is_assignable_effective(display_status: str) -> bool:
    return display_status in {RUNNER_IDLE, RUNNER_BUSY}


def count_assigned_jobs_for_runner(
    strategies: list[dict[str, Any]],
    runner_id: str,
) -> int:
    n = 0
    for s in strategies:
        if str(s.get("runner_id", "")) != runner_id:
            continue
        st = str(s.get("runner_status", ""))
        if st in COUNTS_TOWARD_CAPACITY:
            n += 1
    return n


def job_needs_fleet_assignment(job: dict[str, Any]) -> bool:
    if not job.get("eligible"):
        return False
    rid = str(job.get("runner_id", "") or "")
    if rid:
        return False
    st = str(job.get("runner_status", "") or "")
    return st in {"", RUNNER_PENDING}


def plan_balanced_assignments(
    jobs: list[dict[str, Any]],
    runner_rows: list[dict[str, Any]],
    *,
    now: datetime,
    strategies: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Greedy: highest runner_priority jobs first; pick runner with greatest spare capacity,
    tie-break runner_id lexicographic. Deterministic.
    """
    candidates = [j for j in jobs if job_needs_fleet_assignment(j)]
    candidates.sort(
        key=lambda j: (-float(j.get("runner_priority", 0.0)), str(j.get("strategy_id", ""))),
    )

    enriched: list[dict[str, Any]] = []
    for r in runner_rows:
        rid = str(r["runner_id"])
        ac = count_assigned_jobs_for_runner(strategies, rid)
        disp, tag = effective_runner_status(r, now=now, assigned_jobs_count=ac)
        cap = max(1, int(r.get("runner_capacity", 1) or 1))
        load_rep = int(r.get("current_load", 0) or 0)
        used = max(load_rep, ac)
        spare = max(0, cap - used)
        enriched.append(
            {
                **r,
                "display_status": disp,
                "status_explain": tag,
                "assigned_jobs_count": ac,
                "spare_capacity": spare,
            }
        )

    assigned: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    # Work on mutable spare copy
    spare_by_id = {str(x["runner_id"]): int(x["spare_capacity"]) for x in enriched}

    for job in candidates:
        sid = str(job.get("strategy_id", ""))
        assignable = [
            x
            for x in enriched
            if is_assignable_effective(str(x["display_status"])) and spare_by_id.get(str(x["runner_id"]), 0) > 0
        ]
        if not assignable:
            reasons = []
            for x in enriched:
                rid = str(x["runner_id"])
                if not is_assignable_effective(str(x["display_status"])):
                    reasons.append(f"{rid}:skipped_not_healthy({x['display_status']})")
                elif spare_by_id.get(rid, 0) <= 0:
                    reasons.append(f"{rid}:skipped_no_spare_capacity")
            skipped.append(
                {
                    "strategy_id": sid,
                    "reason": "no_eligible_runner_with_spare_capacity",
                    "runner_notes": reasons,
                }
            )
            continue

        assignable.sort(
            key=lambda x: (-spare_by_id[str(x["runner_id"])], str(x["runner_id"])),
        )
        pick = assignable[0]
        rid = str(pick["runner_id"])
        spare_before = spare_by_id[rid]
        spare_by_id[rid] = spare_before - 1
        assigned.append(
            {
                "strategy_id": sid,
                "runner_id": rid,
                "reason": (
                    f"balanced_pick: highest_spare_capacity={spare_before} "
                    f"then_lexicographic_runner_id among healthy runners"
                ),
            }
        )

    return assigned, skipped


def build_fleet_summary(
    runner_payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for r in runner_payloads:
        st = str(r.get("runner_status", ""))
        counts[st] = counts.get(st, 0) + 1
    degraded = sum(1 for r in runner_payloads if r.get("runner_status") == RUNNER_DEGRADED)
    offline = sum(1 for r in runner_payloads if r.get("runner_status") == RUNNER_OFFLINE)
    healthy = sum(
        1 for r in runner_payloads if r.get("runner_status") not in {RUNNER_OFFLINE, RUNNER_DEGRADED}
    )
    return {
        "runner_count": len(runner_payloads),
        "by_status": counts,
        "healthy_runner_count": healthy,
        "degraded_count": degraded,
        "offline_count": offline,
        "stale_threshold_seconds": STALE_SECONDS,
    }


def group_jobs_by_runner(
    jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    by_runner: dict[str, list[dict[str, Any]]] = {}
    unassigned: list[dict[str, Any]] = []
    for j in jobs:
        rid = str(j.get("runner_id", "") or "")
        if not rid:
            unassigned.append(j)
        else:
            by_runner.setdefault(rid, []).append(j)
    return {
        "unassigned_queue": unassigned,
        "by_runner": by_runner,
        "total_jobs": len(jobs),
    }
