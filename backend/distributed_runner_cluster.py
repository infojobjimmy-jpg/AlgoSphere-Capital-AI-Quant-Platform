"""
Distributed Runner Cluster state and balancing helpers.
Distributed orchestration only — no trading, broker execution, or capital deployment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .database import get_alert_engine_state, get_connection, set_alert_engine_state

RUNNER_ONLINE = "RUNNER_ONLINE"
RUNNER_IDLE = "RUNNER_IDLE"
RUNNER_BUSY = "RUNNER_BUSY"
RUNNER_DEGRADED = "RUNNER_DEGRADED"
RUNNER_OFFLINE = "RUNNER_OFFLINE"

CLUSTER_HEALTHY = "HEALTHY"
CLUSTER_DEGRADED = "DEGRADED"
CLUSTER_CRITICAL = "CRITICAL"

STATE_KEY = "distributed_runner_cluster_state"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _now().isoformat()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _empty_state() -> dict[str, Any]:
    return {"version": 1, "updated_at": None, "runners": {}}


def load_cluster_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, STATE_KEY)
    if not raw:
        return _empty_state()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_state()
    if not isinstance(data, dict):
        return _empty_state()
    out = _empty_state()
    out.update(data)
    if not isinstance(out.get("runners"), dict):
        out["runners"] = {}
    return out


def save_cluster_state(state: dict[str, Any]) -> None:
    payload = json.dumps(state, separators=(",", ":"))
    with get_connection() as conn:
        set_alert_engine_state(conn, STATE_KEY, payload)


def register_runner(
    state: dict[str, Any],
    *,
    runner_id: str,
    hostname: str,
    ip: str,
    capacity: int = 4,
    current_load: int = 0,
    version: str = "",
    region: str = "global",
) -> dict[str, Any]:
    rid = str(runner_id).strip()
    if not rid:
        return {"ok": False, "error": "invalid_runner_id"}
    cap = max(1, int(capacity))
    load = max(0, min(int(current_load), cap))
    st = RUNNER_IDLE if load == 0 else RUNNER_BUSY
    row = {
        "runner_id": rid,
        "hostname": str(hostname or ""),
        "ip": str(ip or ""),
        "status": st if st != RUNNER_IDLE else RUNNER_ONLINE,
        "capacity": cap,
        "current_load": load,
        "last_seen": _iso_now(),
        "version": str(version or ""),
        "region": str(region or "global"),
    }
    state.setdefault("runners", {})[rid] = row
    state["updated_at"] = _iso_now()
    return {"ok": True, "runner": row}


def heartbeat_runner(
    state: dict[str, Any],
    *,
    runner_id: str,
    current_load: int = 0,
    version: str | None = None,
) -> dict[str, Any]:
    rid = str(runner_id).strip()
    row = (state.get("runners") or {}).get(rid)
    if not isinstance(row, dict):
        return {"ok": False, "error": "runner_not_found", "runner_id": rid}
    cap = max(1, int(row.get("capacity", 1) or 1))
    load = max(0, min(int(current_load), cap))
    row["current_load"] = load
    row["last_seen"] = _iso_now()
    row["status"] = RUNNER_IDLE if load == 0 else RUNNER_BUSY
    if version is not None and str(version):
        row["version"] = str(version)
    state["updated_at"] = row["last_seen"]
    return {"ok": True, "runner": row}


def mark_runner_offline(state: dict[str, Any], *, runner_id: str) -> dict[str, Any]:
    rid = str(runner_id).strip()
    row = (state.get("runners") or {}).get(rid)
    if not isinstance(row, dict):
        return {"ok": False, "error": "runner_not_found", "runner_id": rid}
    row["status"] = RUNNER_OFFLINE
    row["last_seen"] = _iso_now()
    state["updated_at"] = row["last_seen"]
    return {"ok": True, "runner": row}


def apply_offline_detection(
    state: dict[str, Any],
    *,
    stale_seconds: int = 180,
) -> dict[str, Any]:
    now = _now()
    changed = 0
    for row in (state.get("runners") or {}).values():
        if not isinstance(row, dict):
            continue
        raw = str(row.get("last_seen", "") or "").replace("Z", "+00:00")
        try:
            t = datetime.fromisoformat(raw)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
        except ValueError:
            row["status"] = RUNNER_DEGRADED
            changed += 1
            continue
        age = max(0.0, (now - t).total_seconds())
        if age > float(stale_seconds):
            row["status"] = RUNNER_OFFLINE
            changed += 1
        elif row.get("status") == RUNNER_ONLINE:
            row["status"] = RUNNER_IDLE
    if changed:
        state["updated_at"] = _iso_now()
    return {"changed": changed, "state": state}


def _runner_rank(row: dict[str, Any], *, preferred_region: str | None = None) -> tuple[int, float, str]:
    st = str(row.get("status", ""))
    # lower rank is better
    if st in {RUNNER_OFFLINE, RUNNER_DEGRADED}:
        health_rank = 2
    elif st in {RUNNER_IDLE, RUNNER_ONLINE, RUNNER_BUSY}:
        health_rank = 0
    else:
        health_rank = 1
    region = str(row.get("region", "global"))
    region_penalty = 0 if not preferred_region or region == preferred_region else 1
    cap = max(1, int(row.get("capacity", 1) or 1))
    load = max(0, int(row.get("current_load", 0) or 0))
    load_ratio = load / float(cap)
    return (health_rank + region_penalty, load_ratio, str(row.get("runner_id", "")))


def pick_best_runner(
    state: dict[str, Any],
    *,
    preferred_region: str | None = None,
) -> dict[str, Any] | None:
    rows = [r for r in (state.get("runners") or {}).values() if isinstance(r, dict)]
    rows = [r for r in rows if str(r.get("status", "")) not in {RUNNER_OFFLINE, RUNNER_DEGRADED}]
    rows = [r for r in rows if int(r.get("current_load", 0) or 0) < int(r.get("capacity", 1) or 1)]
    if not rows:
        return None
    rows.sort(key=lambda r: _runner_rank(r, preferred_region=preferred_region))
    return rows[0]


def estimate_failover_reassignments(
    state: dict[str, Any],
    *,
    failed_runner_id: str,
    queued_jobs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    jobs = list(queued_jobs or [])
    out: list[dict[str, Any]] = []
    for j in jobs:
        rid = str(j.get("runner_id", "") or "")
        if rid and rid != failed_runner_id:
            continue
        target = pick_best_runner(state, preferred_region=str(j.get("region", "") or None))
        if target is None:
            out.append(
                {
                    "strategy_id": str(j.get("strategy_id", "")),
                    "from_runner_id": failed_runner_id,
                    "to_runner_id": None,
                    "reason": "no_healthy_capacity_available",
                }
            )
            continue
        out.append(
            {
                "strategy_id": str(j.get("strategy_id", "")),
                "from_runner_id": failed_runner_id,
                "to_runner_id": str(target.get("runner_id")),
                "reason": "offline_failover_reassignment",
            }
        )
    return out


def build_cluster_status_payload(state: dict[str, Any]) -> dict[str, Any]:
    rows = [r for r in (state.get("runners") or {}).values() if isinstance(r, dict)]
    runner_count = len(rows)
    healthy = sum(1 for r in rows if str(r.get("status")) in {RUNNER_ONLINE, RUNNER_IDLE, RUNNER_BUSY})
    offline = sum(1 for r in rows if str(r.get("status")) == RUNNER_OFFLINE)
    degraded = sum(1 for r in rows if str(r.get("status")) == RUNNER_DEGRADED)
    total_cap = sum(max(1, int(r.get("capacity", 1) or 1)) for r in rows)
    if runner_count == 0:
        cluster_health = CLUSTER_DEGRADED
    elif offline > 0 and (offline + degraded) >= max(1, runner_count // 2):
        cluster_health = CLUSTER_CRITICAL
    elif offline > 0 or degraded > 0:
        cluster_health = CLUSTER_DEGRADED
    else:
        cluster_health = CLUSTER_HEALTHY
    return {
        "runner_count": runner_count,
        "healthy_runners": healthy,
        "offline_runners": offline,
        "total_capacity": total_cap,
        "cluster_health": cluster_health,
        "decision_layer_only": True,
        "distributed_orchestration_only": True,
    }


def build_cluster_runners_payload(state: dict[str, Any]) -> dict[str, Any]:
    rows = [r for r in (state.get("runners") or {}).values() if isinstance(r, dict)]
    rows.sort(key=lambda x: str(x.get("runner_id", "")))
    return {
        "runners": rows,
        "count": len(rows),
        "decision_layer_only": True,
        "distributed_orchestration_only": True,
    }
