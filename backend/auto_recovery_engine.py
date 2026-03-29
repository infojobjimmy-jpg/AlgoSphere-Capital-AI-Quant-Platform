"""
Auto Recovery Engine: safe self-healing from alerts only.
No broker, no live trading, no capital deployment — orchestration / state resets only.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

RECOVERY_IDLE = "RECOVERY_IDLE"
RECOVERY_RUNNING = "RECOVERY_RUNNING"
RECOVERY_SUCCESS = "RECOVERY_SUCCESS"
RECOVERY_FAILED = "RECOVERY_FAILED"

RUNNER_FAILED = "RUNNER_FAILED"
RUNNER_STUCK = "RUNNER_STUCK"
NO_RUNNER_JOBS = "NO_RUNNER_JOBS"
AUTO_LOOP_ERROR = "AUTO_LOOP_ERROR"
CAPITAL_FULL = "CAPITAL_FULL"
PIPELINE_STALLED = "PIPELINE_STALLED"
NO_PAPER_SUCCESS = "NO_PAPER_SUCCESS"

_STATE_PREFIX = "auto_recovery_"

RULE_CODE_TO_TRIGGER: dict[str, str] = {
    "runner_has_failures": RUNNER_FAILED,
    "runner_stale_no_jobs": NO_RUNNER_JOBS,
    "runner_no_eligible_with_executor_ready": NO_RUNNER_JOBS,
    "auto_loop_error": AUTO_LOOP_ERROR,
    "capital_fully_allocated": CAPITAL_FULL,
    "paper_no_success": NO_PAPER_SUCCESS,
    "pipeline_candidate_overflow": PIPELINE_STALLED,
}

RecoveryHandlers = dict[str, Callable[[], dict[str, Any]]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def triggers_from_alerts(alerts: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for a in alerts:
        if not a.get("active", True):
            continue
        rc = str(a.get("rule_code", ""))
        t = RULE_CODE_TO_TRIGGER.get(rc)
        if t:
            out.add(t)
    return out


def triggers_from_stuck_runners(
    jobs: list[dict[str, Any]],
    *,
    max_age_sec: float = 3600.0,
) -> bool:
    now = datetime.now(timezone.utc)
    for j in jobs:
        if str(j.get("runner_status", "")) != "RUNNER_ACTIVE":
            continue
        ts = j.get("runner_started_at")
        if not ts:
            continue
        try:
            raw = str(ts).replace("Z", "+00:00")
            t = datetime.fromisoformat(raw)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if (now - t).total_seconds() >= max_age_sec:
                return True
        except ValueError:
            continue
    return False


def persist_running(conn: Any) -> None:
    from .database import set_alert_engine_state

    set_alert_engine_state(conn, f"{_STATE_PREFIX}state", RECOVERY_RUNNING)
    set_alert_engine_state(conn, f"{_STATE_PREFIX}active", "1")
    set_alert_engine_state(conn, f"{_STATE_PREFIX}started_at", _now_iso())
    conn.commit()


def persist_finished(
    conn: Any,
    *,
    success: bool,
    last_action: str,
    last_result: str,
    detail: dict[str, Any] | None = None,
) -> None:
    from .database import get_alert_engine_state, set_alert_engine_state

    state = RECOVERY_SUCCESS if success else RECOVERY_FAILED
    set_alert_engine_state(conn, f"{_STATE_PREFIX}state", state)
    set_alert_engine_state(conn, f"{_STATE_PREFIX}active", "0")
    set_alert_engine_state(conn, f"{_STATE_PREFIX}last_recovery", _now_iso())
    set_alert_engine_state(conn, f"{_STATE_PREFIX}last_action", last_action)
    set_alert_engine_state(conn, f"{_STATE_PREFIX}last_result", last_result[:2000])
    if detail is not None:
        entry = {
            "at": _now_iso(),
            "recovery_state": state,
            "last_action": last_action,
            "last_result": last_result[:500],
            "detail_keys": list(detail.keys()),
        }
        raw = get_alert_engine_state(conn, f"{_STATE_PREFIX}history") or "[]"
        try:
            hist = json.loads(raw)
        except json.JSONDecodeError:
            hist = []
        if not isinstance(hist, list):
            hist = []
        hist.insert(0, entry)
        set_alert_engine_state(conn, f"{_STATE_PREFIX}history", json.dumps(hist[:20]))
    conn.commit()


def load_recovery_status(conn: Any) -> dict[str, Any]:
    from .database import get_alert_engine_state

    def g(key: str, default: str = "") -> str:
        return get_alert_engine_state(conn, f"{_STATE_PREFIX}{key}") or default

    state = g("state", RECOVERY_IDLE) or RECOVERY_IDLE
    active_s = g("active", "0")
    raw_hist = g("history", "[]")
    try:
        history = json.loads(raw_hist) if raw_hist else []
    except json.JSONDecodeError:
        history = []
    if not isinstance(history, list):
        history = []

    return {
        "recovery_state": state,
        "last_recovery": g("last_recovery") or None,
        "last_action": g("last_action") or None,
        "last_result": g("last_result") or None,
        "active": active_s == "1",
        "recovery_history": history,
    }


def execute_recovery(handlers: RecoveryHandlers) -> dict[str, Any]:
    """
    Scan active alerts + runner jobs, then run safe recovery handlers.
    Handlers must only perform allowed orchestration (no broker/capital).
    """
    alerts = handlers["get_active_alerts"]()
    jobs = handlers["get_runner_jobs"]()
    triggers = triggers_from_alerts(alerts)
    if triggers_from_stuck_runners(jobs):
        triggers.add(RUNNER_STUCK)

    action_log: list[dict[str, Any]] = []
    last_action = "none"
    last_result = "no_triggers"

    priority_order = [
        AUTO_LOOP_ERROR,
        RUNNER_FAILED,
        RUNNER_STUCK,
        NO_RUNNER_JOBS,
        PIPELINE_STALLED,
        NO_PAPER_SUCCESS,
        CAPITAL_FULL,
    ]

    for trig in priority_order:
        if trig not in triggers:
            continue
        fn = handlers.get(f"recover_{trig}")
        if fn is None:
            continue
        last_action = f"recover_{trig}"
        out = fn()
        action_log.append({"trigger": trig, "result": out})
        last_result = "ok" if out.get("ok", True) else "partial_or_failed"

    if not action_log:
        last_action = "scan_only"
        last_result = "no_triggers"

    return {
        "ok": True,
        "triggers": sorted(triggers),
        "actions": action_log,
        "last_action": last_action,
        "last_result": last_result,
    }
