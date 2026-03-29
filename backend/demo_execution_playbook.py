"""
FP Markets Demo Execution Playbook (demo-only, safety-first).
No live trading and no real capital deployment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .database import get_alert_engine_state, get_connection, set_alert_engine_state

STATE_KEY = "demo_execution_playbook_state"

PLAYBOOK_IDLE = "PLAYBOOK_IDLE"
PLAYBOOK_SMOKE_TEST = "PLAYBOOK_SMOKE_TEST"
PLAYBOOK_CONTROLLED = "PLAYBOOK_CONTROLLED"
PLAYBOOK_SCALED = "PLAYBOOK_SCALED"
PLAYBOOK_COMPLETE = "PLAYBOOK_COMPLETE"

PHASE_SMOKE_TEST = "SMOKE_TEST"
PHASE_CONTROLLED_DEMO = "CONTROLLED_DEMO"
PHASE_SCALED_DEMO = "SCALED_DEMO"
PHASE_COMPLETE = "COMPLETE"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_state() -> dict[str, Any]:
    return {
        "state": PLAYBOOK_IDLE,
        "phase": PHASE_SMOKE_TEST,
        "started_at": None,
        "updated_at": None,
        "history": [],
    }


def load_playbook_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, STATE_KEY)
    if not raw:
        return _default_state()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _default_state()
    if not isinstance(data, dict):
        return _default_state()
    st = _default_state()
    st.update(data)
    if not isinstance(st.get("history"), list):
        st["history"] = []
    return st


def save_playbook_state(state: dict[str, Any]) -> None:
    with get_connection() as conn:
        set_alert_engine_state(conn, STATE_KEY, json.dumps(state))


def _runner_failures(runner_status: dict[str, Any]) -> int:
    counts = runner_status.get("counts") if isinstance(runner_status, dict) else {}
    if not isinstance(counts, dict):
        return 0
    return int(counts.get("RUNNER_FAILED", 0) or 0)


def _cluster_unstable(cluster_status: dict[str, Any]) -> bool:
    if not isinstance(cluster_status, dict):
        return True
    if str(cluster_status.get("cluster_health", "")).upper() not in {"HEALTHY", "STABLE"}:
        return True
    offline = int(cluster_status.get("offline_runners", 0) or 0)
    total = int(cluster_status.get("runner_count", 0) or 0)
    if total > 0 and offline > max(0, int(total * 0.34)):
        return True
    return False


def evaluate_readiness_checks(
    *,
    system_health: dict[str, Any],
    risk_status: dict[str, Any],
    meta_status: dict[str, Any],
    runner_status: dict[str, Any],
    cluster_status: dict[str, Any],
    autonomous_status: dict[str, Any],
) -> dict[str, Any]:
    sh = str(system_health.get("system_health", "")).upper()
    rl = str(risk_status.get("risk_level", "")).upper()
    posture = str(meta_status.get("system_posture", "")).upper()
    rf = _runner_failures(runner_status)
    unstable = _cluster_unstable(cluster_status)
    auto_state = str(autonomous_status.get("state", "")).upper()

    checks = {
        "system_health_ok": sh in {"GOOD", "HEALTHY"},
        "risk_level_ok": rl in {"LOW", "MODERATE"},
        "meta_posture_ok": posture not in {"CHAOTIC_SAFE_MODE"},
        "runner_health_ok": rf == 0,
        "cluster_health_ok": not unstable,
        "autonomous_ok": auto_state not in {"AUTONOMOUS_ERROR"},
    }

    blocking: list[str] = []
    if not checks["risk_level_ok"]:
        blocking.append("Risk level is HIGH/CRITICAL.")
    if not checks["system_health_ok"]:
        blocking.append("System health is DEGRADED/CRITICAL.")
    if not checks["runner_health_ok"]:
        blocking.append("Runner failures detected.")
    if not checks["cluster_health_ok"]:
        blocking.append("Cluster is unstable.")
    if not checks["meta_posture_ok"]:
        blocking.append("Meta posture is chaotic-safe.")
    if not checks["autonomous_ok"]:
        blocking.append("Autonomous engine reports error state.")

    readiness = "READY" if not blocking else "BLOCKED"
    recs: list[str] = []
    if blocking:
        recs.append("Hold phase progression and resolve blockers before scaling demo execution.")
        recs.extend(blocking[:4])
    else:
        recs.append("Proceed with demo-only gradual scaling.")
    recs.append("Demo accounts only. No live trading and no real capital.")

    return {
        "readiness": readiness,
        "checks": checks,
        "blocking_conditions": blocking,
        "recommendations": recs[:8],
    }


def build_playbook_status_payload(
    *,
    state: dict[str, Any],
    system_health: dict[str, Any],
    risk_status: dict[str, Any],
    meta_status: dict[str, Any],
    runner_status: dict[str, Any],
    cluster_status: dict[str, Any],
    autonomous_status: dict[str, Any],
) -> dict[str, Any]:
    evald = evaluate_readiness_checks(
        system_health=system_health,
        risk_status=risk_status,
        meta_status=meta_status,
        runner_status=runner_status,
        cluster_status=cluster_status,
        autonomous_status=autonomous_status,
    )
    return {
        "state": state.get("state", PLAYBOOK_IDLE),
        "phase": state.get("phase", PHASE_SMOKE_TEST),
        "readiness": evald["readiness"],
        "checks": evald["checks"],
        "blocking_conditions": evald["blocking_conditions"],
        "recommendations": evald["recommendations"],
        "phase_profiles": {
            PHASE_SMOKE_TEST: {"runners": 1, "strategies": 1, "symbols": 1, "frequency": "very_low"},
            PHASE_CONTROLLED_DEMO: {"runners": "1-2", "strategies": "2-3", "symbols": "1-2", "allocation": "limited"},
            PHASE_SCALED_DEMO: {"runners": "multiple", "strategies": "multiple", "symbols": "multiple"},
        },
        "history": list(state.get("history") or [])[-12:],
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "demo_only": True,
    }


def build_phase_gate_checklist(
    playbook_state: dict[str, Any],
    system_status: dict[str, Any],
    meta_status: dict[str, Any],
    risk_status: dict[str, Any],
    cluster_status: dict[str, Any],
    autonomous_status: dict[str, Any],
) -> dict[str, Any]:
    """Minimal phase-gate checklist payload reusing readiness evaluation logic."""
    evald = evaluate_readiness_checks(
        system_health=system_status,
        risk_status=risk_status,
        meta_status=meta_status,
        runner_status=system_status.get("runner_status") if isinstance(system_status.get("runner_status"), dict) else {},
        cluster_status=cluster_status,
        autonomous_status=autonomous_status,
    )
    return {
        "phase": playbook_state.get("phase", PHASE_SMOKE_TEST),
        "readiness": evald["readiness"],
        "checks": evald["checks"],
        "blocking_conditions": evald["blocking_conditions"],
        "recommendations": evald["recommendations"],
        "ready_to_advance": evald["readiness"] == "READY",
        "demo_only": True,
    }


def start_playbook(state: dict[str, Any]) -> dict[str, Any]:
    st = dict(state)
    st["state"] = PLAYBOOK_SMOKE_TEST
    st["phase"] = PHASE_SMOKE_TEST
    st["started_at"] = st.get("started_at") or _now_iso()
    st["updated_at"] = _now_iso()
    hist = list(st.get("history") or [])
    hist.append({"at": st["updated_at"], "action": "start", "phase": st["phase"], "state": st["state"]})
    st["history"] = hist[-30:]
    return st


def reset_playbook() -> dict[str, Any]:
    st = _default_state()
    st["updated_at"] = _now_iso()
    st["history"] = [{"at": st["updated_at"], "action": "reset", "phase": st["phase"], "state": st["state"]}]
    return st


def next_playbook_phase(state: dict[str, Any], readiness: str) -> tuple[dict[str, Any], bool, str]:
    st = dict(state)
    if readiness != "READY":
        return st, False, "blocked_by_readiness_checks"
    phase = str(st.get("phase", PHASE_SMOKE_TEST))
    if phase == PHASE_SMOKE_TEST:
        st["phase"] = PHASE_CONTROLLED_DEMO
        st["state"] = PLAYBOOK_CONTROLLED
    elif phase == PHASE_CONTROLLED_DEMO:
        st["phase"] = PHASE_SCALED_DEMO
        st["state"] = PLAYBOOK_SCALED
    elif phase == PHASE_SCALED_DEMO:
        st["phase"] = PHASE_COMPLETE
        st["state"] = PLAYBOOK_COMPLETE
    else:
        return st, False, "already_complete"
    st["updated_at"] = _now_iso()
    hist = list(st.get("history") or [])
    hist.append({"at": st["updated_at"], "action": "next", "phase": st["phase"], "state": st["state"]})
    st["history"] = hist[-30:]
    return st, True, "advanced"
