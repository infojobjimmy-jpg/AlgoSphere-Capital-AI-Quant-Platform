"""
Production Hardening Layer: stability, fault tolerance, and persistence helpers.
No trading, broker execution, or capital deployment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .database import get_alert_engine_state, get_connection, set_alert_engine_state

HEALTH_HEALTHY = "HEALTHY"
HEALTH_DEGRADED = "DEGRADED"
HEALTH_CRITICAL = "CRITICAL"

KEY_HEALTH_STATE = "production_health_state"
KEY_SNAPSHOT_STATE = "production_snapshot_state"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _default_health_state() -> dict[str, Any]:
    return {
        "last_errors": [],
        "error_count": 0,
        "last_error": None,
        "last_snapshot_at": None,
        "last_recovery_at": None,
        "auto_restarts": [],
    }


def load_health_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, KEY_HEALTH_STATE)
    if not raw:
        return _default_health_state()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _default_health_state()
    if not isinstance(data, dict):
        return _default_health_state()
    out = _default_health_state()
    out.update(data)
    if not isinstance(out.get("last_errors"), list):
        out["last_errors"] = []
    if not isinstance(out.get("auto_restarts"), list):
        out["auto_restarts"] = []
    return out


def save_health_state(state: dict[str, Any]) -> None:
    payload = json.dumps(state, separators=(",", ":"))
    with get_connection() as conn:
        set_alert_engine_state(conn, KEY_HEALTH_STATE, payload)


def load_snapshot_state() -> dict[str, Any] | None:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, KEY_SNAPSHOT_STATE)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def save_snapshot_state(snapshot: dict[str, Any]) -> None:
    payload = json.dumps(snapshot, separators=(",", ":"))
    with get_connection() as conn:
        set_alert_engine_state(conn, KEY_SNAPSHOT_STATE, payload)


def record_error(
    health_state: dict[str, Any],
    *,
    source: str,
    message: str,
) -> dict[str, Any]:
    row = {"timestamp": _now_iso(), "source": str(source), "message": str(message)}
    errs = health_state.setdefault("last_errors", [])
    errs.append(row)
    health_state["last_errors"] = errs[-60:]
    health_state["error_count"] = int(health_state.get("error_count", 0) or 0) + 1
    health_state["last_error"] = row
    return health_state


def build_engine_health_map(
    *,
    autonomous_status: dict[str, Any],
    meta_status: dict[str, Any],
    evolution_status: dict[str, Any],
    runner_status: dict[str, Any],
    risk_status: dict[str, Any],
    regime_status: dict[str, Any],
    memory_status: dict[str, Any],
    distributed_cluster_status: dict[str, Any] | None = None,
) -> dict[str, str]:
    out: dict[str, str] = {}
    a_state = str(autonomous_status.get("state", "") or "")
    a_errs = autonomous_status.get("errors") or []
    if a_state == "AUTONOMOUS_RUNNING":
        out["autonomous_engine"] = HEALTH_HEALTHY
    elif a_errs:
        out["autonomous_engine"] = HEALTH_DEGRADED
    else:
        out["autonomous_engine"] = HEALTH_HEALTHY

    conf = _f(meta_status.get("confidence"), 0.0)
    out["meta_ai_engine"] = HEALTH_HEALTHY if conf >= 0.45 else HEALTH_DEGRADED

    evo_state = str(evolution_status.get("state", "") or "")
    out["evolution_engine"] = HEALTH_HEALTHY if evo_state in {"EVOLUTION_RUNNING", "EVOLUTION_IDLE", "EVOLUTION_PAUSED"} else HEALTH_DEGRADED

    rc = int((runner_status.get("counts") or {}).get("RUNNER_ACTIVE", 0) or 0)
    out["runner_engine"] = HEALTH_HEALTHY if rc >= 0 else HEALTH_DEGRADED

    risk_level = str(risk_status.get("risk_level", "MODERATE") or "MODERATE")
    out["risk_engine"] = HEALTH_CRITICAL if risk_level == "CRITICAL" else HEALTH_HEALTHY

    reg = str(regime_status.get("current_regime", "") or "")
    out["regime_engine"] = HEALTH_HEALTHY if reg else HEALTH_DEGRADED

    m_health = str(memory_status.get("memory_health", "SEEDING") or "SEEDING")
    out["memory_engine"] = HEALTH_HEALTHY if m_health in {"GOOD", "LEARNING", "MATURE"} else HEALTH_DEGRADED
    if distributed_cluster_status is not None:
        ch = str(distributed_cluster_status.get("cluster_health", "") or "")
        if ch == HEALTH_CRITICAL:
            out["distributed_cluster_engine"] = HEALTH_CRITICAL
        elif ch == HEALTH_DEGRADED:
            out["distributed_cluster_engine"] = HEALTH_DEGRADED
        else:
            out["distributed_cluster_engine"] = HEALTH_HEALTHY
    return out


def classify_system_health(engines: dict[str, str]) -> str:
    vals = list(engines.values())
    if any(v == HEALTH_CRITICAL for v in vals):
        return HEALTH_CRITICAL
    if any(v == HEALTH_DEGRADED for v in vals):
        return HEALTH_DEGRADED
    return HEALTH_HEALTHY


def process_metrics() -> dict[str, float | None]:
    # Optional psutil usage; safe fallback when unavailable.
    try:
        import psutil  # type: ignore

        p = psutil.Process()
        rss_mb = p.memory_info().rss / (1024.0 * 1024.0)
        cpu_pct = p.cpu_percent(interval=0.0)
        return {"memory_usage": round(rss_mb, 2), "cpu_usage": round(float(cpu_pct), 2)}
    except Exception:
        return {"memory_usage": None, "cpu_usage": None}


def build_system_health_payload(
    *,
    uptime_sec: float,
    engines: dict[str, str],
    health_state: dict[str, Any],
) -> dict[str, Any]:
    metrics = process_metrics()
    return {
        "system_health": classify_system_health(engines),
        "engines": engines,
        "memory_usage": metrics.get("memory_usage"),
        "cpu_usage": metrics.get("cpu_usage"),
        "uptime": round(max(0.0, float(uptime_sec)), 2),
        "last_errors": (health_state.get("last_errors") or [])[-10:],
        "decision_layer_only": True,
        "stability_layer_only": True,
    }


def build_errors_payload(health_state: dict[str, Any]) -> dict[str, Any]:
    errs = health_state.get("last_errors") or []
    return {
        "recent_errors": errs[-30:],
        "error_count": int(health_state.get("error_count", 0) or 0),
        "last_error": health_state.get("last_error"),
        "decision_layer_only": True,
        "stability_layer_only": True,
    }


def build_snapshot_payload(
    *,
    memory_state: dict[str, Any],
    portfolio_state: dict[str, Any],
    meta_state: dict[str, Any],
    evolution_state: dict[str, Any],
    autonomous_state: dict[str, Any],
) -> dict[str, Any]:
    return {
        "snapshot_at": _now_iso(),
        "memory_state": memory_state,
        "portfolio_state": portfolio_state,
        "meta_state": meta_state,
        "evolution_state": evolution_state,
        "autonomous_state": autonomous_state,
        "decision_layer_only": True,
        "stability_layer_only": True,
    }
