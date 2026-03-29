"""
Live Control Engine: periodic, in-memory recomputation of bot control signals.
Decision layer only. No broker execution or order sending.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from .control_engine import recommended_action_for_state
from .database import get_alert_engine_state, get_connection, set_alert_engine_state

STATE_KEY = "live_control_engine_state"
DEFAULT_INTERVAL_SEC = 12

STATE_KILL = "KILL"
STATE_REDUCE = "REDUCE"
STATE_MONITOR = "MONITOR"
STATE_BOOST = "BOOST"

_STATE_ALIAS = {
    STATE_KILL: "kill",
    STATE_REDUCE: "defensive",
    STATE_MONITOR: "normal",
    STATE_BOOST: "aggressive",
}

_VOLUME_FLOOR = 1000
_VOLUME_CAP = 10000
_VOLUME_STEP = 250


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _round_step(v: float, step: int = _VOLUME_STEP) -> int:
    if v <= 0:
        return 0
    rounded = int(round(v / float(step)) * step)
    return max(0, rounded)


def _quality_score(bot: dict[str, Any]) -> float:
    score = _f(bot.get("score"), 0.0)
    win_rate = _f(bot.get("win_rate"), 0.0)
    drawdown = _f(bot.get("drawdown"), 0.0)
    score_n = max(0.0, min(1.0, score))
    win_n = max(0.0, min(1.0, win_rate))
    dd_n = max(0.0, min(1.0, 1.0 - (drawdown / 400.0)))
    return max(0.0, min(1.0, 0.45 * score_n + 0.35 * win_n + 0.20 * dd_n))


def _risk_multiplier(risk_level: str, global_risk_score: float) -> float:
    level_mult = {
        "CRITICAL": 0.45,
        "HIGH": 0.70,
        "MODERATE": 0.92,
        "LOW": 1.05,
    }.get(risk_level.upper(), 0.90)
    score_mult = max(0.55, min(1.10, 1.08 - max(0.0, min(1.0, global_risk_score)) * 0.45))
    return round(level_mult * score_mult, 4)


def _regime_multiplier(regime: str, risk_level: str, bot: dict[str, Any]) -> float:
    reg = regime.upper()
    if reg == "CHAOTIC":
        return 0.55
    if reg == "TRANSITIONAL":
        return 0.90
    if reg == "RANGING":
        return 0.98
    if reg == "TRENDING":
        return 1.12
    if reg == "VOLATILE":
        fam = str(bot.get("family", "") or bot.get("strategy_family", "")).upper()
        if fam in {"SESSION_BREAKOUT", "BREAKOUT"} and risk_level.upper() in {"LOW", "MODERATE"}:
            return 1.03
        return 0.88
    return 0.95


def _health_multiplier(system_health: str, cluster_health: str, live_health: str) -> float:
    mult = 1.0
    if system_health.upper() != "HEALTHY":
        mult *= 0.88
    if cluster_health.upper() != "HEALTHY":
        mult *= 0.92
    if live_health.upper() != "LIVE_HEALTHY":
        mult *= 0.90
    return round(max(0.6, min(1.05, mult)), 4)


def _volume_for_state(base: float, state: str, entries_enabled: bool, boost_level: int = 0) -> int:
    if not entries_enabled:
        return 0
    mult = {
        STATE_KILL: 0.0,
        STATE_REDUCE: 0.60,
        STATE_MONITOR: 1.00,
        STATE_BOOST: 1.04 + min(3, max(0, int(boost_level))) * 0.08,
    }.get(state, 1.0)
    raw = max(_VOLUME_FLOOR, min(_VOLUME_CAP, int(base))) * mult
    return max(0, min(_VOLUME_CAP, _round_step(raw)))


def _effective_capital_for_state(base: float, state: str, boost_level: int = 0) -> float:
    mult = {
        STATE_KILL: 0.0,
        STATE_REDUCE: 0.60,
        STATE_MONITOR: 1.00,
        STATE_BOOST: 1.02 + min(3, max(0, int(boost_level))) * 0.07,
    }.get(state, 1.0)
    return round(max(0.0, base * mult), 2)


def _derive_state(bot: dict[str, Any], ctx: dict[str, Any]) -> tuple[str, list[str], int]:
    reasons: list[str] = []
    risk = str((ctx.get("risk_status") or {}).get("risk_level", "MODERATE")).upper()
    global_risk_score = _f((ctx.get("risk_status") or {}).get("global_risk_score"), 0.5)
    posture = str((ctx.get("meta_status") or {}).get("system_posture", "BALANCED")).upper()
    regime = str((ctx.get("regime_status") or {}).get("current_regime", "TRANSITIONAL")).upper()
    regime_conf = _f((ctx.get("regime_status") or {}).get("confidence_score"), 0.0)
    sys_h = str((ctx.get("system_health") or {}).get("system_health", "DEGRADED")).upper()
    cluster_h = str((ctx.get("cluster_status") or {}).get("cluster_health", "DEGRADED")).upper()
    live_h = str((ctx.get("live_data_context") or {}).get("data_health", "LIVE_OFFLINE")).upper()
    quality = _quality_score(bot)

    if sys_h == "CRITICAL":
        reasons.append("System health critical")
        return STATE_KILL, reasons, 0
    if cluster_h == "CRITICAL":
        reasons.append("Cluster health critical")
        return STATE_KILL, reasons, 0
    if risk == "CRITICAL":
        reasons.append("Risk level critical")
        return STATE_KILL, reasons, 0
    if risk == "HIGH":
        reasons.append("Risk level high")
        return STATE_REDUCE, reasons, 0
    if cluster_h == "DEGRADED":
        reasons.append("Cluster degraded")
        return STATE_REDUCE, reasons, 0
    if regime == "CHAOTIC":
        reasons.append("Regime chaotic")
        return STATE_REDUCE, reasons, 0
    if posture in {"CHAOTIC_SAFE_MODE", "DEFENSIVE", "CAPITAL_PRESERVATION"}:
        reasons.append(f"Meta posture {posture.lower()}")
        return STATE_REDUCE, reasons, 0
    if live_h == "LIVE_OFFLINE":
        reasons.append("Live data offline")
        return STATE_REDUCE, reasons, 0

    if posture in {"AGGRESSIVE", "BALANCED"} and risk in {"LOW", "MODERATE"} and sys_h == "HEALTHY" and cluster_h == "HEALTHY":
        if regime in {"TRENDING", "RANGING", "TRANSITIONAL"} and quality >= 0.62 and global_risk_score <= 0.48:
            boost_level = 1
            if quality >= 0.72 and regime_conf >= 0.44 and global_risk_score <= 0.40 and posture == "AGGRESSIVE":
                boost_level = 2
            if quality >= 0.82 and regime == "TRENDING" and regime_conf >= 0.56 and global_risk_score <= 0.32 and posture == "AGGRESSIVE":
                boost_level = 3
            reasons.append(f"Aligned context for progressive boost L{boost_level}")
            return STATE_BOOST, reasons, boost_level

    reasons.append("Balanced safety posture")
    return STATE_MONITOR, reasons, 0


def compute_live_bot_signal(bot: dict[str, Any], ctx: dict[str, Any], updated_at: str) -> dict[str, Any]:
    state, reasoning, boost_level = _derive_state(bot, ctx)
    base_cap = _f(bot.get("effective_capital"), 0.0)
    if base_cap <= 0:
        base_cap = _f(bot.get("capital_alloc"), 0.0)
    if base_cap <= 0:
        base_cap = 1000.0

    risk_status = ctx.get("risk_status") or {}
    regime_status = ctx.get("regime_status") or {}
    live_ctx = ctx.get("live_data_context") or {}
    system_h = str((ctx.get("system_health") or {}).get("system_health", "DEGRADED"))
    cluster_h = str((ctx.get("cluster_status") or {}).get("cluster_health", "DEGRADED"))
    risk_level = str(risk_status.get("risk_level", "MODERATE"))
    global_risk_score = _f(risk_status.get("global_risk_score"), 0.5)
    regime = str(regime_status.get("current_regime", "TRANSITIONAL"))
    regime_conf = _f(regime_status.get("confidence_score"), 0.0)
    quality = _quality_score(bot)
    regime_mult = _regime_multiplier(regime, risk_level, bot)
    risk_mult = _risk_multiplier(risk_level, global_risk_score)
    health_mult = _health_multiplier(system_h, cluster_h, str(live_ctx.get("data_health", "LIVE_OFFLINE")))
    quality_mult = round(0.82 + quality * 0.30, 4)

    control_active = state != STATE_KILL
    entries_enabled = control_active
    if state == STATE_KILL:
        entries_enabled = False
    scaled_base = base_cap * regime_mult * risk_mult * health_mult * quality_mult
    eff = _effective_capital_for_state(scaled_base, state, boost_level=boost_level)
    target_volume = _volume_for_state(base=eff, state=state, entries_enabled=entries_enabled, boost_level=boost_level)
    control_level = state if state != STATE_BOOST else f"BOOST_L{max(1, boost_level)}"
    return {
        "name": str(bot.get("name", "")),
        "control_state": state,
        "control_level": control_level,
        "boost_level": int(boost_level),
        "state_alias": _STATE_ALIAS.get(state, "normal"),
        "recommended_action": recommended_action_for_state(state),
        "control_active": control_active,
        "entriesEnabled": entries_enabled,
        "effective_capital": eff,
        "target_volume": target_volume,
        "regime_multiplier": round(regime_mult, 4),
        "risk_multiplier": round(risk_mult, 4),
        "quality_score": round(quality, 4),
        "regime_confidence": round(regime_conf, 4),
        "global_risk_score": round(global_risk_score, 4),
        "reasoning": reasoning[:6],
        "updated_at": updated_at,
    }


def build_live_control_payload(ctx: dict[str, Any], interval_sec: int) -> dict[str, Any]:
    bots = list(ctx.get("bots") or [])
    ts = _now_iso()
    signals = [compute_live_bot_signal(b, ctx, ts) for b in bots]
    return {
        "count": len(signals),
        "signals": signals,
        "live_engine": True,
        "updated_at": ts,
        "interval_sec": int(interval_sec),
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


class LiveControlEngine:
    def __init__(
        self,
        *,
        context_provider: Callable[[], dict[str, Any]],
        interval_sec: int = DEFAULT_INTERVAL_SEC,
        persist_state: bool = True,
    ) -> None:
        self._context_provider = context_provider
        self._interval_sec = max(5, int(interval_sec))
        self._persist_state = persist_state
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._payload: dict[str, Any] = {"count": 0, "signals": [], "live_engine": True, "updated_at": None}
        self._last_error: str | None = None

    def _persist(self, payload: dict[str, Any]) -> None:
        if not self._persist_state:
            return
        try:
            with get_connection() as conn:
                set_alert_engine_state(conn, STATE_KEY, json.dumps(payload))
        except Exception:
            pass

    def _load_persisted(self) -> dict[str, Any] | None:
        try:
            with get_connection() as conn:
                raw = get_alert_engine_state(conn, STATE_KEY)
            if not raw:
                return None
            data = json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def recompute_once(self) -> dict[str, Any]:
        ctx = self._context_provider()
        payload = build_live_control_payload(ctx, interval_sec=self._interval_sec)
        with self._lock:
            self._payload = payload
            self._last_error = None
        self._persist(payload)
        return payload

    def get_payload(self) -> dict[str, Any]:
        with self._lock:
            cur = dict(self._payload)
            if cur.get("signals"):
                return cur
        persisted = self._load_persisted()
        if persisted:
            return persisted
        return cur

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": bool(self._thread and self._thread.is_alive()),
                "interval_sec": self._interval_sec,
                "updated_at": self._payload.get("updated_at"),
                "count": int(self._payload.get("count", 0) or 0),
                "last_error": self._last_error,
                "live_engine": True,
            }

    def _loop(self) -> None:
        while not self._stop.wait(timeout=float(self._interval_sec)):
            try:
                self.recompute_once()
            except Exception as exc:
                with self._lock:
                    self._last_error = str(exc)

    def start(self) -> dict[str, Any]:
        if self._thread is not None and self._thread.is_alive():
            return {"started": False, "reason": "already_running", "status": self.status()}
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="live_control_engine", daemon=True)
        self._thread.start()
        try:
            self.recompute_once()
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)
        return {"started": True, "status": self.status()}

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        return {"stopped": True, "status": self.status()}
