"""
Full Autonomous Fund Mode engine.
Safe autonomous research orchestration only — no trading, broker execution, or capital deployment.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable

AUTONOMOUS_IDLE = "AUTONOMOUS_IDLE"
AUTONOMOUS_RUNNING = "AUTONOMOUS_RUNNING"
AUTONOMOUS_PAUSED = "AUTONOMOUS_PAUSED"


class AutonomousFundEngine:
    def __init__(
        self,
        *,
        regime_status_fn: Callable[[], dict[str, Any]],
        risk_status_fn: Callable[[], dict[str, Any]],
        memory_update_fn: Callable[[], dict[str, Any]],
        evolution_run_fn: Callable[..., dict[str, Any]],
        continuous_run_once_fn: Callable[[], dict[str, Any]],
        paper_deploy_fn: Callable[[int], dict[str, Any]],
        performance_system_fn: Callable[[], dict[str, Any]],
        portfolio_rotation_fn: Callable[[], dict[str, Any]],
        fund_rebalance_fn: Callable[[], dict[str, Any]],
        multi_runner_assign_fn: Callable[[], dict[str, Any]],
        meta_status_fn: Callable[[], dict[str, Any]],
    ) -> None:
        self._regime_status_fn = regime_status_fn
        self._risk_status_fn = risk_status_fn
        self._memory_update_fn = memory_update_fn
        self._evolution_run_fn = evolution_run_fn
        self._continuous_run_once_fn = continuous_run_once_fn
        self._paper_deploy_fn = paper_deploy_fn
        self._performance_system_fn = performance_system_fn
        self._portfolio_rotation_fn = portfolio_rotation_fn
        self._fund_rebalance_fn = fund_rebalance_fn
        self._multi_runner_assign_fn = multi_runner_assign_fn
        self._meta_status_fn = meta_status_fn

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop_timestamps: deque[float] = deque()

        self.state = AUTONOMOUS_IDLE
        self.interval_sec = 120
        self.max_loops_per_hour = 20
        self.loops_completed = 0
        self.last_loop_at: str | None = None
        self.last_decision: dict[str, Any] | None = None
        self.last_cycle_result: dict[str, Any] | None = None
        self.errors: list[str] = []

    def _trim_hour_window(self, now_ts: float) -> None:
        hour_ago = now_ts - 3600.0
        while self._loop_timestamps and self._loop_timestamps[0] < hour_ago:
            self._loop_timestamps.popleft()

    def _append_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.errors = self.errors[-20:]

    def _policy_from_posture(self, posture: str) -> dict[str, Any]:
        p = str(posture or "BALANCED")
        # Purely orchestration policy; no execution coupling.
        if p == "AGGRESSIVE":
            return {"max_weak": 8, "max_strong": 8, "paper_max_bots": 12, "run_evolution": True, "run_continuous": True}
        if p == "BALANCED":
            return {"max_weak": 5, "max_strong": 5, "paper_max_bots": 8, "run_evolution": True, "run_continuous": True}
        if p == "DEFENSIVE":
            return {"max_weak": 2, "max_strong": 2, "paper_max_bots": 4, "run_evolution": True, "run_continuous": True}
        if p == "CAPITAL_PRESERVATION":
            return {"max_weak": 0, "max_strong": 0, "paper_max_bots": 2, "run_evolution": False, "run_continuous": False}
        if p == "CHAOTIC_SAFE_MODE":
            return {"max_weak": 0, "max_strong": 0, "paper_max_bots": 0, "run_evolution": False, "run_continuous": False}
        return {"max_weak": 5, "max_strong": 5, "paper_max_bots": 8, "run_evolution": True, "run_continuous": True}

    def run_cycle(self) -> dict[str, Any]:
        ts = datetime.now(timezone.utc).isoformat()
        cycle: dict[str, Any] = {"timestamp": ts}
        try:
            cycle["regime"] = self._regime_status_fn()
            cycle["risk"] = self._risk_status_fn()
            cycle["memory_update"] = self._memory_update_fn()
            cycle["meta"] = self._meta_status_fn()

            posture = str((cycle["meta"] or {}).get("system_posture", "BALANCED"))
            confidence = float((cycle["meta"] or {}).get("confidence", 0.0) or 0.0)
            policy = self._policy_from_posture(posture)

            if policy["run_evolution"]:
                cycle["evolution"] = self._evolution_run_fn(
                    seed=None,
                    max_weak=int(policy["max_weak"]),
                    max_strong=int(policy["max_strong"]),
                )
            else:
                cycle["evolution"] = {"skipped": True, "reason": f"{posture}: evolution paused by autonomous policy."}

            if policy["run_continuous"]:
                cycle["continuous_evolution"] = self._continuous_run_once_fn()
            else:
                cycle["continuous_evolution"] = {
                    "ok": True,
                    "skipped": True,
                    "reason": f"{posture}: continuous evolution paused by autonomous policy.",
                }

            max_bots = int(policy["paper_max_bots"])
            if max_bots > 0:
                cycle["paper_deploy"] = self._paper_deploy_fn(max_bots)
            else:
                cycle["paper_deploy"] = {"count": 0, "skipped": True, "reason": f"{posture}: paper deployment frozen."}

            cycle["performance"] = self._performance_system_fn()
            cycle["portfolio_rotation"] = self._portfolio_rotation_fn()
            cycle["fund_rebalance"] = self._fund_rebalance_fn()
            cycle["multi_runner_assign"] = self._multi_runner_assign_fn()

            decision = {
                "posture": posture,
                "confidence": confidence,
                "policy": policy,
                "summary": (
                    f"autonomous cycle posture={posture} confidence={confidence:.3f} "
                    f"evolution={'on' if policy['run_evolution'] else 'paused'} "
                    f"paper_max_bots={max_bots}"
                ),
            }
            cycle["meta_decision"] = decision

            with self._lock:
                self.loops_completed += 1
                self.last_loop_at = ts
                self.last_decision = decision
                self.last_cycle_result = cycle
            return {"ok": True, "cycle": cycle}
        except Exception as exc:  # pragma: no cover - defensive
            with self._lock:
                self._append_error(str(exc))
                self.last_loop_at = ts
                self.last_cycle_result = cycle
            return {"ok": False, "error": str(exc), "cycle": cycle}

    def _loop_runner(self) -> None:
        while not self._stop_event.is_set():
            now_ts = time.time()
            with self._lock:
                self._trim_hour_window(now_ts)
                if len(self._loop_timestamps) >= self.max_loops_per_hour:
                    self._append_error(f"max loops per hour reached ({self.max_loops_per_hour})")
                    self.state = AUTONOMOUS_PAUSED
                    break
            out = self.run_cycle()
            if not out.get("ok", False):
                with self._lock:
                    self.state = AUTONOMOUS_PAUSED
                break
            with self._lock:
                self._loop_timestamps.append(now_ts)
            if self._stop_event.wait(timeout=max(1, int(self.interval_sec))):
                break
        with self._lock:
            if self.state == AUTONOMOUS_RUNNING:
                self.state = AUTONOMOUS_IDLE

    def start(self, *, interval_sec: int = 120, max_loops_per_hour: int = 20) -> dict[str, Any]:
        with self._lock:
            if self.state == AUTONOMOUS_RUNNING:
                return {"started": False, "message": "already running", "state": self.state}
            self.interval_sec = max(10, int(interval_sec))
            self.max_loops_per_hour = max(1, int(max_loops_per_hour))
            self.state = AUTONOMOUS_RUNNING
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop_runner, daemon=True)
            self._thread.start()
        return {"started": True, "state": AUTONOMOUS_RUNNING}

    def pause(self) -> dict[str, Any]:
        self._stop_event.set()
        with self._lock:
            if self.state == AUTONOMOUS_RUNNING:
                self.state = AUTONOMOUS_PAUSED
        return {"paused": True, "state": self.state}

    def run_once(self) -> dict[str, Any]:
        return self.run_cycle()

    def status(self) -> dict[str, Any]:
        with self._lock:
            decision = self.last_decision or {}
            return {
                "state": self.state,
                "interval_sec": self.interval_sec,
                "max_loops_per_hour": self.max_loops_per_hour,
                "loops_completed": self.loops_completed,
                "last_loop_at": self.last_loop_at,
                "last_decision": decision.get("summary"),
                "confidence": float(decision.get("confidence", 0.0) or 0.0),
                "posture": decision.get("posture"),
                "errors": list(self.errors),
                "orchestration_only": True,
                "decision_layer_only": True,
                "demo_simulation_only": True,
            }
