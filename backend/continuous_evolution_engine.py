"""
Continuous Evolution Loop: periodic orchestration of AI evolution → factory → paper → feedback → performance snapshot.
Simulation / desk actions only — no broker, no trading, no capital deployment.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable

EVOLUTION_IDLE = "EVOLUTION_IDLE"
EVOLUTION_RUNNING = "EVOLUTION_RUNNING"
EVOLUTION_PAUSED = "EVOLUTION_PAUSED"


class ContinuousEvolutionLoopEngine:
    """
    Cycle (orchestration only):
      1) Run AI strategy evolution (insert variants into factory)
      2) Deploy paper bots (simulation)
      3) Apply paper feedback (lifecycle updates)
      4) Collect performance snapshot (read-only aggregation)
      → wait interval → repeat while RUNNING
    """

    def __init__(
        self,
        evolution_run_fn: Callable[[], dict[str, Any]],
        paper_deploy_fn: Callable[[], dict[str, Any]],
        feedback_fn: Callable[[], dict[str, Any]],
        performance_snapshot_fn: Callable[[], dict[str, Any]],
    ) -> None:
        self._evolution_run_fn = evolution_run_fn
        self._paper_deploy_fn = paper_deploy_fn
        self._feedback_fn = feedback_fn
        self._performance_snapshot_fn = performance_snapshot_fn

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        self._loop_timestamps: deque[float] = deque()
        self.state = EVOLUTION_IDLE
        self.interval_sec = 120
        self.max_loops_per_hour = 12
        self.max_weak = 5
        self.max_strong = 5
        self.loops_completed = 0
        self.last_cycle_at: str | None = None
        self.last_error: str | None = None
        self.last_cycle_result: dict[str, Any] | None = None

    def _trim_hour_window(self, now_ts: float) -> None:
        hour_ago = now_ts - 3600.0
        while self._loop_timestamps and self._loop_timestamps[0] < hour_ago:
            self._loop_timestamps.popleft()

    def run_cycle(self) -> dict[str, Any]:
        """Execute one full cycle (safe to call from run_once or the background thread)."""
        ts = datetime.now(timezone.utc).isoformat()
        evolution = self._evolution_run_fn()
        paper = self._paper_deploy_fn()
        feedback = self._feedback_fn()
        performance = self._performance_snapshot_fn()
        cycle: dict[str, Any] = {
            "evolution": evolution,
            "paper": paper,
            "feedback": feedback,
            "performance": performance,
            "timestamp": ts,
        }
        with self._lock:
            self.loops_completed += 1
            self.last_cycle_at = ts
            self.last_cycle_result = cycle
            self.last_error = None
        return {"ok": True, "cycle": cycle}

    def _loop_runner(self) -> None:
        while not self._stop_event.is_set():
            try:
                now_ts = time.time()
                with self._lock:
                    self._trim_hour_window(now_ts)
                    if len(self._loop_timestamps) >= self.max_loops_per_hour:
                        self.last_error = (
                            f"max loops per hour reached ({self.max_loops_per_hour})"
                        )
                        self.state = EVOLUTION_PAUSED
                        break
                self.run_cycle()
                with self._lock:
                    self._loop_timestamps.append(now_ts)
            except Exception as exc:  # pragma: no cover - defensive
                with self._lock:
                    self.last_error = str(exc)
                    self.state = EVOLUTION_IDLE
                break
            if self._stop_event.wait(timeout=max(1, int(self.interval_sec))):
                break
        with self._lock:
            # Pause or error already set state; only normalize unexpected RUNNING exit
            if self.state == EVOLUTION_RUNNING:
                self.state = EVOLUTION_IDLE

    def get_cycle_params(self) -> tuple[int, int]:
        with self._lock:
            return self.max_weak, self.max_strong

    def start(
        self,
        *,
        interval_sec: int = 120,
        max_loops_per_hour: int = 12,
        max_weak: int = 5,
        max_strong: int = 5,
    ) -> dict[str, Any]:
        with self._lock:
            if self.state == EVOLUTION_RUNNING:
                return {"started": False, "message": "already running", "state": self.state}
            self.interval_sec = max(10, int(interval_sec))
            self.max_loops_per_hour = max(1, int(max_loops_per_hour))
            self.max_weak = max(0, int(max_weak))
            self.max_strong = max(0, int(max_strong))
            self.last_error = None
            self._stop_event.clear()
            self.state = EVOLUTION_RUNNING
            self._thread = threading.Thread(target=self._loop_runner, daemon=True)
            self._thread.start()
        return {"started": True, "state": EVOLUTION_RUNNING}

    def pause(self) -> dict[str, Any]:
        self._stop_event.set()
        with self._lock:
            if self.state == EVOLUTION_RUNNING:
                self.state = EVOLUTION_PAUSED
            elif self.state == EVOLUTION_IDLE:
                pass
        return {"paused": True, "state": self.state}

    def run_once(self) -> dict[str, Any]:
        """Single cycle without starting the background loop."""
        try:
            return self.run_cycle()
        except Exception as exc:
            with self._lock:
                self.last_error = str(exc)
            return {"ok": False, "error": str(exc)}

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self.state,
                "interval_sec": self.interval_sec,
                "max_loops_per_hour": self.max_loops_per_hour,
                "max_weak": self.max_weak,
                "max_strong": self.max_strong,
                "loops_completed": self.loops_completed,
                "last_cycle_at": self.last_cycle_at,
                "last_error": self.last_error,
                "last_cycle_result": self.last_cycle_result,
                "orchestration_only": True,
                "demo_simulation_only": True,
            }
