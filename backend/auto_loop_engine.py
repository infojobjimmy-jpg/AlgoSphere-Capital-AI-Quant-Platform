from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Callable


class AutoLoopEngine:
    """
    Safe local automation loop:
    Factory -> Evolution -> Paper -> Feedback -> repeat.
    """

    def __init__(
        self,
        generate_fn: Callable[[], dict],
        evolve_fn: Callable[[], dict],
        paper_deploy_fn: Callable[[], dict],
        feedback_fn: Callable[[], dict],
        strategy_count_fn: Callable[[], int],
    ) -> None:
        self._generate_fn = generate_fn
        self._evolve_fn = evolve_fn
        self._paper_deploy_fn = paper_deploy_fn
        self._feedback_fn = feedback_fn
        self._strategy_count_fn = strategy_count_fn

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop_timestamps: deque[float] = deque()

        self.running = False
        self.interval_sec = 60
        self.max_loops_per_hour = 30
        self.max_strategies = 1000
        self.loops_completed = 0
        self.last_cycle_at: str | None = None
        self.last_error: str | None = None
        self.last_cycle_result: dict | None = None

    def _trim_hour_window(self, now_ts: float) -> None:
        hour_ago = now_ts - 3600
        while self._loop_timestamps and self._loop_timestamps[0] < hour_ago:
            self._loop_timestamps.popleft()

    def run_cycle(self) -> dict:
        with self._lock:
            current_count = self._strategy_count_fn()
            if current_count >= self.max_strategies:
                self.last_error = (
                    f"max strategies reached ({current_count} >= {self.max_strategies})"
                )
                self.running = False
                return {"ok": False, "error": self.last_error}

        cycle = {
            "generated": self._generate_fn(),
            "evolved": self._evolve_fn(),
            "paper": self._paper_deploy_fn(),
            "feedback": self._feedback_fn(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        with self._lock:
            self.loops_completed += 1
            self.last_cycle_at = cycle["timestamp"]
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
                        self.running = False
                        break
                cycle_result = self.run_cycle()
                if not cycle_result.get("ok", False):
                    break
                with self._lock:
                    self._loop_timestamps.append(now_ts)
            except Exception as exc:  # pragma: no cover - defensive runtime stop
                with self._lock:
                    self.last_error = str(exc)
                    self.running = False
                break
            if self._stop_event.wait(timeout=max(1, int(self.interval_sec))):
                break
        with self._lock:
            self.running = False

    def start(
        self,
        interval_sec: int = 60,
        max_loops_per_hour: int = 30,
        max_strategies: int = 1000,
    ) -> dict:
        with self._lock:
            if self.running:
                return {"started": False, "message": "already running"}
            self.interval_sec = max(1, int(interval_sec))
            self.max_loops_per_hour = max(1, int(max_loops_per_hour))
            self.max_strategies = max(1, int(max_strategies))
            self._stop_event.clear()
            self.running = True
            self.last_error = None
            self._thread = threading.Thread(target=self._loop_runner, daemon=True)
            self._thread.start()
        return {"started": True}

    def stop(self) -> dict:
        self._stop_event.set()
        with self._lock:
            self.running = False
        return {"stopped": True}

    def clear_last_error(self) -> dict[str, bool]:
        """Safe recovery: clear error flag without executing trades."""
        with self._lock:
            self.last_error = None
        return {"cleared": True}

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self.running,
                "interval_sec": self.interval_sec,
                "max_loops_per_hour": self.max_loops_per_hour,
                "max_strategies": self.max_strategies,
                "loops_completed": self.loops_completed,
                "last_cycle_at": self.last_cycle_at,
                "last_error": self.last_error,
                "last_cycle_result": self.last_cycle_result,
            }
