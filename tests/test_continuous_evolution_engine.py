"""Tests for Continuous Evolution Loop Engine (orchestration only; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.continuous_evolution_engine import (  # noqa: E402
    EVOLUTION_IDLE,
    EVOLUTION_PAUSED,
    EVOLUTION_RUNNING,
    ContinuousEvolutionLoopEngine,
)


class ContinuousEvolutionEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calls: list[str] = []

        def evo() -> dict:
            self.calls.append("evolution")
            return {"evolution": 1}

        def paper() -> dict:
            self.calls.append("paper")
            return {"paper": 2}

        def fb() -> dict:
            self.calls.append("feedback")
            return {"feedback": 3}

        def perf() -> dict:
            self.calls.append("performance")
            return {"performance": 4}

        self.engine = ContinuousEvolutionLoopEngine(
            evolution_run_fn=evo,
            paper_deploy_fn=paper,
            feedback_fn=fb,
            performance_snapshot_fn=perf,
        )

    def test_run_cycle_order_and_state(self) -> None:
        self.assertEqual(self.engine.state, EVOLUTION_IDLE)
        out = self.engine.run_cycle()
        self.assertTrue(out.get("ok"))
        self.assertEqual(
            self.calls,
            ["evolution", "paper", "feedback", "performance"],
        )
        self.assertEqual(self.engine.loops_completed, 1)
        self.assertIsNotNone(self.engine.last_cycle_result)

    def test_start_pause_status(self) -> None:
        s = self.engine.start(interval_sec=3600, max_loops_per_hour=2, max_weak=3, max_strong=1)
        self.assertTrue(s.get("started"))
        self.assertEqual(self.engine.state, EVOLUTION_RUNNING)
        st = self.engine.status()
        self.assertEqual(st["state"], EVOLUTION_RUNNING)
        self.assertEqual(st["max_weak"], 3)
        self.assertEqual(st["max_strong"], 1)
        self.assertTrue(st.get("orchestration_only"))
        p = self.engine.pause()
        self.assertTrue(p.get("paused"))
        self.assertEqual(self.engine.state, EVOLUTION_PAUSED)

    def test_start_idempotent_when_running(self) -> None:
        self.engine.start(interval_sec=3600, max_loops_per_hour=10)
        r = self.engine.start(interval_sec=60)
        self.assertFalse(r.get("started"))

    def test_run_once_does_not_require_running(self) -> None:
        self.assertEqual(self.engine.state, EVOLUTION_IDLE)
        out = self.engine.run_once()
        self.assertIn("cycle", out or {})
        self.assertEqual(self.engine.state, EVOLUTION_IDLE)

    def test_get_cycle_params(self) -> None:
        self.engine.start(interval_sec=3600, max_loops_per_hour=10, max_weak=7, max_strong=2)
        self.assertEqual(self.engine.get_cycle_params(), (7, 2))


class ContinuousEvolutionMainHandlersTests(unittest.TestCase):
    def test_status_and_run_once_handlers(self) -> None:
        from backend.main import (
            get_continuous_evolution_loop_status,
            run_continuous_evolution_loop_once,
        )

        st = get_continuous_evolution_loop_status()
        self.assertIn("state", st)
        self.assertIn(st["state"], {EVOLUTION_IDLE, EVOLUTION_RUNNING, EVOLUTION_PAUSED})
        once = run_continuous_evolution_loop_once()
        self.assertIn("cycle", once)


if __name__ == "__main__":
    unittest.main()
