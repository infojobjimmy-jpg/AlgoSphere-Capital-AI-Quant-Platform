"""Tests for Full Autonomous Fund Mode engine (orchestration only)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.autonomous_fund_engine import (  # noqa: E402
    AUTONOMOUS_IDLE,
    AUTONOMOUS_PAUSED,
    AUTONOMOUS_RUNNING,
    AutonomousFundEngine,
)


class AutonomousFundEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calls: list[str] = []
        self.meta = {"system_posture": "BALANCED", "confidence": 0.7}

        def regime() -> dict:
            self.calls.append("regime")
            return {"current_regime": "RANGING"}

        def risk() -> dict:
            self.calls.append("risk")
            return {"global_risk_score": 0.3, "risk_level": "MODERATE"}

        def memory() -> dict:
            self.calls.append("memory")
            return {"ok": True}

        def evo(*, seed: int | None = None, max_weak: int = 5, max_strong: int = 5) -> dict:
            _ = seed
            self.calls.append(f"evo:{max_weak}:{max_strong}")
            return {"ok": True}

        def cont() -> dict:
            self.calls.append("cont")
            return {"ok": True}

        def paper(max_bots: int) -> dict:
            self.calls.append(f"paper:{max_bots}")
            return {"count": max_bots}

        def perf() -> dict:
            self.calls.append("perf")
            return {"runner_success_rate": 0.8}

        def portfolio() -> dict:
            self.calls.append("portfolio")
            return {"count": 0}

        def rebalance() -> dict:
            self.calls.append("rebalance")
            return {"ok": True}

        def assign() -> dict:
            self.calls.append("assign")
            return {"assigned": []}

        def meta() -> dict:
            self.calls.append("meta")
            return dict(self.meta)

        self.engine = AutonomousFundEngine(
            regime_status_fn=regime,
            risk_status_fn=risk,
            memory_update_fn=memory,
            evolution_run_fn=evo,
            continuous_run_once_fn=cont,
            paper_deploy_fn=paper,
            performance_system_fn=perf,
            portfolio_rotation_fn=portfolio,
            fund_rebalance_fn=rebalance,
            multi_runner_assign_fn=assign,
            meta_status_fn=meta,
        )

    def test_run_once_sequence(self) -> None:
        out = self.engine.run_once()
        self.assertTrue(out.get("ok"))
        expected = ["regime", "risk", "memory", "meta"]
        self.assertEqual(self.calls[:4], expected)
        self.assertIn("meta_decision", out.get("cycle", {}))
        self.assertEqual(self.engine.loops_completed, 1)

    def test_start_pause(self) -> None:
        st = self.engine.start(interval_sec=120, max_loops_per_hour=20)
        self.assertTrue(st.get("started"))
        self.assertEqual(st.get("state"), AUTONOMOUS_RUNNING)
        ps = self.engine.pause()
        self.assertTrue(ps.get("paused"))
        self.assertIn(ps.get("state"), {AUTONOMOUS_PAUSED, AUTONOMOUS_IDLE})

    def test_posture_driven_policy(self) -> None:
        self.meta = {"system_posture": "CHAOTIC_SAFE_MODE", "confidence": 0.2}
        out = self.engine.run_once()
        cycle = out.get("cycle", {})
        self.assertTrue(cycle.get("evolution", {}).get("skipped"))
        self.assertTrue(cycle.get("continuous_evolution", {}).get("skipped"))
        self.assertTrue(cycle.get("paper_deploy", {}).get("skipped"))

    def test_empty_safety(self) -> None:
        self.meta = {}
        out = self.engine.run_once()
        self.assertTrue(out.get("ok"))
        st = self.engine.status()
        self.assertIn(st["state"], {AUTONOMOUS_IDLE, AUTONOMOUS_RUNNING, AUTONOMOUS_PAUSED})


class AutonomousFundMainHandlerTests(unittest.TestCase):
    def test_main_handlers(self) -> None:
        from backend.main import (
            get_autonomous_fund_status,
            pause_autonomous_fund_mode,
            run_autonomous_fund_once,
            start_autonomous_fund_mode,
        )

        before = get_autonomous_fund_status()
        self.assertIn("state", before)
        once = run_autonomous_fund_once()
        self.assertIn("ok", once)
        st = start_autonomous_fund_mode(interval_sec=120, max_loops_per_hour=20)
        self.assertIn("status", st)
        pa = pause_autonomous_fund_mode()
        self.assertIn("status", pa)


if __name__ == "__main__":
    unittest.main()
