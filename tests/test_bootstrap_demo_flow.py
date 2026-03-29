from __future__ import annotations

import unittest

from backend.bootstrap_demo_flow import (
    BOOTSTRAP_DEMO_ONLY,
    BOOTSTRAP_MAX_PAPER_TRADES,
    BOOTSTRAP_RISK_ALLOCATION_PCT,
    build_bootstrap_demo_note,
    should_enter_bootstrap,
)


class BootstrapDemoFlowTests(unittest.TestCase):
    def test_should_enter_bootstrap_for_new_evolved_zero_history(self) -> None:
        strategy = {
            "strategy_id": "s1",
            "generation": 2,
            "parent_strategy_id": "p1",
            "origin_type": "EVOLVED",
            "review_status": "PENDING_REVIEW",
            "demo_status": "",
            "executor_status": "",
            "runner_status": "",
        }
        perf = {"strategy_id": "s1", "total_runs": 0, "success_rate": 0.0, "performance_score": 0.3}
        ok, reason = should_enter_bootstrap(strategy, perf)
        self.assertTrue(ok)
        self.assertEqual(reason, "bootstrap_required")

    def test_should_not_enter_bootstrap_when_already_in_demo(self) -> None:
        strategy = {
            "strategy_id": "s2",
            "generation": 1,
            "parent_strategy_id": "p2",
            "review_status": "PENDING_REVIEW",
            "demo_status": "DEMO_ASSIGNED",
            "executor_status": "",
            "runner_status": "",
        }
        perf = {"strategy_id": "s2", "total_runs": 0, "success_rate": 0.0, "performance_score": 0.3}
        ok, reason = should_enter_bootstrap(strategy, perf)
        self.assertFalse(ok)
        self.assertEqual(reason, "already_in_demo_flow")

    def test_bootstrap_note_contains_safe_constraints(self) -> None:
        note = build_bootstrap_demo_note()
        self.assertIn("bootstrap_demo_flow:", note)
        self.assertIn(str(BOOTSTRAP_MAX_PAPER_TRADES), note)
        self.assertIn(str(BOOTSTRAP_RISK_ALLOCATION_PCT), note)
        self.assertIn(str(BOOTSTRAP_DEMO_ONLY).lower(), note.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
