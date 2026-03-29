"""Unit tests for Auto Recovery Engine (no HTTP; no shared DB assumptions)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.auto_recovery_engine import (  # noqa: E402
    AUTO_LOOP_ERROR,
    NO_PAPER_SUCCESS,
    NO_RUNNER_JOBS,
    RUNNER_FAILED,
    RUNNER_STUCK,
    execute_recovery,
    triggers_from_alerts,
    triggers_from_stuck_runners,
)


class AutoRecoveryEngineTests(unittest.TestCase):
    def test_triggers_from_alerts_maps_rule_codes(self) -> None:
        t = triggers_from_alerts(
            [
                {"active": True, "rule_code": "runner_has_failures"},
                {"active": False, "rule_code": "runner_has_failures"},
                {"active": True, "rule_code": "runner_stale_no_jobs"},
            ]
        )
        self.assertEqual(t, {RUNNER_FAILED, NO_RUNNER_JOBS})

    def test_triggers_from_stuck_runners_old_active(self) -> None:
        jobs = [
            {
                "runner_status": "RUNNER_ACTIVE",
                "runner_started_at": "2020-01-01T00:00:00+00:00",
                "strategy_id": "s1",
            }
        ]
        self.assertTrue(triggers_from_stuck_runners(jobs, max_age_sec=120))

    def test_triggers_from_stuck_runners_recent_active(self) -> None:
        jobs = [
            {
                "runner_status": "RUNNER_ACTIVE",
                "runner_started_at": "2099-01-01T00:00:00+00:00",
                "strategy_id": "s1",
            }
        ]
        self.assertFalse(triggers_from_stuck_runners(jobs, max_age_sec=3600))

    def test_execute_recovery_priority_auto_loop_first(self) -> None:
        order: list[str] = []

        def rec_loop():
            order.append("loop")
            return {"ok": True, "steps": []}

        def rec_paper():
            order.append("paper")
            return {"ok": True, "steps": []}

        handlers = {
            "get_active_alerts": lambda: [
                {"active": True, "rule_code": "auto_loop_error"},
                {"active": True, "rule_code": "paper_no_success"},
            ],
            "get_runner_jobs": lambda: [],
            f"recover_{AUTO_LOOP_ERROR}": rec_loop,
            f"recover_{NO_PAPER_SUCCESS}": rec_paper,
        }
        out = execute_recovery(handlers)
        self.assertIn(AUTO_LOOP_ERROR, out["triggers"])
        self.assertIn(NO_PAPER_SUCCESS, out["triggers"])
        self.assertEqual(order, ["loop", "paper"])

    def test_execute_recovery_stuck_runner_invokes_handler(self) -> None:
        called = False

        def rec_stuck():
            nonlocal called
            called = True
            return {"ok": True, "steps": []}

        old = "2019-06-01T12:00:00+00:00"
        handlers = {
            "get_active_alerts": lambda: [],
            "get_runner_jobs": lambda: [
                {
                    "runner_status": "RUNNER_ACTIVE",
                    "runner_started_at": old,
                    "strategy_id": "x",
                }
            ],
            f"recover_{RUNNER_STUCK}": rec_stuck,
        }
        out = execute_recovery(handlers)
        self.assertIn(RUNNER_STUCK, out["triggers"])
        self.assertTrue(called)

    def test_execute_recovery_no_triggers(self) -> None:
        handlers = {
            "get_active_alerts": lambda: [],
            "get_runner_jobs": lambda: [],
        }
        out = execute_recovery(handlers)
        self.assertEqual(out["triggers"], [])
        self.assertEqual(out["last_action"], "scan_only")


class LoadRecoveryStatusTests(unittest.TestCase):
    def test_load_recovery_status_defaults(self) -> None:
        from unittest.mock import patch

        from backend.auto_recovery_engine import load_recovery_status

        with patch("backend.database.get_alert_engine_state", return_value=None):
            st = load_recovery_status(None)
        self.assertEqual(st["recovery_state"], "RECOVERY_IDLE")
        self.assertFalse(st["active"])
        self.assertEqual(st["recovery_history"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
