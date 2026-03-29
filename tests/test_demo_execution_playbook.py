"""Tests for FP Markets Demo Execution Playbook (demo-only rollout)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.demo_execution_playbook import (  # noqa: E402
    PHASE_CONTROLLED_DEMO,
    PHASE_COMPLETE,
    PHASE_SCALED_DEMO,
    PHASE_SMOKE_TEST,
    PLAYBOOK_CONTROLLED,
    PLAYBOOK_IDLE,
    PLAYBOOK_SCALED,
    build_phase_gate_checklist,
    evaluate_readiness_checks,
    next_playbook_phase,
    reset_playbook,
    start_playbook,
)


def _healthy_inputs() -> dict:
    return {
        "system_health": {"system_health": "GOOD"},
        "risk_status": {"risk_level": "LOW"},
        "meta_status": {"system_posture": "BALANCED"},
        "runner_status": {"counts": {"RUNNER_FAILED": 0}},
        "cluster_status": {"cluster_health": "HEALTHY", "offline_runners": 0, "runner_count": 2},
        "autonomous_status": {"state": "AUTONOMOUS_RUNNING"},
    }


class DemoExecutionPlaybookCoreTests(unittest.TestCase):
    def test_start_and_reset_state(self) -> None:
        st = start_playbook({"state": PLAYBOOK_IDLE, "phase": PHASE_SMOKE_TEST, "history": []})
        self.assertEqual(st["phase"], PHASE_SMOKE_TEST)
        self.assertNotEqual(st["state"], PLAYBOOK_IDLE)
        rst = reset_playbook()
        self.assertEqual(rst["state"], PLAYBOOK_IDLE)
        self.assertEqual(rst["phase"], PHASE_SMOKE_TEST)

    def test_readiness_blocking_logic(self) -> None:
        payload = _healthy_inputs()
        payload["risk_status"] = {"risk_level": "HIGH"}
        out = evaluate_readiness_checks(**payload)
        self.assertEqual(out["readiness"], "BLOCKED")
        self.assertTrue(any("Risk level" in x for x in out["blocking_conditions"]))

    def test_readiness_ready_when_all_ok(self) -> None:
        out = evaluate_readiness_checks(**_healthy_inputs())
        self.assertEqual(out["readiness"], "READY")
        self.assertEqual(out["blocking_conditions"], [])

    def test_phase_transitions(self) -> None:
        st = start_playbook({"state": PLAYBOOK_IDLE, "phase": PHASE_SMOKE_TEST, "history": []})
        st1, ok1, _ = next_playbook_phase(st, "READY")
        self.assertTrue(ok1)
        self.assertEqual(st1["phase"], PHASE_CONTROLLED_DEMO)
        self.assertEqual(st1["state"], PLAYBOOK_CONTROLLED)

        st2, ok2, _ = next_playbook_phase(st1, "READY")
        self.assertTrue(ok2)
        self.assertEqual(st2["phase"], PHASE_SCALED_DEMO)
        self.assertEqual(st2["state"], PLAYBOOK_SCALED)

        st3, ok3, _ = next_playbook_phase(st2, "READY")
        self.assertTrue(ok3)
        self.assertEqual(st3["phase"], PHASE_COMPLETE)

    def test_phase_transition_blocked(self) -> None:
        st = start_playbook({"state": PLAYBOOK_IDLE, "phase": PHASE_SMOKE_TEST, "history": []})
        nxt, adv, reason = next_playbook_phase(st, "BLOCKED")
        self.assertFalse(adv)
        self.assertEqual(reason, "blocked_by_readiness_checks")
        self.assertEqual(nxt["phase"], PHASE_SMOKE_TEST)

    def test_phase_gate_checklist(self) -> None:
        st = {"phase": PHASE_SMOKE_TEST}
        safe = build_phase_gate_checklist(
            playbook_state=st,
            system_status={"system_health": "GOOD", "runner_status": {"counts": {"RUNNER_FAILED": 0}}},
            meta_status={"system_posture": "BALANCED"},
            risk_status={"risk_level": "LOW"},
            cluster_status={"cluster_health": "HEALTHY", "offline_runners": 0, "runner_count": 2},
            autonomous_status={"state": "AUTONOMOUS_RUNNING"},
        )
        self.assertEqual(safe["readiness"], "READY")
        self.assertTrue(safe["ready_to_advance"])
        self.assertEqual(safe["blocking_conditions"], [])

        blocked = build_phase_gate_checklist(
            playbook_state=st,
            system_status={"system_health": "DEGRADED", "runner_status": {"counts": {"RUNNER_FAILED": 1}}},
            meta_status={"system_posture": "CHAOTIC_SAFE_MODE"},
            risk_status={"risk_level": "HIGH"},
            cluster_status={"cluster_health": "DEGRADED", "offline_runners": 2, "runner_count": 2},
            autonomous_status={"state": "AUTONOMOUS_ERROR"},
        )
        self.assertEqual(blocked["readiness"], "BLOCKED")
        self.assertFalse(blocked["ready_to_advance"])
        self.assertGreater(len(blocked["blocking_conditions"]), 0)


class DemoExecutionPlaybookHandlerTests(unittest.TestCase):
    def test_handlers_shape(self) -> None:
        import backend.main as main

        with patch.object(main, "get_system_health", return_value={"system_health": "GOOD"}):
            with patch.object(main, "get_global_risk_status", return_value={"risk_level": "LOW"}):
                with patch.object(main, "get_meta_ai_control_status", return_value={"system_posture": "BALANCED"}):
                    with patch.object(main, "get_runner_status", return_value={"counts": {"RUNNER_FAILED": 0}}):
                        with patch.object(
                            main,
                            "get_cluster_status",
                            return_value={"cluster_health": "HEALTHY", "offline_runners": 0, "runner_count": 2},
                        ):
                            with patch.object(main, "get_autonomous_fund_status", return_value={"state": "AUTONOMOUS_RUNNING"}):
                                main.post_demo_execution_playbook_reset()
                                started = main.post_demo_execution_playbook_start()
                                self.assertIn("phase", started)
                                self.assertIn("readiness", started)
                                self.assertIn("checks", started)

                                nxt = main.post_demo_execution_playbook_next()
                                self.assertIn("advanced", nxt)
                                self.assertIn("reason", nxt)

                                status = main.get_demo_execution_playbook_status()
                                self.assertIn("recommendations", status)
                                self.assertIn("state", status)

                                checks = main.get_demo_playbook_checks()
                                self.assertIn("phase", checks)
                                self.assertIn("readiness", checks)
                                self.assertIn("checks", checks)
                                self.assertIn("ready_to_advance", checks)


if __name__ == "__main__":
    unittest.main()
