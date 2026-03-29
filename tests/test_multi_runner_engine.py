"""Unit tests for multi-runner coordination (planning, status, grouping; no broker)."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.demo_runner_bridge import RUNNER_ACTIVE  # noqa: E402
from backend.multi_runner_engine import (  # noqa: E402
    RUNNER_BUSY,
    RUNNER_DEGRADED,
    RUNNER_IDLE,
    RUNNER_OFFLINE,
    RUNNER_PENDING,
    build_fleet_summary,
    count_assigned_jobs_for_runner,
    effective_runner_status,
    group_jobs_by_runner,
    plan_balanced_assignments,
    seconds_since_last_seen,
)


class MultiRunnerEngineTests(unittest.TestCase):
    def test_effective_runner_offline(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        row = {
            "runner_id": "r1",
            "runner_capacity": 4,
            "current_load": 0,
            "runner_status": RUNNER_OFFLINE,
            "last_seen_at": now.isoformat(),
            "runner_health": "GOOD",
        }
        disp, tag = effective_runner_status(row, now=now, assigned_jobs_count=0)
        self.assertEqual(disp, RUNNER_OFFLINE)
        self.assertEqual(tag, "fleet_marked_offline")

    def test_effective_runner_stale_is_degraded(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        old = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        row = {
            "runner_id": "r1",
            "runner_capacity": 4,
            "current_load": 0,
            "runner_status": RUNNER_IDLE,
            "last_seen_at": old.isoformat(),
            "runner_health": "GOOD",
        }
        disp, tag = effective_runner_status(row, now=now, assigned_jobs_count=0)
        self.assertEqual(disp, RUNNER_DEGRADED)
        self.assertEqual(tag, "heartbeat_stale_or_missing")

    def test_effective_runner_idle_and_busy(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        row = {
            "runner_id": "r1",
            "runner_capacity": 2,
            "current_load": 1,
            "runner_status": RUNNER_BUSY,
            "last_seen_at": now.isoformat(),
            "runner_health": "GOOD",
        }
        disp0, _ = effective_runner_status(row, now=now, assigned_jobs_count=0)
        self.assertEqual(disp0, RUNNER_BUSY)
        disp_idle, _ = effective_runner_status(
            {**row, "current_load": 0}, now=now, assigned_jobs_count=0
        )
        self.assertEqual(disp_idle, RUNNER_IDLE)

    def test_count_assigned_jobs_for_runner(self) -> None:
        strategies = [
            {"runner_id": "a", "runner_status": RUNNER_PENDING},
            {"runner_id": "a", "runner_status": RUNNER_ACTIVE},
            {"runner_id": "b", "runner_status": RUNNER_PENDING},
            {"runner_id": "a", "runner_status": "RUNNER_COMPLETED"},
        ]
        self.assertEqual(count_assigned_jobs_for_runner(strategies, "a"), 2)

    def test_group_jobs_by_runner(self) -> None:
        jobs = [
            {"strategy_id": "s1", "runner_id": ""},
            {"strategy_id": "s2", "runner_id": "r1"},
            {"strategy_id": "s3", "runner_id": "r1"},
        ]
        g = group_jobs_by_runner(jobs)
        self.assertEqual(len(g["unassigned_queue"]), 1)
        self.assertEqual(len(g["by_runner"]["r1"]), 2)
        self.assertEqual(g["total_jobs"], 3)

    def test_plan_balanced_assignments_prefers_spare_capacity(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        jobs = [
            {
                "strategy_id": "j1",
                "eligible": True,
                "runner_id": "",
                "runner_status": RUNNER_PENDING,
                "runner_priority": 10.0,
            },
            {
                "strategy_id": "j2",
                "eligible": True,
                "runner_id": "",
                "runner_status": RUNNER_PENDING,
                "runner_priority": 5.0,
            },
        ]
        runners = [
            {
                "runner_id": "rA",
                "runner_capacity": 2,
                "current_load": 0,
                "runner_status": RUNNER_IDLE,
                "last_seen_at": now.isoformat(),
                "runner_health": "GOOD",
            },
            {
                "runner_id": "rB",
                "runner_capacity": 4,
                "current_load": 0,
                "runner_status": RUNNER_IDLE,
                "last_seen_at": now.isoformat(),
                "runner_health": "GOOD",
            },
        ]
        strategies: list[dict] = []
        assigned, skipped = plan_balanced_assignments(
            jobs, runners, now=now, strategies=strategies
        )
        self.assertEqual(len(skipped), 0)
        self.assertEqual(len(assigned), 2)
        # rB has higher spare (4 vs 2) — both jobs should go to rB first per greedy rule
        self.assertEqual(assigned[0]["runner_id"], "rB")
        self.assertEqual(assigned[1]["runner_id"], "rB")
        self.assertIn("spare_capacity", assigned[0]["reason"].lower() or "")

    def test_plan_skips_offline_and_degraded(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        jobs = [
            {
                "strategy_id": "j1",
                "eligible": True,
                "runner_id": "",
                "runner_status": RUNNER_PENDING,
                "runner_priority": 1.0,
            },
        ]
        runners = [
            {
                "runner_id": "rOff",
                "runner_capacity": 4,
                "current_load": 0,
                "runner_status": RUNNER_OFFLINE,
                "last_seen_at": now.isoformat(),
                "runner_health": "GOOD",
            },
        ]
        assigned, skipped = plan_balanced_assignments(jobs, runners, now=now, strategies=[])
        self.assertEqual(assigned, [])
        self.assertEqual(len(skipped), 1)
        self.assertIn("no_eligible_runner", skipped[0]["reason"])

    def test_build_fleet_summary_payload(self) -> None:
        payloads = [
            {"runner_status": RUNNER_IDLE},
            {"runner_status": RUNNER_DEGRADED},
            {"runner_status": RUNNER_OFFLINE},
        ]
        s = build_fleet_summary(payloads)
        self.assertEqual(s["runner_count"], 3)
        self.assertEqual(s["healthy_runner_count"], 1)
        self.assertEqual(s["degraded_count"], 1)
        self.assertEqual(s["offline_count"], 1)
        self.assertIn("by_status", s)

    def test_seconds_since_last_seen(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        prev = datetime(2025, 1, 1, 11, 58, 0, tzinfo=timezone.utc)
        sec = seconds_since_last_seen(prev.isoformat(), now)
        self.assertAlmostEqual(sec or 0, 120.0, places=3)


if __name__ == "__main__":
    unittest.main()
