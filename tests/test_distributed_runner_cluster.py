"""Tests for Distributed Runner Cluster (safe scaling orchestration only)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.distributed_runner_cluster import (  # noqa: E402
    RUNNER_OFFLINE,
    build_cluster_runners_payload,
    build_cluster_status_payload,
    estimate_failover_reassignments,
    heartbeat_runner,
    mark_runner_offline,
    pick_best_runner,
    register_runner,
)


def _state() -> dict:
    return {"version": 1, "updated_at": None, "runners": {}}


class DistributedRunnerClusterTests(unittest.TestCase):
    def test_register_runner(self) -> None:
        st = _state()
        out = register_runner(
            st,
            runner_id="r1",
            hostname="h1",
            ip="10.0.0.1",
            capacity=4,
            current_load=1,
            version="1.0",
            region="us-east",
        )
        self.assertTrue(out["ok"])
        self.assertEqual(len(st["runners"]), 1)

    def test_heartbeat_updates_load(self) -> None:
        st = _state()
        register_runner(st, runner_id="r1", hostname="h1", ip="1", capacity=4)
        hb = heartbeat_runner(st, runner_id="r1", current_load=3, version="2.0")
        self.assertTrue(hb["ok"])
        self.assertEqual(int(hb["runner"]["current_load"]), 3)

    def test_offline_detection_mark(self) -> None:
        st = _state()
        register_runner(st, runner_id="r1", hostname="h1", ip="1", capacity=2)
        out = mark_runner_offline(st, runner_id="r1")
        self.assertTrue(out["ok"])
        self.assertEqual(out["runner"]["status"], RUNNER_OFFLINE)

    def test_load_balancing_pick_lowest_ratio(self) -> None:
        st = _state()
        register_runner(st, runner_id="r1", hostname="h1", ip="1", capacity=4, current_load=3, region="us-east")
        register_runner(st, runner_id="r2", hostname="h2", ip="2", capacity=4, current_load=1, region="us-east")
        best = pick_best_runner(st, preferred_region="us-east")
        self.assertIsNotNone(best)
        self.assertEqual(best["runner_id"], "r2")

    def test_failover_reassignments(self) -> None:
        st = _state()
        register_runner(st, runner_id="r1", hostname="h1", ip="1", capacity=4, current_load=4, region="us-east")
        register_runner(st, runner_id="r2", hostname="h2", ip="2", capacity=4, current_load=1, region="us-east")
        mark_runner_offline(st, runner_id="r1")
        plan = estimate_failover_reassignments(
            st,
            failed_runner_id="r1",
            queued_jobs=[{"strategy_id": "s1", "runner_id": "r1", "region": "us-east"}],
        )
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["to_runner_id"], "r2")

    def test_status_payload(self) -> None:
        st = _state()
        register_runner(st, runner_id="r1", hostname="h1", ip="1", capacity=2)
        payload = build_cluster_status_payload(st)
        self.assertIn("cluster_health", payload)
        runners = build_cluster_runners_payload(st)
        self.assertIn("runners", runners)


class DistributedRunnerClusterMainTests(unittest.TestCase):
    def test_cluster_endpoints_handlers(self) -> None:
        from backend.main import (
            cluster_heartbeat,
            cluster_offline,
            cluster_register,
            get_cluster_runners,
            get_cluster_status,
        )

        r = cluster_register(
            runner_id="test-runner-1",
            hostname="host",
            ip="127.0.0.1",
            capacity=4,
            current_load=0,
            version="1.0.0",
            region="test",
        )
        self.assertTrue(r.get("ok"))
        hb = cluster_heartbeat(runner_id="test-runner-1", current_load=1, version="1.0.1")
        self.assertTrue(hb.get("ok"))
        st = get_cluster_status()
        self.assertIn("cluster_health", st)
        rr = get_cluster_runners()
        self.assertIn("runners", rr)
        off = cluster_offline(runner_id="test-runner-1")
        self.assertTrue(off.get("ok"))

    def test_integration_with_health(self) -> None:
        from backend.main import get_system_health

        with patch("backend.main.get_cluster_status", return_value={"cluster_health": "DEGRADED"}):
            h = get_system_health()
        self.assertIn("engines", h)
        self.assertIn("distributed_cluster_engine", h["engines"])


if __name__ == "__main__":
    unittest.main()
