"""Tests for Performance Engine (pure metrics logic)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.performance_engine import (  # noqa: E402
    OUTCOME_FAIL,
    OUTCOME_SUCCESS,
    SOURCE_RUNNER,
    aggregate_run_logs,
    build_performance_trends,
    build_strategies_performance,
    build_system_performance,
    compute_performance_score,
    duration_sec_between,
    finalize_strategy_row,
    merge_paper_snapshots,
    pipeline_throughput_score,
    recovery_rate_from_history,
)


class PerformanceEngineMathTests(unittest.TestCase):
    def test_compute_performance_score_weights(self) -> None:
        s = compute_performance_score(1.0, 1.0, 1.0)
        self.assertEqual(s, 1.0)
        s2 = compute_performance_score(0.0, 0.0, 0.0)
        self.assertEqual(s2, 0.0)
        s3 = compute_performance_score(1.0, 0.0, 0.0)
        self.assertEqual(s3, 0.5)

    def test_duration_sec_between(self) -> None:
        d = duration_sec_between(
            "2024-01-01T00:00:00+00:00",
            "2024-01-01T00:00:30+00:00",
        )
        self.assertEqual(d, 30.0)

    def test_aggregate_run_logs(self) -> None:
        rows = [
            {
                "strategy_id": "a",
                "outcome": OUTCOME_SUCCESS,
                "duration_sec": 10.0,
                "run_ended_at": "2025-01-02T00:00:00+00:00",
                "source": SOURCE_RUNNER,
            },
            {
                "strategy_id": "a",
                "outcome": OUTCOME_FAIL,
                "duration_sec": 5.0,
                "run_ended_at": "2025-01-03T00:00:00+00:00",
                "source": SOURCE_RUNNER,
            },
        ]
        by_sid = aggregate_run_logs(rows)
        self.assertEqual(by_sid["a"]["total_runs"], 2)
        self.assertEqual(by_sid["a"]["success_count"], 1)
        self.assertEqual(by_sid["a"]["fail_count"], 1)
        row = finalize_strategy_row("a", by_sid["a"])
        self.assertEqual(row["total_runs"], 2)
        self.assertEqual(row["success_rate"], 0.5)
        self.assertAlmostEqual(row["avg_duration"], 7.5, places=3)

    def test_merge_paper_snapshots(self) -> None:
        by_sid = aggregate_run_logs([])
        merge_paper_snapshots(
            by_sid,
            [{"strategy_id": "p1", "status": "PAPER_SUCCESS", "last_updated": "2025-02-01T00:00:00+00:00"}],
        )
        r = finalize_strategy_row("p1", by_sid["p1"])
        self.assertEqual(r["total_runs"], 1)
        self.assertEqual(r["success_rate"], 1.0)

    def test_recovery_rate_from_history(self) -> None:
        hist = [
            {"recovery_state": "RECOVERY_SUCCESS"},
            {"recovery_state": "RECOVERY_FAILED"},
        ]
        import json

        r = recovery_rate_from_history(json.dumps(hist))
        self.assertEqual(r, 0.5)

    def test_pipeline_throughput_score(self) -> None:
        p = pipeline_throughput_score(
            {
                "total_candidates": 100,
                "paper_success": 10,
                "demo_queued": 5,
                "live_safe_ready": 5,
                "paper_running": 0,
            }
        )
        self.assertEqual(p, 0.2)

    def test_build_performance_trends(self) -> None:
        rows = [
            {"run_ended_at": "2025-03-01T12:00:00+00:00", "outcome": OUTCOME_SUCCESS},
            {"run_ended_at": "2025-03-01T13:00:00+00:00", "outcome": OUTCOME_FAIL},
            {"run_ended_at": "2025-03-02T10:00:00+00:00", "outcome": OUTCOME_SUCCESS},
        ]
        t = build_performance_trends(rows, max_days=14)
        self.assertTrue(len(t) >= 2)


class PerformanceEngineIntegrityTests(unittest.TestCase):
    def test_strategies_list_aligned_with_factory(self) -> None:
        factory = [
            {"strategy_id": "s1", "family": "EMA_CROSS"},
            {"strategy_id": "s2", "family": "MOMENTUM"},
        ]
        runs = [
            {
                "strategy_id": "s1",
                "outcome": OUTCOME_SUCCESS,
                "duration_sec": 1.0,
                "run_ended_at": "2025-01-01T00:00:00+00:00",
                "source": SOURCE_RUNNER,
            }
        ]
        out = build_strategies_performance(factory, runs, [])
        self.assertEqual(len(out), 2)
        by_id = {x["strategy_id"]: x for x in out}
        self.assertEqual(by_id["s1"]["total_runs"], 1)
        self.assertEqual(by_id["s2"]["total_runs"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
