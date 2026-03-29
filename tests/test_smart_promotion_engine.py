"""Unit tests for Smart Promotion Engine classification logic."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.smart_promotion_engine import (  # noqa: E402
    PROMOTE_TO_DEMO,
    PROMOTE_TO_REVIEW,
    PROMOTE_TO_RUNNER,
    cumulative_tier_lists,
    highest_tier_for_row,
    qualifies_tier,
)


def _row(
    *,
    perf: float,
    sr: float,
    runs: int,
    fails: int = 0,
) -> dict:
    sc = int(round(sr * runs)) if runs else 0
    return {
        "strategy_id": "x",
        "total_runs": runs,
        "success_count": sc,
        "fail_count": fails,
        "success_rate": sr if runs else 0.0,
        "performance_score": perf,
        "avg_duration": 0.0,
        "last_run": None,
    }


class SmartPromotionClassificationTests(unittest.TestCase):
    def test_review_threshold_requires_runs(self) -> None:
        r = _row(perf=0.61, sr=1.0, runs=2)
        self.assertFalse(qualifies_tier(r, PROMOTE_TO_REVIEW))
        r2 = _row(perf=0.61, sr=1.0, runs=3)
        self.assertTrue(qualifies_tier(r2, PROMOTE_TO_REVIEW))

    def test_highest_tier_picks_max(self) -> None:
        r = _row(perf=0.85, sr=0.75, runs=10, fails=1)
        self.assertEqual(highest_tier_for_row(r), PROMOTE_TO_RUNNER)
        r2 = _row(perf=0.65, sr=0.5, runs=5)
        self.assertEqual(highest_tier_for_row(r2), PROMOTE_TO_REVIEW)

    def test_demo_requires_success_rate(self) -> None:
        r = _row(perf=0.71, sr=0.55, runs=8)
        self.assertFalse(qualifies_tier(r, PROMOTE_TO_DEMO))

    def test_cumulative_lists(self) -> None:
        factory = [{"strategy_id": "a", "family": "EMA_CROSS", "review_status": "PENDING_REVIEW", "demo_status": "", "executor_status": "", "runner_status": ""}]
        perf = [_row(perf=0.85, sr=0.75, runs=8)]
        perf[0]["strategy_id"] = "a"
        out = cumulative_tier_lists(factory, perf)
        self.assertTrue(len(out["review_candidates"]) >= 1)
        self.assertTrue(len(out["runner_candidates"]) >= 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
