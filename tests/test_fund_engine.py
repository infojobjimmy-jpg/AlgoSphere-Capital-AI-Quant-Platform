"""Tests for simulated Fund Engine allocation logic."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.fund_engine import (  # noqa: E402
    DEFAULT_TOTAL_CAPITAL,
    MAX_ALLOCATION_PCT,
    MIN_ALLOCATION_PCT,
    build_fund_simulation,
    compute_allocation_percents,
    select_eligible_strategies,
)


def _perf_row(sid: str, score: float, runs: int, fails: int = 0) -> dict:
    sc = runs - fails
    return {
        "strategy_id": sid,
        "total_runs": runs,
        "success_count": sc,
        "fail_count": fails,
        "success_rate": sc / runs if runs else 0.0,
        "performance_score": score,
        "avg_duration": 1.0,
        "last_run": None,
    }


class FundEngineLogicTests(unittest.TestCase):
    def test_eligible_filters_score_floor(self) -> None:
        rows = [_perf_row("a", 0.71, 5), _perf_row("b", 0.69, 5)]
        el = select_eligible_strategies(rows)
        self.assertEqual(len(el), 1)
        self.assertEqual(el[0]["strategy_id"], "a")

    def test_max_per_strategy_cap(self) -> None:
        rows = [_perf_row(f"s{i}", 0.95, 10, 0) for i in range(20)]
        el = select_eligible_strategies(rows)
        pairs = compute_allocation_percents(el)
        for _, p in pairs:
            self.assertLessEqual(p, MAX_ALLOCATION_PCT + 1e-6)
            self.assertGreaterEqual(p, MIN_ALLOCATION_PCT - 1e-6)

    def test_build_status_totals(self) -> None:
        rows = [_perf_row("x", 0.9, 8, 1)]
        paper = [{"strategy_id": "x", "paper_win_rate": 0.5, "paper_drawdown": 100.0}]
        st, strat, meta = build_fund_simulation(
            perf_rows=rows,
            paper_items=paper,
            total_capital=100_000.0,
        )
        self.assertEqual(st["total_capital"], 100_000.0)
        self.assertGreaterEqual(st["free_capital"], 0.0)
        self.assertAlmostEqual(st["allocated_capital"] + st["free_capital"], 100_000.0, places=1)
        self.assertEqual(len(strat), 1)
        self.assertIn("allocation_percent", strat[0])
        self.assertIn("allocation_amount", strat[0])
        self.assertGreater(meta["eligible_count"], 0)

    def test_empty_eligible_all_free(self) -> None:
        rows = [_perf_row("z", 0.5, 3)]
        st, strat, _ = build_fund_simulation(perf_rows=rows, paper_items=[], total_capital=DEFAULT_TOTAL_CAPITAL)
        self.assertEqual(strat, [])
        self.assertEqual(st["allocated_capital"], 0.0)
        self.assertEqual(st["free_capital"], float(DEFAULT_TOTAL_CAPITAL))


if __name__ == "__main__":
    unittest.main(verbosity=2)
