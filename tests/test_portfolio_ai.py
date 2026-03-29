"""Tests for Portfolio AI / Portfolio Brain upgrade (decision layer only)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.portfolio_ai import (  # noqa: E402
    CORRELATION_PENALTY,
    SAME_FAMILY_CORR,
    _apply_family_soft_cap,
    _avg_correlation,
    _classify_brain_action,
    _pair_correlation,
    build_portfolio_allocation,
)


class PortfolioAITests(unittest.TestCase):
    def test_pair_correlation_same_family_stronger(self) -> None:
        a = {"strategy_id": "s1", "family": "EMA_CROSS", "parent_strategy_id": None}
        b = {"strategy_id": "s2", "family": "EMA_CROSS", "parent_strategy_id": None}
        c = {"strategy_id": "s3", "family": "MOMENTUM", "parent_strategy_id": None}
        self.assertGreaterEqual(_pair_correlation(a, b), SAME_FAMILY_CORR - 0.01)
        self.assertLess(_pair_correlation(a, c), _pair_correlation(a, b))

    def test_empty_allocation_has_brain(self) -> None:
        out = build_portfolio_allocation([], fund_portfolio_strategies=[])
        self.assertEqual(out["count"], 0)
        self.assertIn("brain", out)
        self.assertEqual(out["brain"]["top_priorities"], [])

    def test_brain_actions_and_rotation(self) -> None:
        candidates = [
            {
                "strategy_id": "live_a",
                "family": "EMA_CROSS",
                "paper_drawdown": 100.0,
                "paper_win_rate": 0.6,
                "promotion_score": 70.0,
                "target_status": "LIVE_SAFE_READY",
            },
            {
                "strategy_id": "live_b",
                "family": "MOMENTUM",
                "paper_drawdown": 120.0,
                "paper_win_rate": 0.58,
                "promotion_score": 68.0,
                "target_status": "LIVE_SAFE_READY",
            },
        ]
        fund = [
            {"strategy_id": "live_a", "allocation_percent": 0.01},
            {"strategy_id": "orphan_x", "allocation_percent": 0.08},
        ]
        out = build_portfolio_allocation(
            candidates,
            max_percent_per_strategy=50.0,
            fund_portfolio_strategies=fund,
        )
        self.assertGreaterEqual(out["count"], 2)
        self.assertIn("brain_action", out["allocations"][0])
        self.assertIn("priority_rank", out["allocations"][0])
        brain = out["brain"]
        self.assertTrue(any("orphan_x" == str(x.get("strategy_id")) for x in brain["rotate_out"]))
        self.assertGreaterEqual(CORRELATION_PENALTY, 0.5)

    def test_classify_brain_action_pause(self) -> None:
        act, _ = _classify_brain_action(0.1, 0.1, 0.9)
        self.assertEqual(act, "PAUSE_ALLOCATION")

    def test_family_soft_cap_reduces_weight(self) -> None:
        raw = [1.0, 1.0, 1.0, 1.0]
        # Three distinct families so a 30% soft cap is feasible (avoids 2-family impossibility).
        fams = ["A", "A", "B", "C"]
        out = _apply_family_soft_cap(raw, fams, cap=0.30)
        total = sum(out)
        w = [x / total for x in out]
        share_a = sum(w[i] for i, f in enumerate(fams) if f == "A")
        self.assertLessEqual(share_a, 0.36)

    def test_avg_correlation_single_is_zero(self) -> None:
        items = [
            {"strategy_id": "x", "family": "F", "parent_strategy_id": None},
        ]
        self.assertEqual(_avg_correlation(0, items), 0.0)


class PortfolioBrainMetaTests(unittest.TestCase):
    def test_meta_accepts_portfolio_brain(self) -> None:
        from backend.meta_ai import build_meta_status

        brain = {
            "top_priorities": [
                {
                    "strategy_id": "abc",
                    "brain_action": "INCREASE_ALLOCATION",
                    "brain_reason": "test reason",
                }
            ],
            "rotate_in": [],
            "rotate_out": [{"strategy_id": "x"}],
            "family_concentration": [
                {"family": "EMA_CROSS", "weight_share": 0.5, "within_target": False},
            ],
            "capital_shift_recommendations": [],
        }
        out = build_meta_status(
            fund_status={"summary": {"total_profit": 0.0, "average_score": 50.0}, "portfolio_state": "NORMAL"},
            factory_strategies=[],
            paper_status={"running_paper_bots": []},
            auto_status={"loops_completed": 0},
            portfolio_brain=brain,
        )
        self.assertIn("portfolio_brain", out["diagnostics"])
        self.assertGreaterEqual(len(out["recommendations"]), 1)


if __name__ == "__main__":
    unittest.main()
