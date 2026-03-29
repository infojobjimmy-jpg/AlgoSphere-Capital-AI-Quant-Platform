"""Tests for Self-Improving Meta AI engine (orchestration learning only)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.self_improving_meta_engine import (  # noqa: E402
    build_learning_entry,
    build_learning_insights_payload,
    build_learning_status_payload,
    compute_outcome_score,
    run_learning_update_cycle,
    update_learning_state,
)


def _empty_state() -> dict:
    return {"version": 1, "last_update": None, "meta_history": []}


def _snap() -> dict:
    return {
        "control_status": {"system_posture": "BALANCED", "confidence": 0.6},
        "global_risk_status": {"risk_level": "MODERATE", "global_risk_score": 0.35},
        "regime_status": {"current_regime": "RANGING"},
        "memory_status": {"memory_health": "GOOD"},
        "performance_system": {"runner_success_rate": 0.7, "pipeline_throughput": 0.6},
        "multi_runner_status": {"fleet_summary": {"runner_count": 3, "degraded_count": 0, "offline_count": 0}},
        "fund_status": {"portfolio_state": "NORMAL"},
        "fund_allocation_status": {"drawdown": 0.12},
    }


class SelfImprovingMetaEngineTests(unittest.TestCase):
    def test_outcome_scoring_direction(self) -> None:
        prev = {"performance_score": 0.4, "risk_score": 0.6, "drawdown": 0.3}
        better = compute_outcome_score(prev_entry=prev, performance_score=0.7, risk_score=0.3, drawdown=0.2)
        worse = compute_outcome_score(prev_entry=prev, performance_score=0.2, risk_score=0.8, drawdown=0.5)
        self.assertGreater(better, 0.0)
        self.assertLess(worse, 0.0)

    def test_learning_entry_shape(self) -> None:
        s = _snap()
        e = build_learning_entry(prev_entry=None, **s)
        for key in (
            "timestamp",
            "posture",
            "confidence",
            "risk_level",
            "regime",
            "memory_health",
            "performance_score",
            "runner_health",
            "portfolio_state",
            "outcome_score",
        ):
            self.assertIn(key, e)

    def test_update_and_aggregation(self) -> None:
        state = _empty_state()
        s = _snap()
        e1 = build_learning_entry(prev_entry=None, **s)
        state = update_learning_state(state, e1)
        status = build_learning_status_payload(state)
        self.assertEqual(status["learning_entries"], 1)
        insights = build_learning_insights_payload(state)
        self.assertIn("top_patterns", insights)

    def test_empty_safety(self) -> None:
        status = build_learning_status_payload(_empty_state())
        self.assertEqual(status["learning_entries"], 0)
        self.assertIn(status["learning_health"], {"SEEDING", "LEARNING", "MATURE"})
        insights = build_learning_insights_payload(_empty_state())
        self.assertTrue(len(insights["top_patterns"]) >= 1)

    def test_run_learning_update_cycle(self) -> None:
        s = _snap()
        with patch("backend.self_improving_meta_engine.load_learning_state", return_value=_empty_state()):
            with patch("backend.self_improving_meta_engine.save_learning_state") as saver:
                out = run_learning_update_cycle(**s)
        self.assertTrue(out.get("ok"))
        self.assertTrue(saver.called)
        self.assertIn("learning_entries", out)


class SelfImprovingMetaMainTests(unittest.TestCase):
    def test_learning_endpoints_handlers(self) -> None:
        from backend.main import (
            get_meta_learning_insights,
            get_meta_learning_status,
            post_meta_learning_update,
        )

        st = get_meta_learning_status()
        self.assertIn("learning_entries", st)
        ins = get_meta_learning_insights()
        self.assertIn("top_patterns", ins)
        up = post_meta_learning_update()
        self.assertIn("ok", up)


if __name__ == "__main__":
    unittest.main()
