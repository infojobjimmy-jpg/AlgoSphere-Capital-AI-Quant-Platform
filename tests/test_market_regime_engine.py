"""Tests for Market Regime Engine (read-only advisory; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.market_regime_engine import (  # noqa: E402
    REGIME_CHAOTIC,
    REGIME_RANGING,
    REGIME_TRENDING,
    REGIME_TRANSITIONAL,
    REGIME_VOLATILE,
    build_market_regime_payload,
    build_regime_recommendations_response,
)


def _base_global_risk(**overrides: object) -> dict:
    base = {
        "global_risk_score": 0.35,
        "risk_level": "MODERATE",
        "components": {
            "concentration_risk": 0.25,
            "correlation_risk": 0.3,
            "drawdown_risk": 0.28,
            "runner_risk": 0.22,
            "pipeline_risk": 0.3,
            "capital_risk": 0.15,
        },
    }
    base.update(overrides)
    return base


def _minimal_payload(**overrides: object) -> dict:
    p: dict = {
        "performance_system": {
            "runner_fail_rate": 0.08,
            "runner_success_rate": 0.72,
            "pipeline_throughput": 0.55,
            "total_jobs": 12,
        },
        "portfolio_allocation": {"brain": {}, "allocations": []},
        "fund_allocation_status": {"portfolio_return": 0.008, "drawdown": 0.12},
        "paper_status": {"running_paper_bots": [{"strategy_id": "a", "status": "PAPER_SUCCESS"}]},
        "global_risk_assessment": _base_global_risk(),
        "meta_status": {"system_health": "WARNING", "risk_mode": "NORMAL"},
        "factory_strategies": [
            {"strategy_id": "a", "family": "MOMENTUM"},
            {"strategy_id": "b", "family": "MEAN_REVERSION"},
        ],
        "strategies_performance": [
            {"strategy_id": "a", "performance_score": 0.72, "success_rate": 0.8},
            {"strategy_id": "b", "performance_score": 0.45, "success_rate": 0.5},
        ],
    }
    p.update(overrides)
    return p


class MarketRegimeEngineTests(unittest.TestCase):
    def test_confidence_in_range(self) -> None:
        out = build_market_regime_payload(**_minimal_payload())
        c = float(out["confidence_score"])
        self.assertGreaterEqual(c, 0.0)
        self.assertLessEqual(c, 1.0)

    def test_flags_always_true(self) -> None:
        out = build_market_regime_payload(**_minimal_payload())
        self.assertTrue(out["decision_layer_only"])
        self.assertTrue(out["demo_simulation_only"])

    def test_chaotic_when_critical_risk(self) -> None:
        out = build_market_regime_payload(
            **_minimal_payload(
                global_risk_assessment=_base_global_risk(risk_level="CRITICAL", global_risk_score=0.88),
            )
        )
        self.assertEqual(out["current_regime"], REGIME_CHAOTIC)
        self.assertIn("MOMENTUM", out["paused_strategy_families"])

    def test_trending_favors_momentum_families(self) -> None:
        out = build_market_regime_payload(
            **_minimal_payload(
                fund_allocation_status={"portfolio_return": 0.04, "drawdown": 0.05},
                global_risk_assessment=_base_global_risk(
                    global_risk_score=0.22,
                    risk_level="LOW",
                    components={
                        "concentration_risk": 0.2,
                        "correlation_risk": 0.12,
                        "drawdown_risk": 0.15,
                        "runner_risk": 0.12,
                        "pipeline_risk": 0.18,
                        "capital_risk": 0.15,
                    },
                ),
                performance_system={
                    "runner_fail_rate": 0.02,
                    "runner_success_rate": 0.9,
                    "pipeline_throughput": 0.85,
                    "total_jobs": 40,
                },
            )
        )
        self.assertEqual(out["current_regime"], REGIME_TRENDING)
        self.assertIn("MOMENTUM", out["favored_strategy_families"])
        self.assertIn("MEAN_REVERSION", out["reduced_strategy_families"])

    def test_ranging_reduces_momentum(self) -> None:
        out = build_market_regime_payload(
            **_minimal_payload(
                fund_allocation_status={"portfolio_return": 0.0, "drawdown": 0.08},
                global_risk_assessment=_base_global_risk(
                    global_risk_score=0.28,
                    risk_level="LOW",
                    components={
                        "concentration_risk": 0.22,
                        "correlation_risk": 0.72,
                        "drawdown_risk": 0.2,
                        "runner_risk": 0.18,
                        "pipeline_risk": 0.25,
                        "capital_risk": 0.2,
                    },
                ),
                performance_system={
                    "runner_fail_rate": 0.05,
                    "runner_success_rate": 0.75,
                    "pipeline_throughput": 0.35,
                    "total_jobs": 25,
                },
            )
        )
        self.assertEqual(out["current_regime"], REGIME_RANGING)
        self.assertIn("MEAN_REVERSION", out["favored_strategy_families"])
        self.assertIn("MOMENTUM", out["reduced_strategy_families"])

    def test_volatile_favors_session_breakout(self) -> None:
        out = build_market_regime_payload(
            **_minimal_payload(
                global_risk_assessment=_base_global_risk(
                    global_risk_score=0.38,
                    risk_level="MODERATE",
                    components={
                        "concentration_risk": 0.3,
                        "correlation_risk": 0.35,
                        "drawdown_risk": 0.68,
                        "runner_risk": 0.62,
                        "pipeline_risk": 0.4,
                        "capital_risk": 0.25,
                    },
                ),
                performance_system={
                    "runner_fail_rate": 0.42,
                    "runner_success_rate": 0.45,
                    "pipeline_throughput": 0.22,
                    "total_jobs": 30,
                },
            )
        )
        self.assertEqual(out["current_regime"], REGIME_VOLATILE)
        self.assertIn("SESSION_BREAKOUT", out["favored_strategy_families"])

    def test_sparse_data_safe_transitional(self) -> None:
        out = build_market_regime_payload(
            **_minimal_payload(
                fund_allocation_status={"portfolio_return": 0.0, "drawdown": 0.2},
                performance_system={
                    "runner_fail_rate": 0.0,
                    "runner_success_rate": 0.0,
                    "pipeline_throughput": 0.5,
                    "total_jobs": 0,
                },
                paper_status={"running_paper_bots": []},
                factory_strategies=[],
                strategies_performance=[],
                global_risk_assessment=_base_global_risk(
                    global_risk_score=0.41,
                    risk_level="MODERATE",
                    components={
                        "concentration_risk": 0.27,
                        "correlation_risk": 0.27,
                        "drawdown_risk": 0.27,
                        "runner_risk": 0.26,
                        "pipeline_risk": 0.28,
                        "capital_risk": 0.26,
                    },
                ),
            )
        )
        self.assertLessEqual(float(out["confidence_score"]), 0.55)
        self.assertTrue(
            any("No runner" in str(x) or "No paper" in str(x) for x in out["regime_reasoning"]),
            msg=f"expected sparse-data hints in reasoning, got {out['regime_reasoning']!r}",
        )
        self.assertIn(out["current_regime"], (REGIME_TRANSITIONAL, REGIME_TRENDING, REGIME_RANGING))

    def test_recommendations_actions(self) -> None:
        status = build_market_regime_payload(**_minimal_payload())
        rec = build_regime_recommendations_response(status)
        self.assertIn("recommendations", rec)
        actions = {r["action"] for r in rec["recommendations"]}
        self.assertTrue(actions <= {"FAVOR", "REDUCE", "PAUSE"})


class MarketRegimeMainHandlerTests(unittest.TestCase):
    def test_regime_endpoints(self) -> None:
        from backend.main import get_market_regime_recommendations, get_market_regime_status

        st = get_market_regime_status()
        self.assertIn("current_regime", st)
        self.assertIn("confidence_score", st)
        self.assertIn("favored_strategy_families", st)
        rc = get_market_regime_recommendations()
        self.assertIn("recommendations", rc)


if __name__ == "__main__":
    unittest.main()
