"""Tests for Global Risk Engine (decision layer; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.global_risk_engine import (  # noqa: E402
    RISK_CRITICAL,
    RISK_HIGH,
    RISK_LOW,
    RISK_MODERATE,
    build_global_risk_alerts_payload,
    build_global_risk_assessment,
    build_global_risk_status_payload,
    risk_level_from_score,
)


def _minimal_inputs(**overrides: object) -> dict:
    base = {
        "portfolio_allocation": {"count": 0, "allocations": [], "brain": {}},
        "fund_allocation_status": {"risk_score": 0.2, "drawdown": 0.1},
        "performance_system": {
            "runner_fail_rate": 0.1,
            "runner_success_rate": 0.5,
            "pipeline_throughput": 0.4,
        },
        "multi_runner_status": {
            "fleet_summary": {
                "runner_count": 2,
                "degraded_count": 0,
                "offline_count": 0,
            }
        },
        "recovery_status": {"recovery_state": "RECOVERY_IDLE"},
        "capital_status": {"risk_usage": 0.3, "allocated": 3000, "total_capital": 10000},
        "review_status": {"counts": {"PENDING_REVIEW": 10}},
        "paper_status": {"summary": {"drawdown": 100.0}},
        "factory_candidate_count": 100,
    }
    base.update(overrides)
    return base


class GlobalRiskEngineTests(unittest.TestCase):
    def test_risk_level_thresholds(self) -> None:
        self.assertEqual(risk_level_from_score(0.0), RISK_LOW)
        self.assertEqual(risk_level_from_score(0.29), RISK_LOW)
        self.assertEqual(risk_level_from_score(0.30), RISK_MODERATE)
        self.assertEqual(risk_level_from_score(0.49), RISK_MODERATE)
        self.assertEqual(risk_level_from_score(0.50), RISK_HIGH)
        self.assertEqual(risk_level_from_score(0.69), RISK_HIGH)
        self.assertEqual(risk_level_from_score(0.70), RISK_CRITICAL)
        self.assertEqual(risk_level_from_score(1.0), RISK_CRITICAL)

    def test_global_score_in_range(self) -> None:
        full = build_global_risk_assessment(**_minimal_inputs())
        self.assertGreaterEqual(full["global_risk_score"], 0.0)
        self.assertLessEqual(full["global_risk_score"], 1.0)
        self.assertIn("concentration_risk", full["components"])

    def test_high_runner_fail_increases_runner_risk(self) -> None:
        low = build_global_risk_assessment(**_minimal_inputs())["components"]["runner_risk"]
        high = build_global_risk_assessment(
            **_minimal_inputs(
                performance_system={
                    "runner_fail_rate": 0.55,
                    "runner_success_rate": 0.2,
                    "pipeline_throughput": 0.4,
                }
            )
        )["components"]["runner_risk"]
        self.assertGreater(high, low)

    def test_recommendations_when_stressed(self) -> None:
        full = build_global_risk_assessment(
            **_minimal_inputs(
                capital_status={"risk_usage": 0.95, "allocated": 9500, "total_capital": 10000},
                fund_allocation_status={"risk_score": 0.9, "drawdown": 0.85},
            )
        )
        self.assertTrue(any("capital" in r.lower() or "allocation" in r.lower() for r in full["recommendations"]))

    def test_alerts_for_critical_components(self) -> None:
        full = build_global_risk_assessment(
            **_minimal_inputs(
                portfolio_allocation={
                    "count": 1,
                    "allocations": [{"avg_correlation": 0.95}],
                    "brain": {},
                }
            )
        )
        alerts = full.get("alerts") or []
        self.assertTrue(any(a.get("severity") == "CRITICAL" for a in alerts))

    def test_status_and_alerts_payloads(self) -> None:
        full = build_global_risk_assessment(**_minimal_inputs())
        st = build_global_risk_status_payload(full)
        self.assertNotIn("alerts", st)
        self.assertIn("recommendations", st)
        al = build_global_risk_alerts_payload(full)
        self.assertIn("alerts", al)


class GlobalRiskMainHandlerTests(unittest.TestCase):
    def test_get_global_risk_endpoints(self) -> None:
        from backend.main import get_global_risk_alerts, get_global_risk_status

        st = get_global_risk_status()
        self.assertIn("global_risk_score", st)
        self.assertIn("risk_level", st)
        self.assertIn("components", st)
        al = get_global_risk_alerts()
        self.assertIn("alerts", al)


if __name__ == "__main__":
    unittest.main()
