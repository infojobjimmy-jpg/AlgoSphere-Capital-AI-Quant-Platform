"""Tests for Meta AI Control Engine (orchestration only; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.meta_ai_control_engine import (  # noqa: E402
    POSTURE_AGGRESSIVE,
    POSTURE_BALANCED,
    POSTURE_CAPITAL_PRESERVATION,
    POSTURE_CHAOTIC_SAFE_MODE,
    POSTURE_DEFENSIVE,
    build_meta_ai_control_status,
    build_meta_recommendations_payload,
    classify_system_posture,
    compute_control_confidence,
)


def _risk(**kwargs: object) -> dict:
    base = {
        "global_risk_score": 0.4,
        "risk_level": "MODERATE",
        "components": {
            "concentration_risk": 0.3,
            "correlation_risk": 0.3,
            "drawdown_risk": 0.3,
            "runner_risk": 0.3,
            "pipeline_risk": 0.3,
            "capital_risk": 0.3,
        },
    }
    base.update(kwargs)
    return base


def _reg(**kwargs: object) -> dict:
    base = {"current_regime": "TRANSITIONAL", "confidence_score": 0.5}
    base.update(kwargs)
    return base


def _fund_st(**kwargs: object) -> dict:
    return {"portfolio_state": "NORMAL", **kwargs}


def _fund_alloc(**kwargs: object) -> dict:
    return {"drawdown": 0.1, **kwargs}


def _mem(**kwargs: object) -> dict:
    return {"memory_entries": 100, "memory_health": "GOOD", "update_count": 2, **kwargs}


def _perf(**kwargs: object) -> dict:
    return {
        "runner_success_rate": 0.7,
        "runner_fail_rate": 0.1,
        "pipeline_throughput": 0.6,
        "total_jobs": 20,
        **kwargs,
    }


def _mr(**kwargs: object) -> dict:
    return {
        "fleet_summary": {"runner_count": 3, "degraded_count": 0, "offline_count": 0},
        **kwargs,
    }


def _cap(**kwargs: object) -> dict:
    return {
        "total_capital": 10000.0,
        "allocated": 3000.0,
        "free": 7000.0,
        "risk_usage": 0.3,
        **kwargs,
    }


def _port() -> dict:
    return {"brain": {"rotate_in": [], "rotate_out": [], "family_concentration": []}, "allocations": []}


def _legacy() -> dict:
    return {"recommendations": ["Legacy hint for testing."]}


class MetaAiControlEngineTests(unittest.TestCase):
    def test_posture_trending_low_risk_aggressive(self) -> None:
        p, _ = classify_system_posture(
            global_risk_full=_risk(global_risk_score=0.25, risk_level="LOW"),
            regime_status=_reg(current_regime="TRENDING"),
            fund_status=_fund_st(),
            fund_allocation_status=_fund_alloc(),
        )
        self.assertEqual(p, POSTURE_AGGRESSIVE)

    def test_posture_chaotic_high_defensive(self) -> None:
        p, _ = classify_system_posture(
            global_risk_full=_risk(global_risk_score=0.55, risk_level="HIGH"),
            regime_status=_reg(current_regime="CHAOTIC"),
            fund_status=_fund_st(),
            fund_allocation_status=_fund_alloc(),
        )
        self.assertIn(p, (POSTURE_DEFENSIVE, POSTURE_CHAOTIC_SAFE_MODE))

    def test_posture_capital_preservation_fund(self) -> None:
        p, _ = classify_system_posture(
            global_risk_full=_risk(global_risk_score=0.35, risk_level="MODERATE"),
            regime_status=_reg(current_regime="TRENDING"),
            fund_status=_fund_st(portfolio_state="DEFENSIVE"),
            fund_allocation_status=_fund_alloc(drawdown=0.2),
        )
        self.assertEqual(p, POSTURE_CAPITAL_PRESERVATION)

    def test_confidence_in_range(self) -> None:
        c, _ = compute_control_confidence(
            global_risk_full=_risk(),
            memory_payload=_mem(),
            performance_system=_perf(),
            multi_runner_status=_mr(),
        )
        self.assertGreaterEqual(c, 0.0)
        self.assertLessEqual(c, 1.0)

    def test_confidence_low_jobs(self) -> None:
        c_hi, _ = compute_control_confidence(
            global_risk_full=_risk(),
            memory_payload=_mem(),
            performance_system=_perf(total_jobs=30),
            multi_runner_status=_mr(),
        )
        c_lo, _ = compute_control_confidence(
            global_risk_full=_risk(),
            memory_payload=_mem(),
            performance_system=_perf(total_jobs=0),
            multi_runner_status=_mr(),
        )
        self.assertLessEqual(c_lo, c_hi)

    def test_build_full_payload(self) -> None:
        out = build_meta_ai_control_status(
            global_risk_full=_risk(),
            regime_status=_reg(current_regime="RANGING"),
            portfolio_allocation=_port(),
            memory_payload=_mem(),
            performance_system=_perf(),
            multi_runner_status=_mr(),
            fund_allocation_status=_fund_alloc(),
            capital_status=_cap(),
            fund_status=_fund_st(),
            legacy_meta=_legacy(),
        )
        self.assertIn("system_posture", out)
        self.assertIn(out["system_posture"], (POSTURE_BALANCED, POSTURE_AGGRESSIVE, POSTURE_DEFENSIVE, POSTURE_CAPITAL_PRESERVATION, POSTURE_CHAOTIC_SAFE_MODE))
        self.assertIn("confidence", out)
        self.assertIsInstance(out["reasoning"], list)
        self.assertIsInstance(out["recommendations"], list)
        self.assertIn("risk", out["diagnostics"])
        self.assertIn("portfolio_brain", out["diagnostics"])

    def test_empty_like_inputs_safe(self) -> None:
        out = build_meta_ai_control_status(
            global_risk_full={"global_risk_score": 0.0, "risk_level": "LOW", "components": {}},
            regime_status={"current_regime": "TRANSITIONAL"},
            portfolio_allocation={"brain": {}},
            memory_payload={"memory_entries": 0, "memory_health": "SEEDING", "update_count": 0},
            performance_system={"runner_fail_rate": 0.0, "pipeline_throughput": 0.5, "total_jobs": 0},
            multi_runner_status={"fleet_summary": {}},
            fund_allocation_status={"drawdown": 0.0},
            capital_status={},
            fund_status={"portfolio_state": "NORMAL"},
            legacy_meta={"recommendations": []},
        )
        self.assertIn("system_posture", out)
        self.assertGreaterEqual(float(out["confidence"]), 0.0)

    def test_recommendations_payload(self) -> None:
        ctrl = build_meta_ai_control_status(
            global_risk_full=_risk(),
            regime_status=_reg(),
            portfolio_allocation=_port(),
            memory_payload=_mem(),
            performance_system=_perf(),
            multi_runner_status=_mr(),
            fund_allocation_status=_fund_alloc(),
            capital_status=_cap(),
            fund_status=_fund_st(),
            legacy_meta=_legacy(),
        )
        rp = build_meta_recommendations_payload(ctrl)
        self.assertIn("recommendations", rp)
        self.assertEqual(rp.get("system_posture"), ctrl.get("system_posture"))


class MetaAiControlMainTests(unittest.TestCase):
    def test_meta_endpoints(self) -> None:
        from backend.main import get_meta_ai_control_status, get_meta_ai_recommendations

        st = get_meta_ai_control_status()
        self.assertIn("system_posture", st)
        self.assertIn("diagnostics", st)
        rc = get_meta_ai_recommendations()
        self.assertIn("recommendations", rc)


if __name__ == "__main__":
    unittest.main()
