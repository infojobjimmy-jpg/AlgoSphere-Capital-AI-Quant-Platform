"""Tests for Production Hardening Layer (stability only, no execution)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.production_hardening_engine import (  # noqa: E402
    HEALTH_CRITICAL,
    HEALTH_DEGRADED,
    HEALTH_HEALTHY,
    build_engine_health_map,
    build_errors_payload,
    build_snapshot_payload,
    build_system_health_payload,
    classify_system_health,
    record_error,
)


class ProductionHardeningEngineTests(unittest.TestCase):
    def test_engine_health_tracking(self) -> None:
        engines = build_engine_health_map(
            autonomous_status={"state": "AUTONOMOUS_RUNNING", "errors": []},
            meta_status={"confidence": 0.7},
            evolution_status={"state": "EVOLUTION_RUNNING"},
            runner_status={"counts": {"RUNNER_ACTIVE": 1}},
            risk_status={"risk_level": "MODERATE"},
            regime_status={"current_regime": "RANGING"},
            memory_status={"memory_health": "GOOD"},
        )
        self.assertEqual(engines["autonomous_engine"], HEALTH_HEALTHY)
        self.assertEqual(engines["risk_engine"], HEALTH_HEALTHY)
        self.assertEqual(classify_system_health(engines), HEALTH_HEALTHY)

    def test_health_degraded_and_critical(self) -> None:
        self.assertEqual(
            classify_system_health({"a": HEALTH_DEGRADED, "b": HEALTH_HEALTHY}),
            HEALTH_DEGRADED,
        )
        self.assertEqual(
            classify_system_health({"a": HEALTH_CRITICAL, "b": HEALTH_HEALTHY}),
            HEALTH_CRITICAL,
        )

    def test_snapshot_payload(self) -> None:
        snap = build_snapshot_payload(
            memory_state={"x": 1},
            portfolio_state={"y": 2},
            meta_state={"z": 3},
            evolution_state={"a": 4},
            autonomous_state={"b": 5},
        )
        self.assertIn("snapshot_at", snap)
        self.assertIn("memory_state", snap)
        self.assertTrue(snap["decision_layer_only"])

    def test_error_tracking(self) -> None:
        state = {"last_errors": [], "error_count": 0, "last_error": None}
        state = record_error(state, source="unit", message="boom")
        self.assertEqual(state["error_count"], 1)
        ep = build_errors_payload(state)
        self.assertEqual(ep["error_count"], 1)
        self.assertEqual(len(ep["recent_errors"]), 1)

    def test_system_health_payload(self) -> None:
        payload = build_system_health_payload(
            uptime_sec=12.3,
            engines={"autonomous_engine": HEALTH_DEGRADED},
            health_state={"last_errors": [], "error_count": 0, "last_error": None},
        )
        self.assertIn("system_health", payload)
        self.assertIn(payload["system_health"], {HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL})
        self.assertIn("uptime", payload)


class ProductionHardeningMainTests(unittest.TestCase):
    def test_main_handlers_exist(self) -> None:
        from backend.main import get_system_errors, get_system_health, post_system_snapshot

        h = get_system_health()
        self.assertIn("system_health", h)
        e = get_system_errors()
        self.assertIn("error_count", e)
        s = post_system_snapshot()
        self.assertTrue(s.get("ok"))

    def test_restart_safety_path(self) -> None:
        from backend.main import get_system_health

        with patch("backend.main.get_autonomous_fund_status", return_value={"state": "AUTONOMOUS_PAUSED", "errors": ["x"]}):
            with patch("backend.main.start_autonomous_fund_mode", return_value={"started": True}):
                h = get_system_health()
        self.assertIn("engines", h)


if __name__ == "__main__":
    unittest.main()
