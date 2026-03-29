"""Unit tests for local alerting rules and summary (no HTTP)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.alerting_engine import (  # noqa: E402
    apply_acknowledgements,
    build_alerts,
    build_alerts_summary,
)


def _minimal_baseline(**overrides: object) -> dict:
    base = {
        "meta_status": {"system_health": "GOOD", "risk_mode": "NORMAL"},
        "capital_status": {
            "total_capital": 10000.0,
            "free": 5000.0,
            "risk_usage": 0.1,
            "allocated": 5000.0,
        },
        "fund_status": {"portfolio_state": "NORMAL"},
        "report_summary": {"portfolio_state": "NORMAL", "warnings": []},
        "portfolio_allocation": {"total_allocated_percent": 0.0, "allocations": []},
        "paper_status": {"running_paper_bots": []},
        "review_status": {"counts": {"PENDING_REVIEW": 0}},
        "demo_status": {"counts": {"DEMO_REJECTED": 0}},
        "executor_status": {"counts": {"EXECUTOR_READY": 0, "EXECUTOR_REJECTED": 0}},
        "runner_status": {"counts": {"RUNNER_FAILED": 0}},
        "runner_jobs": {"jobs": []},
        "auto_status": {"last_error": None},
        "operator_console": None,
        "runner_stale_no_jobs": False,
    }
    base.update(overrides)
    return base


class AlertingEngineTests(unittest.TestCase):
    def test_risk_usage_critical_alert(self) -> None:
        kwargs = _minimal_baseline(
            capital_status={
                "total_capital": 10000.0,
                "free": 100.0,
                "risk_usage": 0.96,
                "allocated": 9900.0,
            }
        )
        alerts = build_alerts(**kwargs)
        crit = [a for a in alerts if a["severity"] == "CRITICAL"]
        self.assertTrue(any("risk usage" in str(a["title"]).lower() for a in crit))

    def test_auto_loop_error_critical(self) -> None:
        kwargs = _minimal_baseline(auto_status={"last_error": "connection reset"})
        alerts = build_alerts(**kwargs)
        self.assertTrue(any(a.get("title") == "Auto loop reported an error" for a in alerts))
        self.assertTrue(any(a.get("severity") == "CRITICAL" for a in alerts))

    def test_review_pending_warning_threshold(self) -> None:
        kwargs = _minimal_baseline(
            review_status={"counts": {"PENDING_REVIEW": 150}},
        )
        alerts = build_alerts(**kwargs)
        titles = {a["title"] for a in alerts}
        self.assertIn("Elevated review backlog", titles)

    def test_runner_failures_warning(self) -> None:
        kwargs = _minimal_baseline(
            runner_status={"counts": {"RUNNER_FAILED": 2}},
        )
        alerts = build_alerts(**kwargs)
        self.assertTrue(any(a.get("title") == "Runner jobs failed" for a in alerts))

    def test_summary_counts_active_only(self) -> None:
        kwargs = _minimal_baseline(
            meta_status={"system_health": "CRITICAL", "risk_mode": "NORMAL"},
        )
        alerts = build_alerts(**kwargs)
        summary = build_alerts_summary(alerts)
        self.assertGreaterEqual(summary["critical_count"], 1)
        self.assertEqual(
            summary["total_alerts"],
            summary["critical_count"] + summary["warning_count"] + summary["info_count"],
        )
        self.assertIsInstance(summary["top_active_alerts"], list)

    def test_ack_marks_inactive(self) -> None:
        kwargs = _minimal_baseline(
            meta_status={"system_health": "WARNING", "risk_mode": "NORMAL"},
        )
        alerts = build_alerts(**kwargs)
        self.assertTrue(alerts)
        aid = str(alerts[0]["alert_id"])
        merged = apply_acknowledgements(alerts, {aid})
        self.assertFalse(next(a for a in merged if a["alert_id"] == aid)["active"])
        summary = build_alerts_summary(merged)
        self.assertLessEqual(summary["total_alerts"], len(merged))


if __name__ == "__main__":
    unittest.main(verbosity=2)
