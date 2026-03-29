"""Tests for Live Control Engine (decision layer only, no execution)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.live_control_engine import (  # noqa: E402
    LiveControlEngine,
    build_live_control_payload,
    compute_live_bot_signal,
)


def _bot(**kwargs: object) -> dict[str, object]:
    base = {
        "name": "ctrader_first_live_bot",
        "score": 0.70,
        "win_rate": 0.56,
        "drawdown": 120.0,
        "effective_capital": 4000.0,
        "capital_alloc": 3500.0,
    }
    base.update(kwargs)
    return base


def _ctx(**kwargs: object) -> dict[str, object]:
    base = {
        "bots": [_bot()],
        "risk_status": {"risk_level": "MODERATE", "global_risk_score": 0.35},
        "meta_status": {"system_posture": "BALANCED"},
        "regime_status": {"current_regime": "TRANSITIONAL"},
        "live_data_context": {"data_health": "LIVE_HEALTHY"},
        "portfolio_allocation": {},
        "system_health": {"system_health": "HEALTHY"},
        "cluster_status": {"cluster_health": "HEALTHY"},
    }
    base.update(kwargs)
    return base


class LiveControlEngineUnitTests(unittest.TestCase):
    def test_unhealthy_system_drives_kill_or_reduce(self) -> None:
        sig = compute_live_bot_signal(
            _bot(),
            _ctx(system_health={"system_health": "CRITICAL"}),
            updated_at="t",
        )
        self.assertIn(sig["control_state"], {"KILL", "REDUCE"})
        self.assertIn(sig["recommended_action"], {"STOP", "LOWER_VOLUME"})

    def test_strong_aligned_context_can_boost(self) -> None:
        sig = compute_live_bot_signal(
            _bot(score=0.82, win_rate=0.61, drawdown=90.0),
            _ctx(
                risk_status={"risk_level": "LOW", "global_risk_score": 0.20},
                meta_status={"system_posture": "AGGRESSIVE"},
                regime_status={"current_regime": "TRENDING"},
                live_data_context={"data_health": "LIVE_HEALTHY"},
            ),
            updated_at="t",
        )
        self.assertIn(sig["control_state"], {"MONITOR", "BOOST"})
        self.assertIn(sig["recommended_action"], {"NO_CHANGE", "INCREASE_VOLUME"})

    def test_target_volume_changes_by_state(self) -> None:
        k = compute_live_bot_signal(_bot(), _ctx(system_health={"system_health": "CRITICAL"}), updated_at="t")
        m = compute_live_bot_signal(_bot(), _ctx(), updated_at="t")
        b = compute_live_bot_signal(
            _bot(score=0.9, win_rate=0.65),
            _ctx(
                risk_status={"risk_level": "LOW"},
                meta_status={"system_posture": "AGGRESSIVE"},
                regime_status={"current_regime": "TRENDING"},
            ),
            updated_at="t",
        )
        self.assertEqual(k["target_volume"], 0)
        self.assertGreaterEqual(m["target_volume"], 0)
        self.assertGreaterEqual(b["target_volume"], m["target_volume"])

    def test_recompute_behavior_changes_with_context(self) -> None:
        calls = {"n": 0}

        def provider() -> dict[str, object]:
            calls["n"] += 1
            if calls["n"] == 1:
                return _ctx(risk_status={"risk_level": "HIGH"})
            return _ctx(
                risk_status={"risk_level": "LOW"},
                meta_status={"system_posture": "AGGRESSIVE"},
                regime_status={"current_regime": "TRENDING"},
            )

        eng = LiveControlEngine(context_provider=provider, interval_sec=10, persist_state=False)
        p1 = eng.recompute_once()
        p2 = eng.recompute_once()
        self.assertEqual(p1["live_engine"], True)
        self.assertEqual(p2["live_engine"], True)
        self.assertNotEqual(
            p1["signals"][0]["control_state"],
            p2["signals"][0]["control_state"],
        )

    def test_payload_shape_contains_live_fields(self) -> None:
        p = build_live_control_payload(_ctx(), interval_sec=12)
        self.assertEqual(p["live_engine"], True)
        self.assertIn("updated_at", p)
        self.assertIn("signals", p)
        self.assertIn("entriesEnabled", p["signals"][0])
        self.assertIn("target_volume", p["signals"][0])
        self.assertIn("reasoning", p["signals"][0])


class LiveControlMainHandlerTests(unittest.TestCase):
    def test_control_signals_uses_live_engine_payload(self) -> None:
        import backend.main as main

        class FakeEngine:
            def __init__(self) -> None:
                self.calls = 0

            def get_payload(self) -> dict[str, object]:
                self.calls += 1
                if self.calls == 1:
                    return {"count": 0, "signals": [], "live_engine": True}
                return {
                    "count": 1,
                    "signals": [
                        {
                            "name": "x",
                            "control_state": "MONITOR",
                            "control_active": True,
                            "effective_capital": 1000.0,
                            "entriesEnabled": True,
                            "target_volume": 1000,
                            "recommended_action": "NO_CHANGE",
                            "reasoning": ["ok"],
                            "updated_at": "t",
                        }
                    ],
                    "live_engine": True,
                }

            def recompute_once(self) -> dict[str, object]:
                return {
                    "count": 1,
                    "signals": [
                        {
                            "name": "x",
                            "control_state": "MONITOR",
                            "control_active": True,
                            "effective_capital": 1000.0,
                            "entriesEnabled": True,
                            "target_volume": 1000,
                            "recommended_action": "NO_CHANGE",
                            "reasoning": ["ok"],
                            "updated_at": "t2",
                        }
                    ],
                    "live_engine": True,
                }

        fake = FakeEngine()
        with patch.object(main, "_get_live_control_engine", return_value=fake):
            out = main.get_control_signals()
        self.assertEqual(out.get("live_engine"), True)
        self.assertEqual(int(out.get("count", 0)), 1)
        self.assertIn("target_volume", out["signals"][0])


if __name__ == "__main__":
    unittest.main()
