"""Tests for Long Term Memory Engine (learning memory only; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.long_term_memory_engine import (  # noqa: E402
    ST_HEALTH_SEEDING,
    aggregate_evolution_memory,
    aggregate_family_memory,
    aggregate_regime_memory,
    aggregate_strategy_memory,
    build_learning_insights,
    build_memory_status_payload,
    count_memory_entries,
    evolution_success_rate_for_strategy,
    memory_health,
    run_memory_update_cycle,
)


def _sample_state() -> dict:
    return {
        "version": 1,
        "last_update": "2025-01-01T00:00:00+00:00",
        "update_count": 3,
        "strategy_observations": {
            "sid-a": [
                {
                    "at": "2025-01-01T00:00:00+00:00",
                    "regime": "TRENDING",
                    "performance_score": 0.7,
                    "success_rate": 0.8,
                },
                {
                    "at": "2025-01-02T00:00:00+00:00",
                    "regime": "RANGING",
                    "performance_score": 0.5,
                    "success_rate": 0.6,
                },
            ],
            "sid-b": [
                {
                    "at": "2025-01-01T00:00:00+00:00",
                    "regime": "CHAOTIC",
                    "performance_score": 0.3,
                    "success_rate": 0.4,
                }
            ],
        },
        "regime_observations": [
            {
                "at": "2025-01-01T00:00:00+00:00",
                "regime": "TRENDING",
                "mean_strategy_performance": 0.55,
            },
            {
                "at": "2025-01-02T00:00:00+00:00",
                "regime": "TRENDING",
                "mean_strategy_performance": 0.62,
            },
        ],
        "risk_snapshots": [{"at": "t", "global_risk_score": 0.4, "risk_level": "MODERATE"}],
        "evolution_snapshots": [{"at": "t", "weak_count": 2, "strong_count": 1, "loops_completed": 0}],
    }


def _factory() -> list[dict]:
    return [
        {"strategy_id": "sid-a", "family": "MOMENTUM", "generation": 0, "fitness_score": 60.0},
        {"strategy_id": "sid-b", "family": "MEAN_REVERSION", "generation": 0, "fitness_score": 40.0},
        {
            "strategy_id": "sid-c",
            "family": "MOMENTUM",
            "generation": 1,
            "parent_strategy_id": "sid-a",
            "fitness_score": 58.0,
        },
    ]


class LongTermMemoryEngineTests(unittest.TestCase):
    def test_aggregate_strategy_memory(self) -> None:
        st = _sample_state()
        rows = aggregate_strategy_memory(st, _factory())
        self.assertTrue(any(r["strategy_id"] == "sid-a" for r in rows))
        a = next(r for r in rows if r["strategy_id"] == "sid-a")
        self.assertAlmostEqual(a["avg_performance"], 0.6, places=2)
        self.assertGreater(a["observation_count"], 0)

    def test_evolution_success_rate(self) -> None:
        r = evolution_success_rate_for_strategy("sid-a", _factory())
        self.assertEqual(r, 1.0)

    def test_aggregate_family_memory(self) -> None:
        st = _sample_state()
        strat = aggregate_strategy_memory(st, _factory())
        fam = aggregate_family_memory(strat)
        self.assertEqual(len(fam), 4)
        self.assertTrue(all("family" in x for x in fam))

    def test_aggregate_regime_memory(self) -> None:
        st = _sample_state()
        strat = aggregate_strategy_memory(st, _factory())
        reg = aggregate_regime_memory(st, strat)
        self.assertEqual(len(reg), 5)
        tr = next(x for x in reg if x["regime"] == "TRENDING")
        self.assertGreater(tr["snapshot_count"], 0)

    def test_learning_insights_non_empty(self) -> None:
        st = _sample_state()
        strat = aggregate_strategy_memory(st, _factory())
        fam = aggregate_family_memory(strat)
        reg = aggregate_regime_memory(st, strat)
        evo = aggregate_evolution_memory(st)
        risk = {"samples": 1, "avg_global_risk_score": 0.4}
        ins = build_learning_insights(strat, fam, reg, evo, risk)
        self.assertTrue(len(ins) >= 1)

    def test_memory_health_seeding(self) -> None:
        self.assertEqual(memory_health({"last_update": None, "update_count": 0}), ST_HEALTH_SEEDING)

    def test_count_memory_entries(self) -> None:
        n = count_memory_entries(_sample_state())
        self.assertGreater(n, 0)

    def test_build_memory_status_payload(self) -> None:
        p = build_memory_status_payload(_sample_state(), _factory())
        self.assertIn("memory_entries", p)
        self.assertIn("memory_health", p)
        self.assertIn("learning_insights", p)

    def test_run_memory_update_cycle_persists(self) -> None:
        bundle = {
            "regime_status": {
                "current_regime": "TRENDING",
                "confidence_score": 0.5,
                "favored_strategy_families": ["MOMENTUM"],
                "reduced_strategy_families": [],
            },
            "global_risk_full": {
                "global_risk_score": 0.33,
                "risk_level": "LOW",
                "components": {"drawdown_risk": 0.2},
            },
            "strategies_performance": [
                {"strategy_id": "z1", "performance_score": 0.55, "success_rate": 0.7},
            ],
            "factory_strategies": [{"strategy_id": "z1", "family": "EMA_CROSS", "generation": 0}],
            "evolution_candidates": {"weak_strategies": [], "strong_strategies": []},
            "evolution_lineage": {"lineage": []},
            "continuous_evolution_status": {"loops_completed": 0},
            "auto_loop_status": {"loops_completed": 0},
        }
        captured: list[str] = []

        def fake_save(state: dict) -> None:
            captured.append("ok")

        with patch("backend.long_term_memory_engine.load_memory_state", return_value=_empty_mem()):
            with patch("backend.long_term_memory_engine.save_memory_state", side_effect=fake_save):
                out = run_memory_update_cycle(bundle)
        self.assertTrue(out.get("ok"))
        self.assertEqual(captured, ["ok"])


def _empty_mem() -> dict:
    return {
        "version": 1,
        "last_update": None,
        "update_count": 0,
        "strategy_observations": {},
        "regime_observations": [],
        "risk_snapshots": [],
        "evolution_snapshots": [],
    }


class LongTermMemoryMainTests(unittest.TestCase):
    def test_memory_endpoints(self) -> None:
        from backend.main import (
            get_long_term_memory_status,
            get_memory_family_view,
            get_memory_regime_view,
            get_memory_strategy_view,
        )

        st = get_long_term_memory_status()
        self.assertIn("memory_entries", st)
        self.assertIn("memory_health", st)
        s = get_memory_strategy_view()
        self.assertIn("strategies", s)
        f = get_memory_family_view()
        self.assertIn("families", f)
        r = get_memory_regime_view()
        self.assertIn("regimes", r)


if __name__ == "__main__":
    unittest.main()
