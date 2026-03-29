"""Tests for AI Strategy Evolution Engine (mutations, lineage, API; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.ai_strategy_evolution_engine import (  # noqa: E402
    MAX_PARENT_GENERATION,
    WEAK_MIN_TOTAL_RUNS,
    WEAK_PERFORMANCE_MAX,
    build_evolution_candidates_payload,
    build_lineage_payload,
    evolve_strong_variant,
    evolve_weak_variant,
    extract_mutation_type,
    is_strong_strategy,
    is_weak_strategy,
    run_evolution_batch,
    variant_summary_row,
)


class AIStrategyEvolutionEngineTests(unittest.TestCase):
    def test_weak_strong_classification(self) -> None:
        self.assertTrue(
            is_weak_strategy(
                {"performance_score": 0.4, "total_runs": 3, "success_rate": 0.3}
            )
        )
        self.assertFalse(
            is_weak_strategy(
                {"performance_score": 0.4, "total_runs": 2, "success_rate": 0.0}
            )
        )
        self.assertFalse(
            is_weak_strategy(
                {"performance_score": 0.5, "total_runs": 5, "success_rate": 0.5}
            )
        )
        self.assertTrue(
            is_strong_strategy(
                {"performance_score": 0.71, "total_runs": 5, "success_rate": 0.61}
            )
        )
        self.assertFalse(
            is_strong_strategy(
                {"performance_score": 0.71, "total_runs": 5, "success_rate": 0.5}
            )
        )

    def test_candidates_payload_joins_perf(self) -> None:
        factory = [
            {
                "strategy_id": "a1",
                "family": "EMA_CROSS",
                "generation": 0,
                "parameters": {},
            },
            {
                "strategy_id": "a2",
                "family": "EMA_CROSS",
                "generation": 0,
                "parameters": {},
            },
        ]
        perf = [
            {
                "strategy_id": "a1",
                "performance_score": 0.3,
                "total_runs": 4,
                "success_rate": 0.25,
            },
            {
                "strategy_id": "a2",
                "performance_score": 0.85,
                "total_runs": 10,
                "success_rate": 0.8,
            },
        ]
        out = build_evolution_candidates_payload(factory, perf)
        weak_ids = {x["strategy_id"] for x in out["weak_strategies"]}
        strong_ids = {x["strategy_id"] for x in out["strong_strategies"]}
        self.assertIn("a1", weak_ids)
        self.assertIn("a2", strong_ids)
        self.assertNotIn("a2", weak_ids)

    def test_weak_mutation_deterministic(self) -> None:
        parent = {
            "strategy_id": "parent1",
            "family": "EMA_CROSS",
            "generation": 0,
            "parameters": {
                "ema_fast": 10,
                "ema_slow": 30,
                "tp_sl_ratio": 2.0,
                "risk_multiplier": 1.0,
                "session_filter": "ASIA",
                "volatility_filter": "LOW",
            },
            "fitness_score": 55.0,
            "expected_win_rate": 0.5,
            "expected_drawdown": 200.0,
            "risk_profile": "MEDIUM",
            "status": "TESTING",
        }
        c1, _ = evolve_weak_variant(parent, seed=99)
        c2, _ = evolve_weak_variant(parent, seed=99)
        self.assertNotEqual(c1["strategy_id"], c2["strategy_id"])
        self.assertEqual(c1["parameters"], c2["parameters"])
        self.assertEqual(c1["mutation_note"], c2["mutation_note"])
        self.assertNotEqual(c1["strategy_id"], parent["strategy_id"])
        self.assertEqual(c1["parent_strategy_id"], "parent1")
        self.assertEqual(c1["generation"], 1)
        self.assertEqual(c1["origin_type"], "AI_EVOLVED_WEAK")
        self.assertIn("timeframe", c1["parameters"])
        mt = extract_mutation_type(str(c1["mutation_note"]))
        self.assertEqual(mt, "WEAK_PARAM_TIME_FILTER_VARIANT")

    def test_strong_mutation_new_id(self) -> None:
        parent = {
            "strategy_id": "p2",
            "family": "SESSION_BREAKOUT",
            "generation": 2,
            "parameters": {"breakout_window": 10, "confirmation_bars": 2},
            "fitness_score": 70.0,
            "expected_win_rate": 0.6,
            "expected_drawdown": 150.0,
            "risk_profile": "LOW",
            "status": "CANDIDATE",
        }
        child, _ = evolve_strong_variant(parent, seed=1)
        self.assertNotEqual(child["strategy_id"], parent["strategy_id"])
        self.assertEqual(child["origin_type"], "AI_EVOLVED_STRONG")
        self.assertEqual(extract_mutation_type(child["mutation_note"]), "STRONG_CLONE_OPTIMIZED_VARIANT")

    def test_lineage_payload(self) -> None:
        strategies = [
            {"strategy_id": "root", "parent_strategy_id": None, "generation": 0, "mutation_note": ""},
            {
                "strategy_id": "c1",
                "parent_strategy_id": "root",
                "generation": 1,
                "mutation_note": "mutation_type=WEAK_X|detail",
                "created_at": "2025-01-02T00:00:00+00:00",
            },
        ]
        edges = build_lineage_payload(strategies)
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["parent"], "root")
        self.assertEqual(edges[0]["child"], "c1")
        self.assertEqual(edges[0]["mutation"], "WEAK_X")
        self.assertEqual(edges[0]["created_at"], "2025-01-02T00:00:00+00:00")

    def test_run_batch_skips_high_generation(self) -> None:
        factory = [
            {
                "strategy_id": "old",
                "family": "EMA_CROSS",
                "generation": MAX_PARENT_GENERATION,
                "parameters": {"ema_fast": 5, "ema_slow": 20},
                "fitness_score": 40.0,
                "expected_win_rate": 0.4,
                "expected_drawdown": 300.0,
                "risk_profile": "HIGH",
                "status": "REJECTED",
            }
        ]
        perf = [
            {
                "strategy_id": "old",
                "performance_score": 0.2,
                "total_runs": WEAK_MIN_TOTAL_RUNS,
                "success_rate": 0.1,
            }
        ]
        created, skipped = run_evolution_batch(factory, perf, seed=1, max_weak=3, max_strong=0)
        self.assertEqual(created, [])
        self.assertTrue(any("generation_cap" in str(s.get("reason", "")) for s in skipped))

    def test_parent_unchanged_after_evolve_weak(self) -> None:
        parent = {
            "strategy_id": "orig",
            "family": "MOMENTUM",
            "generation": 0,
            "parameters": {"lookback": 20, "threshold": 1.0, "risk_multiplier": 1.0},
            "fitness_score": 50.0,
            "expected_win_rate": 0.5,
            "expected_drawdown": 200.0,
            "risk_profile": "MEDIUM",
            "status": "TESTING",
        }
        snap = str(parent["parameters"])
        child, _ = evolve_weak_variant(parent, seed=7)
        self.assertEqual(str(parent["parameters"]), snap)
        self.assertIsNot(child, parent)

    def test_variant_summary_row(self) -> None:
        row = variant_summary_row(
            {
                "strategy_id": "n1",
                "parent_strategy_id": "p1",
                "generation": 3,
                "mutation_note": "mutation_type=FOO|bar",
                "family": "X",
                "origin_type": "AI_EVOLVED_WEAK",
                "created_at": "t",
            }
        )
        self.assertEqual(row["mutation_type"], "FOO")


class AIStrategyEvolutionHandlerTests(unittest.TestCase):
    """Route handlers (no httpx TestClient — keeps deps minimal)."""

    def test_get_evolution_candidates_handler(self) -> None:
        from backend.main import get_evolution_candidates

        data = get_evolution_candidates()
        self.assertIn("weak_strategies", data)
        self.assertIn("strong_strategies", data)
        self.assertIsInstance(data["weak_strategies"], list)
        self.assertIsInstance(data["strong_strategies"], list)

    def test_get_evolution_lineage_handler(self) -> None:
        from backend.main import get_evolution_lineage

        data = get_evolution_lineage()
        self.assertIn("lineage", data)
        self.assertIsInstance(data["lineage"], list)

    def test_post_evolution_run_handler_shape(self) -> None:
        from backend.main import post_evolution_run

        data = post_evolution_run(seed=123, max_weak=0, max_strong=0)
        self.assertIn("created_variants", data)
        self.assertIn("skipped", data)
        self.assertEqual(data["seed"], 123)


if __name__ == "__main__":
    unittest.main()
