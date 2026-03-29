"""Tests for weekend evolution / replay (no DB)."""

from __future__ import annotations

import unittest
from collections import Counter
from pathlib import Path

from backend.weekend_evolution_engine import (
    DIVERSITY_BUCKETS,
    generate_diverse_candidates,
    historical_replay_backtest,
    pearson_correlation,
    portfolio_weights,
    risk_parity_weights,
    run_weekend_cycle,
    select_diversified_portfolio,
    synthetic_closes,
)


class WeekendEvolutionTests(unittest.TestCase):
    def test_generate_at_least_min_total(self) -> None:
        g = generate_diverse_candidates(1000, seed=1)
        self.assertGreaterEqual(len(g), 1000)
        families = {x["family"] for x in g}
        expected = {b[1] for b in DIVERSITY_BUCKETS}
        self.assertTrue(expected.issubset(families))

    def test_backtest_and_correlation(self) -> None:
        closes = synthetic_closes(bars=120, seed=42)
        strat = generate_diverse_candidates(1, seed=2)[0]
        bt = historical_replay_backtest(strat, closes)
        self.assertIn("win_rate", bt)
        self.assertIn("segment_pnls", bt)
        self.assertIn("research_composite", bt)
        self.assertIn("regime_adaptability", bt)
        self.assertEqual(len(bt["segment_pnls"]), 12)
        a, b = [1.0, 2.0, 3.0], [2.0, 4.0, 6.0]
        self.assertGreater(pearson_correlation(a, b), 0.99)

    def test_diversified_select(self) -> None:
        items = []
        for i in range(30):
            vec = [float(i)] * 12
            items.append(
                {
                    "strategy_id": f"id{i}",
                    "family": "MOMENTUM",
                    "_weekend_composite": 80.0 - i,
                    "_segment_pnls": vec,
                }
            )
        port, _meta = select_diversified_portfolio(
            items, top_n=5, max_correlation=0.5, enforce_min_quotas=False
        )
        self.assertGreaterEqual(len(port), 1)
        self.assertLessEqual(len(port), 5)

    def test_run_weekend_cycle_smoke(self) -> None:
        r = run_weekend_cycle(
            [],
            min_generate=25,
            seed=7,
            synthetic_bars=200,
            top_portfolio_n=5,
            prefer_synthetic_history=True,
            portfolio_weighting="equal",
            enforce_portfolio_quotas=False,
            cycle_index=1,
        )
        self.assertEqual(r["generated_count"], 25)
        self.assertEqual(r["evolved_count"], 0)
        self.assertGreaterEqual(r["evaluated_count"], 25)
        self.assertGreaterEqual(len(r["strategies_for_db"]), 25)
        self.assertTrue(r["demo_only"])
        self.assertEqual(r["portfolio_weighting"], "equal")
        self.assertEqual(r["cycle_index"], 1)

    def test_csv_missing_ok(self) -> None:
        r = run_weekend_cycle(
            [],
            min_generate=10,
            seed=9,
            historical_csv=Path("nonexistent_path_12345.csv"),
            synthetic_bars=100,
            top_portfolio_n=3,
            prefer_synthetic_history=True,
            enforce_portfolio_quotas=False,
        )
        self.assertIn("explicit csv missing", r["history_source"])

    def test_risk_parity_weights_sum(self) -> None:
        port = [
            {"_segment_pnls": [0.1, -0.05, 0.02, 0.0] * 3},
            {"_segment_pnls": [0.5, 0.5, 0.5, 0.5] * 3},
        ]
        w = risk_parity_weights(port)
        self.assertEqual(len(w), 2)
        self.assertAlmostEqual(sum(w), 1.0, places=5)

    def test_portfolio_weights_equal(self) -> None:
        s = [{"_segment_pnls": [1.0]} for _ in range(4)]
        w = portfolio_weights(s, "equal")
        self.assertAlmostEqual(sum(w), 1.0)
        self.assertTrue(all(x == w[0] for x in w))

    def test_family_cap(self) -> None:
        items = []
        for i in range(12):
            items.append(
                {
                    "strategy_id": f"m{i}",
                    "family": "MOMENTUM",
                    "_weekend_composite": 100.0 - i,
                    "_segment_pnls": [float(i)] * 12,
                }
            )
        for i in range(8):
            items.append(
                {
                    "strategy_id": f"e{i}",
                    "family": "EMA_CROSS",
                    "_weekend_composite": 80.0 - i,
                    "_segment_pnls": [float(i + 100)] * 12,
                }
            )
        port, _ = select_diversified_portfolio(
            items,
            top_n=8,
            max_correlation=1.0,
            family_cap=2,
            enforce_min_quotas=False,
        )
        mom = sum(1 for p in port if p["family"] == "MOMENTUM")
        self.assertLessEqual(mom, 2)
        self.assertGreaterEqual(len(port), 1)

    def test_portfolio_min_quotas_and_hard_cap(self) -> None:
        r = run_weekend_cycle(
            [],
            min_generate=120,
            seed=42,
            synthetic_bars=220,
            top_portfolio_n=20,
            prefer_synthetic_history=True,
            enforce_portfolio_quotas=True,
            portfolio_weighting="equal",
            cycle_index=3,
        )
        self.assertEqual(r["generated_count"], 120)
        mix = Counter(p["family"] for p in r["diversified_portfolio"])
        self.assertGreaterEqual(mix.get("EMA_CROSS", 0), 3)
        self.assertGreaterEqual(mix.get("SESSION_BREAKOUT", 0), 3)
        self.assertGreaterEqual(mix.get("MOMENTUM", 0), 3)
        self.assertGreaterEqual(mix.get("MEAN_REVERSION", 0), 3)
        self.assertGreaterEqual(mix.get("VOLATILITY_REGIME", 0), 2)
        for _f, n in mix.items():
            self.assertLessEqual(n, 4)
        self.assertTrue(r["risk_profile"]["quotas_met"])
        self.assertGreaterEqual(r["risk_profile"]["diversification_score"], 40.0)
        self.assertEqual(r["cycle_index"], 3)
        self.assertEqual(r["portfolio_selection"]["family_hard_cap"], 4)

    def test_research_mode_presentation(self) -> None:
        r = run_weekend_cycle(
            [],
            min_generate=80,
            seed=11,
            synthetic_bars=400,
            top_portfolio_n=10,
            prefer_synthetic_history=True,
            ranking_mode="research",
            include_presentation_portfolios=True,
            presentation_top_n=6,
            enforce_portfolio_quotas=False,
        )
        self.assertEqual(r["ranking_mode"], "research")
        pres = r.get("presentation") or {}
        self.assertIn("growth_portfolio", pres)
        self.assertIn("demo_safe_portfolio", pres)
        self.assertIn("client_demo_verdict", pres)
        self.assertEqual(len(pres.get("top_5_safest_candidates") or []), 5)


if __name__ == "__main__":
    unittest.main()
