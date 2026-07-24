"""
Offline competition metrics for strategy variants.
Reuses sandbox.evaluate_candidate_vs_history + collect_weighted_scores (no duplicate weighting).
"""

from __future__ import annotations

import random
import statistics
from typing import Any

from app.meta_learning.sandbox import (
    collect_weighted_scores,
    evaluate_candidate_vs_history,
    threshold_delta_l1_norm,
)


def _norm_performance(mean_score: float) -> float:
    return max(0.0, min(1.0, (float(mean_score) - 0.35) / 0.55))


def bootstrap_stability(weights: list[float], scores: list[float], *, rounds: int = 8) -> float:
    """Higher when weighted means are stable under subsampling (inverse spread of bootstrap means)."""
    n = len(scores)
    if n < 3 or len(weights) != n:
        return 0.55
    rng = random.Random(31)
    means: list[float] = []
    k = max(2, int(0.65 * n))
    for _ in range(rounds):
        idx = [rng.randrange(n) for _ in range(k)]
        ws = sum(weights[i] for i in idx)
        if ws <= 0:
            continue
        m = sum(weights[i] * scores[i] for i in idx) / ws
        means.append(m)
    if len(means) < 2:
        return 0.55
    sd = statistics.pstdev(means)
    return 1.0 / (1.0 + 10.0 * sd)


def efficiency_score(guidance: dict[str, Any]) -> float:
    """Prefers smaller threshold intervention magnitude (bounded 0–1)."""
    l1 = threshold_delta_l1_norm(guidance)
    return 1.0 / (1.0 + 12.0 * l1)


def score_variant_offline(
    *,
    historical_episodes: list[dict[str, Any]],
    baseline_fingerprint: str,
    baseline_mean_score: float,
    variant_id: str,
    candidate_guidance: dict[str, Any],
) -> dict[str, Any]:
    lift, detail = evaluate_candidate_vs_history(
        historical_episodes=historical_episodes,
        baseline_fingerprint=baseline_fingerprint,
        candidate_guidance=candidate_guidance,
        baseline_mean_score=baseline_mean_score,
    )
    w, s = collect_weighted_scores(
        historical_episodes=historical_episodes,
        baseline_fingerprint=baseline_fingerprint,
        candidate_guidance=candidate_guidance,
    )
    stability = bootstrap_stability(w, s) if s else 0.5
    eff = efficiency_score(candidate_guidance)
    if isinstance(detail, dict) and detail.get("candidate_mean") is not None:
        perf_raw = float(detail["candidate_mean"])
    else:
        perf_raw = float(baseline_mean_score)
    perf = _norm_performance(perf_raw)
    composite = 0.52 * perf + 0.28 * stability + 0.20 * eff
    return {
        "variant_id": variant_id,
        "lift": float(lift),
        "performance": perf,
        "stability": float(stability),
        "efficiency": float(eff),
        "composite": float(composite),
        "detail": detail,
        "guidance": candidate_guidance,
    }
