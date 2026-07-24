"""
Safe experimentation: score a candidate guidance vector against historical episodes
without touching the live cognitive loop.
"""

from __future__ import annotations

import random
from typing import Any


def _l1_delta(a: dict[str, Any], b: dict[str, Any]) -> float:
    keys = set(a) | set(b)
    s = 0.0
    for k in keys:
        try:
            s += abs(float(a.get(k, 0.0)) - float(b.get(k, 0.0)))
        except (TypeError, ValueError):
            continue
    return s


def collect_weighted_scores(
    *,
    historical_episodes: list[dict[str, Any]],
    baseline_fingerprint: str,
    candidate_guidance: dict[str, Any],
) -> tuple[list[float], list[float]]:
    """Shared weighting over historical episodes (used by sandbox + evolutionary competition)."""
    td_c = (candidate_guidance.get("threshold_deltas") or {}) if isinstance(candidate_guidance, dict) else {}
    weights: list[float] = []
    scores: list[float] = []
    for ep in historical_episodes:
        fp = str(ep.get("fingerprint", ""))
        if fp != baseline_fingerprint:
            continue
        g = ep.get("guidance_applied") or {}
        if not isinstance(g, dict):
            continue
        td_h = g.get("threshold_deltas") or {}
        if not isinstance(td_h, dict):
            td_h = {}
        dist = _l1_delta(td_h, td_c)
        w = 1.0 / (1.0 + dist)
        os = ep.get("outcome_score")
        if os is None:
            continue
        weights.append(w)
        scores.append(float(os))
    return weights, scores


def evaluate_candidate_vs_history(
    *,
    historical_episodes: list[dict[str, Any]],
    baseline_fingerprint: str,
    candidate_guidance: dict[str, Any],
    baseline_mean_score: float,
) -> tuple[float, dict[str, Any]]:
    """
    Expected lift from candidate vs baseline using local similarity weighting.
    No re-execution of detectors — uses recorded outcomes only.
    """
    if not historical_episodes or baseline_mean_score <= 0:
        return 0.0, {"reason": "insufficient_history"}

    weights, scores = collect_weighted_scores(
        historical_episodes=historical_episodes,
        baseline_fingerprint=baseline_fingerprint,
        candidate_guidance=candidate_guidance,
    )

    if not scores:
        return 0.0, {"reason": "no_matching_episodes"}

    num = sum(w * s for w, s in zip(weights, scores, strict=False))
    den = sum(weights) or 1.0
    candidate_mean = num / den
    lift = candidate_mean - baseline_mean_score
    return lift, {"candidate_mean": candidate_mean, "n": len(scores)}


def threshold_delta_l1_norm(guidance: dict[str, Any]) -> float:
    td = (guidance.get("threshold_deltas") or {}) if isinstance(guidance, dict) else {}
    if not isinstance(td, dict):
        return 0.0
    s = 0.0
    for v in td.values():
        try:
            s += abs(float(v))
        except (TypeError, ValueError):
            continue
    return float(s)


def propose_candidate_guidance(baseline: dict[str, Any]) -> dict[str, Any]:
    """Small random mutation for sandbox exploration (not used on live path unless approved)."""
    out = dict(baseline) if baseline else {}
    td = dict(out.get("threshold_deltas") or {}) if isinstance(out.get("threshold_deltas"), dict) else {}
    key = random.choice(["iso_contamination", "iso_warn", "air_density_z"])
    jitter = random.choice([-0.008, -0.004, 0.004, 0.008])
    try:
        td[key] = float(td.get(key, 0.0)) + jitter
    except (TypeError, ValueError):
        td[key] = jitter
    out["threshold_deltas"] = td
    out["planner_profile"] = out.get("planner_profile") or "template"
    return out
