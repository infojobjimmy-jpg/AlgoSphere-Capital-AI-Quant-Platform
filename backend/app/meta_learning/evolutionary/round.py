"""
One evolutionary round: generate variants, score in parallel (offline), select, promote.
Extends meta-learning without touching the Cortex hot path.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from app.config import settings
from app.meta_learning import guidance as gbus
from app.meta_learning.evolutionary.competition import score_variant_offline
from app.meta_learning.evolutionary.selection import pick_winner
from app.meta_learning.fingerprint import strategy_fingerprint
from app.meta_learning.guidance import compact_guidance_payload
from app.meta_learning.store import insert_sandbox_result, insert_strategy_episode, load_top_guidance_seeds
from app.meta_learning.strategy_generator import generate_strategy_variants
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("acap.evolutionary")


def _dedupe_variants(pairs: list[tuple[str, dict[str, Any]]]) -> list[tuple[str, dict[str, Any]]]:
    seen: set[str] = set()
    out: list[tuple[str, dict[str, Any]]] = []
    for vid, g in pairs:
        key = json.dumps(compact_guidance_payload(g), sort_keys=True, separators=(",", ":"))
        if key in seen:
            continue
        seen.add(key)
        out.append((vid, g))
    return out


def _score_one(
    hist: list[dict[str, Any]],
    baseline_fp: str,
    baseline_mean: float,
    variant_id: str,
    guidance: dict[str, Any],
) -> dict[str, Any]:
    return score_variant_offline(
        historical_episodes=hist,
        baseline_fingerprint=baseline_fp,
        baseline_mean_score=baseline_mean,
        variant_id=variant_id,
        candidate_guidance=guidance,
    )


async def run_evolutionary_round(
    r: Any,
    session: AsyncSession,
    stats: list[dict[str, Any]],
    hist: list[dict[str, Any]],
) -> bool:
    if not settings.evolutionary_enabled:
        return False
    if not stats:
        return False

    best_row = stats[0]
    baseline_fp = str(best_row["fingerprint"])
    baseline_mean = float(best_row["avg_score"])

    seeds = await load_top_guidance_seeds(session, stats, limit=int(settings.evolutionary_seed_fingerprints))
    if not seeds:
        seeds = [await gbus.load_guidance(r) or {}]

    rng_budget = max(4, int(settings.evolutionary_variants_per_round))
    per_seed = max(2, rng_budget // max(1, len(seeds)))
    pairs: list[tuple[str, dict[str, Any]]] = []
    for i, seed in enumerate(seeds):
        for vid, g in generate_strategy_variants(
            seed,
            max_variants=per_seed,
            openai_available=bool(settings.openai_api_key),
        ):
            pairs.append((f"s{i}:{vid}", g))

    pairs = _dedupe_variants(pairs)[: int(settings.evolutionary_variants_per_round)]

    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(None, _score_one, hist, baseline_fp, baseline_mean, vid, g) for vid, g in pairs
    ]
    scored = list(await asyncio.gather(*tasks))

    winner = pick_winner(scored)
    wid = str(winner.get("variant_id")) if winner else None

    for row in scored:
        await insert_sandbox_result(
            session,
            candidate_fingerprint=str(row.get("variant_id", "unknown"))[:128],
            baseline_fingerprint=baseline_fp,
            expected_lift=float(row.get("lift", 0.0)),
            approved=bool(winner and str(row.get("variant_id")) == wid),
            details={
                "competition": {
                    "performance": row.get("performance"),
                    "stability": row.get("stability"),
                    "efficiency": row.get("efficiency"),
                    "composite": row.get("composite"),
                },
                "detail": row.get("detail"),
                "evolutionary": True,
            },
        )

    if not winner:
        logger.debug("evolutionary: no variant promoted (n=%s)", len(scored))
        return False

    winner_g = winner.get("guidance") or {}
    cur = await gbus.load_guidance(r)
    merged = {
        "version": 1,
        "threshold_deltas": winner_g.get("threshold_deltas") or {},
        "severity_lift_delta": float(winner_g.get("severity_lift_delta", cur.get("severity_lift_delta", 0.0))),
        "planner_profile": winner_g.get("planner_profile") or cur.get("planner_profile") or "template",
        "decision_confidence_boost": float(
            winner_g.get("decision_confidence_boost", cur.get("decision_confidence_boost", 0.0))
        ),
        "best_fingerprint": baseline_fp,
        "evolved_variant": str(winner.get("variant_id", ""))[:120],
    }
    await gbus.save_guidance(r, merged)

    slim = compact_guidance_payload(merged)
    fp = strategy_fingerprint(guidance=slim, goal_ids=())
    await insert_strategy_episode(
        session,
        fingerprint=fp,
        deployment="offline_evolved",
        metrics={
            "source": "evolutionary_round",
            "variant": winner.get("variant_id"),
            "composite": winner.get("composite"),
            "lift": winner.get("lift"),
        },
        outcome_score=float(winner.get("composite", 0.0)),
        plan_id=None,
        guidance_applied=slim,
    )

    logger.info(
        "evolutionary: promoted variant=%s composite=%.3f lift=%.4f",
        winner.get("variant_id"),
        float(winner.get("composite", 0.0)),
        float(winner.get("lift", 0.0)),
    )
    return True
