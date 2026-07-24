"""
Evolution meta-agent: observes stored episodes, ranks strategies, optionally promotes
sandbox-approved guidance to Redis. Never runs inside the hot per-tick path.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from app.config import settings
from app.db.session import SessionLocal
from app.meta_learning import guidance as gbus
from app.meta_learning import sandbox as sbx
from app.meta_learning.evolutionary.round import run_evolutionary_round
from app.meta_learning.store import aggregate_fingerprint_stats, insert_sandbox_result, latest_guidance_for_fingerprint, upsert_ranking
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("acap.evolution")

_last_run_monotonic = 0.0
_running = False


async def _recent_episodes(session: AsyncSession, limit: int = 300) -> list[dict[str, Any]]:
    res = await session.execute(
        text(
            """
            SELECT fingerprint, outcome_score, guidance_applied, metrics
            FROM strategy_memory
            WHERE deployment = 'live' AND outcome_score IS NOT NULL
            ORDER BY id DESC
            LIMIT :lim
            """
        ),
        {"lim": limit},
    )
    return [dict(r) for r in res.mappings().all()]


async def run_evolution_pass(r: Any) -> None:
    if not settings.meta_learning_enabled:
        return
    async with SessionLocal() as session:
        stats = await aggregate_fingerprint_stats(
            session,
            limit_rows=800,
            min_samples=int(settings.meta_learning_min_rank_samples),
        )
        if not stats:
            return
        for row in stats[:12]:
            await upsert_ranking(
                session,
                fingerprint=str(row["fingerprint"]),
                aggregate_score=float(row["avg_score"]),
                sample_count=int(row["n"]),
                last_metrics={"avg_score": float(row["avg_score"])},
            )

        hist = await _recent_episodes(session, limit=400)

        if settings.evolutionary_enabled:
            await run_evolutionary_round(r, session, stats, hist)
            return

        best = stats[0]
        baseline_fp = str(best["fingerprint"])
        baseline_mean = float(best["avg_score"])
        baseline_guidance = await latest_guidance_for_fingerprint(session, baseline_fp)
        if not baseline_guidance:
            baseline_guidance = await gbus.load_guidance(r)

        candidate = sbx.propose_candidate_guidance(baseline_guidance)
        lift, detail = sbx.evaluate_candidate_vs_history(
            historical_episodes=hist,
            baseline_fingerprint=baseline_fp,
            candidate_guidance=candidate,
            baseline_mean_score=baseline_mean,
        )

        approved = lift >= float(settings.meta_learning_sandbox_promote_min_lift)
        await insert_sandbox_result(
            session,
            candidate_fingerprint=f"sandbox:{baseline_fp[:20]}",
            baseline_fingerprint=baseline_fp,
            expected_lift=lift,
            approved=approved,
            details=detail,
        )

        if approved and candidate:
            cur = await gbus.load_guidance(r)
            merged = {
                "version": 1,
                "threshold_deltas": candidate.get("threshold_deltas") or {},
                "severity_lift_delta": float(candidate.get("severity_lift_delta", cur.get("severity_lift_delta", 0.0))),
                "planner_profile": candidate.get("planner_profile") or cur.get("planner_profile") or "template",
                "decision_confidence_boost": float(
                    candidate.get("decision_confidence_boost", cur.get("decision_confidence_boost", 0.0))
                ),
                "best_fingerprint": baseline_fp,
            }
            await gbus.save_guidance(r, merged)
            logger.info("meta-learning: promoted sandbox candidate (lift=%.4f)", lift)
        else:
            logger.debug("meta-learning: sandbox candidate not promoted (lift=%.4f)", lift)


def schedule_evolution_tick(r: Any) -> None:
    """Fire-and-forget evolution pass; throttled and single-flight."""
    global _last_run_monotonic, _running
    if not settings.meta_learning_enabled:
        return
    now = time.monotonic()
    if now - _last_run_monotonic < float(settings.meta_learning_evolution_interval_sec):
        return
    if _running:
        return
    _last_run_monotonic = now

    async def _job() -> None:
        global _running
        _running = True
        try:
            await run_evolution_pass(r)
        except Exception:
            logger.exception("evolution pass failed")
        finally:
            _running = False

    try:
        asyncio.get_running_loop().create_task(_job())
    except RuntimeError:
        logger.debug("meta-learning: no running loop; skip evolution scheduling")
