from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def insert_strategy_episode(
    session: AsyncSession,
    *,
    fingerprint: str,
    deployment: str,
    metrics: dict[str, Any],
    outcome_score: float | None,
    plan_id: int | None,
    guidance_applied: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO strategy_memory (fingerprint, deployment, metrics, outcome_score, plan_id, guidance_applied)
            VALUES (:fp, :dep, CAST(:m AS jsonb), :os, :pid, CAST(:g AS jsonb))
            """
        ),
        {
            "fp": fingerprint[:128],
            "dep": deployment[:32],
            "m": json.dumps(metrics),
            "os": outcome_score,
            "pid": plan_id,
            "g": json.dumps(guidance_applied),
        },
    )
    await session.commit()


async def aggregate_fingerprint_stats(
    session: AsyncSession, *, limit_rows: int = 500, min_samples: int = 3
) -> list[dict[str, Any]]:
    ms = max(1, min(min_samples, 50))
    res = await session.execute(
        text(
            """
            SELECT fingerprint,
                   AVG(outcome_score) AS avg_score,
                   COUNT(*) AS n
            FROM strategy_memory
            WHERE deployment = 'live' AND outcome_score IS NOT NULL
            GROUP BY fingerprint
            HAVING COUNT(*) >= :ms
            ORDER BY AVG(outcome_score) DESC
            LIMIT :lim
            """
        ),
        {"lim": max(10, min(limit_rows, 5000)), "ms": ms},
    )
    return [dict(r) for r in res.mappings().all()]


async def upsert_ranking(
    session: AsyncSession,
    *,
    fingerprint: str,
    aggregate_score: float,
    sample_count: int,
    last_metrics: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO strategy_rankings (fingerprint, aggregate_score, sample_count, last_metrics, updated_at)
            VALUES (:fp, :sc, :n, CAST(:m AS jsonb), NOW())
            ON CONFLICT (fingerprint) DO UPDATE SET
                aggregate_score = EXCLUDED.aggregate_score,
                sample_count = EXCLUDED.sample_count,
                last_metrics = EXCLUDED.last_metrics,
                updated_at = NOW()
            """
        ),
        {"fp": fingerprint[:128], "sc": float(aggregate_score), "n": int(sample_count), "m": json.dumps(last_metrics)},
    )
    await session.commit()


async def insert_sandbox_result(
    session: AsyncSession,
    *,
    candidate_fingerprint: str,
    baseline_fingerprint: str,
    expected_lift: float,
    approved: bool,
    details: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO sandbox_results (candidate_fingerprint, baseline_fingerprint, expected_lift, approved, details)
            VALUES (:c, :b, :lift, :ok, CAST(:d AS jsonb))
            """
        ),
        {
            "c": candidate_fingerprint[:128],
            "b": baseline_fingerprint[:128],
            "lift": float(expected_lift),
            "ok": approved,
            "d": json.dumps(details),
        },
    )
    await session.commit()


async def load_top_guidance_seeds(
    session: AsyncSession, stats: list[dict[str, Any]], *, limit: int = 3
) -> list[dict[str, Any]]:
    """Latest guidance_applied for top-ranked fingerprints (async seeds for the generator)."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in stats[: max(5, limit * 3)]:
        fp = str(row.get("fingerprint", ""))
        if not fp or fp in seen:
            continue
        seen.add(fp)
        g = await latest_guidance_for_fingerprint(session, fp)
        if g:
            out.append(g)
        if len(out) >= limit:
            break
    return out


async def latest_guidance_for_fingerprint(session: AsyncSession, fingerprint: str) -> dict[str, Any]:
    res = await session.execute(
        text(
            """
            SELECT guidance_applied
            FROM strategy_memory
            WHERE fingerprint = :fp AND deployment = 'live'
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"fp": fingerprint[:128]},
    )
    row = res.mappings().first()
    if not row:
        return {}
    g = row["guidance_applied"]
    return g if isinstance(g, dict) else {}
