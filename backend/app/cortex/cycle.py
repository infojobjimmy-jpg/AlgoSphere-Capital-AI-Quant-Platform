"""Single orchestration path for the telemetry bridge: goals drive the full loop."""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

import numpy as np

from app.config import settings
from app.cortex import agents as cortex_agents
from app.cortex.memory import remember_episode
from app.db.session import SessionLocal
from app.decision.persist_decisions import persist_decisions
from app.fusion.persist_events import persist_fusion_events
from app.goals.engine import apply_goal_bias_to_thresholds, attention_weights, compute_goal_bias, global_priority_score
from app.goals.pipeline import run_goals_post_cycle
from app.goals.store import list_active_goals
from app.intel.orchestrator import build_snapshot
from app.learning.adaptive_thresholds import load_thresholds
from app.learning.performance import record_cycle
from app.meta_learning.cortex_recorder import record_strategy_memory_tick
from app.trading.hook import on_cortex_tick
from app.meta_learning.guidance import (
    compact_guidance_payload,
    load_guidance,
    merge_evolution_into_thresholds,
    severity_lift_delta,
)

logger = logging.getLogger("acap.cortex")

META_VERSION = 1


def _attach_goal_strategy_overlay(snap: dict[str, Any], goals: list[dict[str, Any]], bias: dict[str, float]) -> None:
    strat = snap.setdefault("meta", {}).setdefault("strategy", {})
    if not goals:
        return
    strat["goals_active"] = [
        {
            "id": int(g.get("id", 0)),
            "description": str(g.get("description", ""))[:200],
            "priority": float(g.get("priority", 0.0)),
        }
        for g in goals[:16]
    ]
    strat["global_priority"] = global_priority_score(goals, snap)
    strat["attention_weights"] = attention_weights(goals)
    if bias:
        strat["goal_bias"] = {k: float(v) for k, v in bias.items()}


def _attach_evolution_meta(snap: dict[str, Any], guidance: dict[str, Any]) -> None:
    meta = snap.setdefault("meta", {})
    meta["evolution"] = {
        "guidance": compact_guidance_payload(guidance),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _attach_cortex_meta(snap: dict[str, Any], *, phases: list[dict[str, Any]]) -> None:
    meta = snap.setdefault("meta", {})
    meta["cortex"] = {
        "version": META_VERSION,
        "loop": "detect→analyze→decide→plan→execute→evaluate→learn",
        "agents": [name for name, _ in cortex_agents.PIPELINE],
        "goal_driver": True,
        "phases": phases,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


async def run_kafka_intel_tick(
    *,
    r: Any,
    layers: dict[str, list],
    ac_hist: deque[int],
    sh_hist: deque[int],
    ewma_state: dict[str, float],
    feat_history: list[np.ndarray],
) -> tuple[dict[str, Any], dict[str, float], list[np.ndarray]]:
    """
    One full cognitive tick: load goals → perceive & reason → merge strategy view → persist + plan/execute/critic.
    Mutates layers in place via bridge; returns updated snapshot and state carriers.
    """
    phases: list[dict[str, Any]] = []
    t0 = datetime.now(timezone.utc)

    goals: list[dict[str, Any]] = []
    try:
        async with SessionLocal() as s:
            goals = await list_active_goals(s)
    except Exception:
        logger.exception("cortex: goals load failed")
    phases.append({"name": "goals", "ms": _ms_since(t0), "active": len(goals)})
    t1 = datetime.now(timezone.utc)

    bias = compute_goal_bias(goals) if goals else {}
    th0 = await load_thresholds(r)
    guidance_raw = await load_guidance(r)
    th_evolved = merge_evolution_into_thresholds(th0, guidance_raw)
    th_working = apply_goal_bias_to_thresholds(th_evolved, bias) if goals else th_evolved
    sev_lift = float(bias.get("severity_lift", 0.0)) if goals else 0.0
    sev_lift = min(0.25, max(0.0, sev_lift + severity_lift_delta(guidance_raw)))

    snap, ewma2, feat_hist2 = await build_snapshot(
        layers=layers,
        ac_hist=ac_hist,
        sh_hist=sh_hist,
        ewma_state=ewma_state,
        feat_history=feat_history,
        r=r,
        working_thresholds=th_working,
        goal_severity_lift=sev_lift,
        evolution_guidance=guidance_raw or None,
    )
    _attach_goal_strategy_overlay(snap, goals, bias)
    _attach_evolution_meta(snap, guidance_raw)
    phases.append({"name": "detect_analyze_decide", "ms": _ms_since(t1)})

    t2 = datetime.now(timezone.utc)
    try:
        async with SessionLocal() as session:
            if settings.fusion_event_db_write and snap.get("events"):
                await persist_fusion_events(session, snap["events"])
            if settings.decision_db_write and snap.get("decisions"):
                await persist_decisions(session, snap["decisions"])
            await record_cycle(session, snap)
            await run_goals_post_cycle(session, snap, goals, evolution_guidance=guidance_raw or None)
            await record_strategy_memory_tick(session, snap, goals, guidance_raw)
    except Exception:
        logger.exception("cortex: persistence / plan phase failed")
    phases.append({"name": "plan_execute_evaluate_persist", "ms": _ms_since(t2)})

    t3 = datetime.now(timezone.utc)
    await remember_episode(snap)
    phases.append({"name": "memory", "ms": _ms_since(t3)})

    if not settings.public_geospatial_mode:
        t4 = datetime.now(timezone.utc)
        try:
            await on_cortex_tick(r, snap)
        except Exception:
            logger.exception("cortex: trading hook failed")
        phases.append({"name": "trading", "ms": _ms_since(t4)})

    _attach_cortex_meta(snap, phases=phases)

    return snap, ewma2, feat_hist2


def _ms_since(t: datetime) -> float:
    return max(0.0, (datetime.now(timezone.utc) - t).total_seconds() * 1000.0)
