"""
Strategy variant generation: controlled mutations on top-performing guidance.
Reuses sandbox.propose_candidate_guidance for one mutation class (no duplicate mutation math).
"""

from __future__ import annotations

import copy
import random
from typing import Any

from app.meta_learning.sandbox import propose_candidate_guidance


def _deepish_guidance(g: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(g) if g else {}


def _mutate_thresholds_small(g: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    out = _deepish_guidance(g)
    td = dict(out.get("threshold_deltas") or {}) if isinstance(out.get("threshold_deltas"), dict) else {}
    key = rng.choice(["iso_contamination", "iso_warn", "iso_critical", "air_density_z", "maritime_z"])
    delta = rng.choice([-0.006, -0.003, 0.003, 0.006])
    try:
        td[key] = float(td.get(key, 0.0)) + delta
    except (TypeError, ValueError):
        td[key] = delta
    out["threshold_deltas"] = td
    return out


def _mutate_planner_profile(g: dict[str, Any], openai_available: bool, rng: random.Random) -> dict[str, Any]:
    out = _deepish_guidance(g)
    cur = str(out.get("planner_profile") or "template")
    if not openai_available:
        out["planner_profile"] = "template"
        return out
    out["planner_profile"] = "openai" if cur == "template" else "template"
    return out


def _mutate_decision_boost(g: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    out = _deepish_guidance(g)
    try:
        b = float(out.get("decision_confidence_boost", 0.0))
    except (TypeError, ValueError):
        b = 0.0
    b = max(0.0, min(0.08, b + rng.choice([-0.02, -0.01, 0.01, 0.02])))
    out["decision_confidence_boost"] = b
    return out


def _mutate_severity_lift_delta(g: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    out = _deepish_guidance(g)
    try:
        s = float(out.get("severity_lift_delta", 0.0))
    except (TypeError, ValueError):
        s = 0.0
    s = max(-0.06, min(0.08, s + rng.choice([-0.02, -0.01, 0.01, 0.02])))
    out["severity_lift_delta"] = s
    return out


def generate_strategy_variants(
    base_guidance: dict[str, Any],
    *,
    max_variants: int,
    openai_available: bool,
    rng: random.Random | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """
    Produce (variant_id, guidance) pairs including a baseline clone and mutations.
    """
    rng = rng or random.Random()
    cap = max(2, min(int(max_variants), 24))
    out: list[tuple[str, dict[str, Any]]] = []
    out.append(("baseline", _deepish_guidance(base_guidance)))

    mutators = [
        ("mut_sandbox_proposal", lambda: propose_candidate_guidance(base_guidance)),
        ("mut_thresholds", lambda: _mutate_thresholds_small(base_guidance, rng)),
        ("mut_planner", lambda: _mutate_planner_profile(base_guidance, openai_available, rng)),
        ("mut_decision_boost", lambda: _mutate_decision_boost(base_guidance, rng)),
        ("mut_severity_lift", lambda: _mutate_severity_lift_delta(base_guidance, rng)),
    ]
    rng.shuffle(mutators)

    for label, fn in mutators:
        if len(out) >= cap:
            break
        try:
            out.append((label, fn()))
        except Exception:
            continue

    while len(out) < cap:
        try:
            out.append((f"mut_extra_{len(out)}", propose_candidate_guidance(base_guidance)))
        except Exception:
            break
    return out[:cap]
