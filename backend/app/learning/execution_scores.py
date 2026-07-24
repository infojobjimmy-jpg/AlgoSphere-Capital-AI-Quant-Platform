"""Shared plan execution → scalar score (critic + meta-learning reuse)."""

from __future__ import annotations

from typing import Any


def plan_execution_score(exec_view: dict[str, Any]) -> float:
    st = str(exec_view.get("status", ""))
    if st == "completed":
        return 0.92
    if st == "advanced":
        return 0.65
    if st == "fallback":
        return 0.38
    return 0.45
