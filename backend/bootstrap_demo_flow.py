from __future__ import annotations

import json
from typing import Any

BOOTSTRAP_MAX_PAPER_TRADES = 20
BOOTSTRAP_RISK_ALLOCATION_PCT = 0.1
BOOTSTRAP_DEMO_ONLY = True
BOOTSTRAP_ASSIGNEE = "bootstrap_demo_flow"


def is_newly_evolved_strategy(strategy: dict[str, Any]) -> bool:
    generation = int(strategy.get("generation", 0) or 0)
    parent = str(strategy.get("parent_strategy_id", "") or "").strip()
    origin = str(strategy.get("origin_type", "") or "").upper()
    return generation > 0 or bool(parent) or origin in {"EVOLVED", "EVOLUTION"}


def is_bootstrap_blocked(perf_row: dict[str, Any]) -> bool:
    total_runs = int(perf_row.get("total_runs", 0) or 0)
    success_rate = float(perf_row.get("success_rate", 0.0) or 0.0)
    perf_score = float(perf_row.get("performance_score", 0.0) or 0.0)
    return total_runs == 0 and success_rate <= 0.0 and perf_score <= 0.30


def should_enter_bootstrap(
    strategy: dict[str, Any],
    perf_row: dict[str, Any],
) -> tuple[bool, str]:
    if not is_newly_evolved_strategy(strategy):
        return False, "not_newly_evolved"
    if not is_bootstrap_blocked(perf_row):
        return False, "has_history_or_score"
    if str(strategy.get("review_status", "")) not in {"", "PENDING_REVIEW", "UNDER_REVIEW"}:
        return False, "review_already_advanced"
    if str(strategy.get("demo_status", "")) in {"DEMO_QUEUE", "DEMO_ASSIGNED", "DEMO_RUNNING"}:
        return False, "already_in_demo_flow"
    if str(strategy.get("executor_status", "")) in {"EXECUTOR_READY", "EXECUTOR_RUNNING"}:
        return False, "already_in_executor_flow"
    if str(strategy.get("runner_status", "")) in {"RUNNER_PENDING", "RUNNER_ACKNOWLEDGED", "RUNNER_ACTIVE"}:
        return False, "already_in_runner_flow"
    return True, "bootstrap_required"


def build_bootstrap_demo_note() -> str:
    payload = {
        "bootstrap": True,
        "demo_only": BOOTSTRAP_DEMO_ONLY,
        "risk_allocation_pct": BOOTSTRAP_RISK_ALLOCATION_PCT,
        "max_paper_trades": BOOTSTRAP_MAX_PAPER_TRADES,
    }
    return f"bootstrap_demo_flow:{json.dumps(payload, sort_keys=True)}"
