import json
import sqlite3
from datetime import datetime, timezone
from typing import Any
import threading
import time

from fastapi import FastAPI

from .ai_regime import detect_regime
from .ai_strategy_evolution_engine import (
    build_evolution_candidates_payload,
    build_lineage_payload,
    run_evolution_batch,
    variant_summary_row,
)
from .alerting_engine import apply_acknowledgements, build_alerts, build_alerts_summary
from .auto_recovery_engine import (
    execute_recovery,
    load_recovery_status,
    persist_finished,
    persist_running,
)
from .auto_loop_engine import AutoLoopEngine
from .autonomous_fund_engine import AutonomousFundEngine
from .bot_factory import generate_candidates, rank_candidates
from .bootstrap_demo_flow import (
    BOOTSTRAP_ASSIGNEE,
    build_bootstrap_demo_note,
    should_enter_bootstrap,
)
from .capital import allocate_capital
from .capital_engine import build_capital_status
from .candidate_review_desk import (
    REVIEW_APPROVED,
    REVIEW_NEEDS_TESTING,
    REVIEW_PENDING,
    REVIEW_REJECTED,
    REVIEW_UNDER,
    build_review_candidates,
    build_review_status_payload,
)
from .continuous_evolution_engine import ContinuousEvolutionLoopEngine
from .control_engine import compute_control, recommended_action_for_state
from .database import (
    acknowledge_alert_id,
    fetch_account,
    fetch_acknowledged_alert_ids,
    fetch_bots,
    fetch_factory_strategy_by_id,
    fetch_factory_strategies,
    fetch_factory_strategies_for_runner_pipeline,
    fetch_multi_runner_by_id,
    fetch_multi_runner_runners,
    fetch_paper_bots,
    fetch_paper_bots_for_strategies,
    fetch_performance_run_log,
    fetch_promotion_history,
    get_alert_engine_state,
    get_connection,
    init_db,
    insert_factory_strategies,
    insert_performance_run,
    insert_promotion_history,
    set_alert_engine_state,
    update_factory_strategy_status,
    update_factory_strategy_review,
    update_factory_strategy_demo,
    update_factory_strategy_executor,
    update_factory_strategy_runner,
    upsert_paper_bot,
    upsert_multi_runner_register,
    update_multi_runner_heartbeat,
    set_multi_runner_offline,
)
from .demo_deploy_desk import (
    DEMO_ASSIGNED,
    DEMO_PAUSED,
    DEMO_QUEUE,
    DEMO_REJECTED,
    build_demo_candidates,
    build_demo_status,
    is_demo_eligible,
)
from .evolution_engine import evolve_candidates
from .demo_executor_adapter import (
    EXECUTOR_PAUSED,
    EXECUTOR_READY,
    EXECUTOR_RUNNING,
    EXECUTOR_STOPPED,
    build_executor_candidates,
    build_executor_status,
    is_executor_eligible,
)
from .demo_execution_playbook import (
    build_phase_gate_checklist,
    build_playbook_status_payload,
    load_playbook_state,
    next_playbook_phase,
    reset_playbook,
    save_playbook_state,
    start_playbook,
)
from .execution import get_execution_decision
from .demo_runner_bridge import (
    RUNNER_ACKNOWLEDGED,
    RUNNER_ACTIVE,
    RUNNER_COMPLETED,
    RUNNER_FAILED,
    RUNNER_PAUSED,
    RUNNER_PENDING,
    RUNNER_STATES,
    build_runner_jobs,
    build_runner_status_payload,
    compute_runner_priority,
    is_runner_eligible,
)
from .global_risk_engine import (
    build_global_risk_alerts_payload,
    build_global_risk_assessment,
    build_global_risk_status_payload,
)
from .fund_engine import DEFAULT_TOTAL_CAPITAL, build_fund_simulation
from .fund_mode import evaluate_fund_status
from .live_data_engine import (
    format_portfolio_brain_hint,
    format_regime_reason_line,
    get_aggregate_context,
    get_market_payload,
    get_runner_live_sim_feed,
    get_status_payload,
    get_symbol_payload,
    refresh_once as live_data_refresh_once,
    start_live_data_background_loop,
)
from .investor_snapshot import get_investor_research_snapshot
from .merged_ohlc_dataset import (
    build_merged_daily_panel,
    merged_panel_path,
    validate_data_pipeline,
)
from .live_control_engine import DEFAULT_INTERVAL_SEC as LIVE_CONTROL_INTERVAL_SEC
from .live_control_engine import LiveControlEngine
from .live_safe_mode import evaluate_live_safe_candidate
from .meta_ai import build_meta_status
from .market_regime_engine import (
    advisory_line_for_meta,
    build_market_regime_payload,
    build_regime_recommendations_response,
    meta_regime_diagnostic,
)
from . import market_regime_engine as regime_debug_engine
from .long_term_memory_engine import (
    aggregate_family_memory,
    aggregate_regime_memory,
    aggregate_strategy_memory,
    build_memory_status_payload,
    load_memory_state,
    memory_hint_for_regime_engine,
    memory_hints_for_meta,
    run_memory_update_cycle,
)
from .meta_ai_control_engine import (
    build_meta_ai_control_status,
    build_meta_recommendations_payload,
)
from .self_improving_meta_engine import (
    build_learning_insights_payload,
    build_learning_status_payload,
    load_learning_state,
    run_learning_update_cycle,
)
from .production_hardening_engine import (
    HEALTH_CRITICAL,
    HEALTH_HEALTHY,
    HEALTH_DEGRADED,
    build_engine_health_map,
    build_errors_payload,
    build_snapshot_payload,
    build_system_health_payload,
    load_health_state,
    load_snapshot_state,
    record_error,
    save_health_state,
    save_snapshot_state,
)
from .distributed_runner_cluster import (
    apply_offline_detection,
    build_cluster_runners_payload,
    build_cluster_status_payload,
    estimate_failover_reassignments,
    heartbeat_runner as cluster_heartbeat_runner,
    load_cluster_state,
    mark_runner_offline as cluster_mark_runner_offline,
    register_runner as cluster_register_runner,
    save_cluster_state,
)
from .multi_runner_engine import (
    RUNNER_BUSY,
    RUNNER_IDLE,
    count_assigned_jobs_for_runner,
    effective_runner_status,
    group_jobs_by_runner,
    is_assignable_effective,
    job_needs_fleet_assignment,
    plan_balanced_assignments,
    build_fleet_summary,
)
from .paper_trading_engine import MAX_PAPER_BOTS, select_paper_candidates, simulate_paper_metrics
from .feedback_engine import evaluate_paper_feedback
from .operator_console import build_operator_console_status
from .performance_engine import (
    OUTCOME_FAIL,
    OUTCOME_SUCCESS,
    SOURCE_RUNNER,
    build_strategies_performance,
    build_system_performance,
    build_top_strategies,
    duration_sec_between,
)
from .smart_promotion_engine import (
    PROMOTE_TO_DEMO,
    PROMOTE_TO_EXECUTOR,
    PROMOTE_TO_REVIEW,
    PROMOTE_TO_RUNNER,
    TIER_RANK,
    activity_score_for_row,
    build_promotion_candidates_response,
    highest_tier_for_row,
    stability_score_for_row,
    zero_perf_row,
)
from .portfolio_ai import build_portfolio_allocation
from .reporting_engine import build_report_daily, build_report_summary
from .risk_engine import evaluate_risk
from .scoring import compute_bot_score
from .schemas import AccountUpdate, BotUpdate, BrainOut

app = FastAPI(title="algo-sphere API", version="0.1.0")
_auto_loop_engine: AutoLoopEngine | None = None
_continuous_evolution_loop_engine: ContinuousEvolutionLoopEngine | None = None
_autonomous_fund_engine: AutonomousFundEngine | None = None
_live_control_engine: LiveControlEngine | None = None
_runner_heartbeat_thread: threading.Thread | None = None
_runner_heartbeat_stop = threading.Event()
_system_health_cache_lock = threading.Lock()
_system_health_cache: dict[str, object] | None = None
_system_health_updater_thread: threading.Thread | None = None
_system_health_updater_stop = threading.Event()
_meta_status_cache_lock = threading.Lock()
_meta_status_cache: dict[str, object] | None = None
_meta_status_updater_thread: threading.Thread | None = None
_meta_status_updater_stop = threading.Event()
_regime_status_cache_lock = threading.Lock()
_regime_status_cache: dict[str, object] | None = None
_regime_status_cache_written_at: str | None = None
_regime_status_updater_thread: threading.Thread | None = None
_regime_status_updater_stop = threading.Event()
_risk_status_cache_lock = threading.Lock()
_risk_status_cache: dict[str, object] | None = None
_risk_status_updater_thread: threading.Thread | None = None
_risk_status_updater_stop = threading.Event()
_operator_status_cache_lock = threading.Lock()
_operator_status_cache: dict[str, object] | None = None
_operator_status_updater_thread: threading.Thread | None = None
_operator_status_updater_stop = threading.Event()
_started_at_ts: float = time.time()

_RUNNER_HEARTBEAT_TARGETS: tuple[dict[str, object], ...] = (
    {
        "runner_id": "node-a",
        "hostname": "host-a",
        "ip": "10.0.0.11",
        "capacity": 6,
        "region": "us-east",
    },
    {
        "runner_id": "node-b",
        "hostname": "host-b",
        "ip": "10.0.0.12",
        "capacity": 4,
        "region": "us-west",
    },
    {
        "runner_id": "test-runner-1",
        "hostname": "host",
        "ip": "127.0.0.1",
        "capacity": 4,
        "region": "test",
    },
)


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    _restore_production_state_on_startup()
    start_live_data_background_loop()
    start_runner_heartbeat_stabilizer()
    start_system_health_cache_updater()
    start_cockpit_status_cache_updaters()
    prime_cockpit_status_caches()

    def _defer_live_control_start() -> None:
        time.sleep(0.15)
        try:
            start_live_control_engine()
        except Exception:
            pass

    threading.Thread(target=_defer_live_control_start, name="live_control_deferred_start", daemon=True).start()


def _recalculate_allocations() -> None:
    with get_connection() as conn:
        account = fetch_account(conn)
        bots = fetch_bots(conn)
        bot_scores = {bot["id"]: bot["score"] for bot in bots}
        allocations = allocate_capital(
            total_balance=account["balance"],
            bot_scores=bot_scores,
            risk_limit=account["risk_limit"],
        )
        for bot_id, capital in allocations.items():
            conn.execute(
                "UPDATE bots SET capital_alloc = ? WHERE id = ?",
                (capital, bot_id),
            )
        conn.commit()


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "ok", "service": "algo-sphere"}


@app.post("/account/update")
def update_account(payload: AccountUpdate) -> dict[str, float]:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE account
            SET balance = ?, risk_limit = ?
            WHERE id = 1
            """,
            (payload.balance, payload.risk_limit),
        )
        conn.commit()
    _recalculate_allocations()
    return {"balance": payload.balance, "risk_limit": payload.risk_limit}


@app.post("/bot/update")
def upsert_bot(payload: BotUpdate) -> dict[str, str | float]:
    score = compute_bot_score(
        profit=payload.profit,
        drawdown=payload.drawdown,
        win_rate=payload.win_rate,
        trades=payload.trades,
    )
    risk_level = evaluate_risk(drawdown=payload.drawdown, win_rate=payload.win_rate)
    decision = get_execution_decision(score=score, risk_level=risk_level)
    ctrl = compute_control(decision=decision, risk_level=risk_level, score=score)

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO bots (
                name, profit, drawdown, win_rate, trades, score, risk_level, capital_alloc, decision,
                control_state, control_active, alloc_multiplier, control_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                profit = excluded.profit,
                drawdown = excluded.drawdown,
                win_rate = excluded.win_rate,
                trades = excluded.trades,
                score = excluded.score,
                risk_level = excluded.risk_level,
                decision = excluded.decision,
                control_state = excluded.control_state,
                control_active = excluded.control_active,
                alloc_multiplier = excluded.alloc_multiplier,
                control_reason = excluded.control_reason
            """,
            (
                payload.name,
                payload.profit,
                payload.drawdown,
                payload.win_rate,
                payload.trades,
                score,
                risk_level,
                0.0,
                decision,
                ctrl.state,
                1 if ctrl.active else 0,
                ctrl.alloc_multiplier,
                ctrl.reason,
            ),
        )
        conn.commit()
    _recalculate_allocations()
    bots_after = get_bots()
    eff = next(
        (
            b["effective_capital"]
            for b in bots_after["bots"]
            if b["name"] == payload.name
        ),
        0.0,
    )
    return {
        "name": payload.name,
        "score": score,
        "risk_level": risk_level,
        "decision": decision,
        "control_state": ctrl.state,
        "control_active": ctrl.active,
        "alloc_multiplier": ctrl.alloc_multiplier,
        "effective_capital": eff,
        "control_reason": ctrl.reason,
    }


@app.get("/bots")
def get_bots() -> dict[str, object]:
    with get_connection() as conn:
        bots = fetch_bots(conn)
        total_profit = round(sum(bot["profit"] for bot in bots), 2)
    return {"count": len(bots), "total_profit": total_profit, "bots": bots}


def _build_live_control_context() -> dict[str, Any]:
    with get_connection() as conn:
        bots = fetch_bots(conn)
    return {
        "bots": bots,
        "risk_status": get_global_risk_status(),
        "meta_status": get_meta_ai_control_status(),
        "regime_status": get_market_regime_status(),
        "live_data_context": get_aggregate_context(),
        "portfolio_allocation": get_portfolio_allocation(),
        "system_health": get_system_health(),
        "cluster_status": get_cluster_status(),
    }


def _get_live_control_engine() -> LiveControlEngine:
    global _live_control_engine
    if _live_control_engine is None:
        _live_control_engine = LiveControlEngine(
            context_provider=_build_live_control_context,
            interval_sec=LIVE_CONTROL_INTERVAL_SEC,
            persist_state=True,
        )
    return _live_control_engine


def start_live_control_engine() -> dict[str, object]:
    return _get_live_control_engine().start()


def get_control_signals() -> dict[str, object]:
    """
    Live read of control signals from in-memory AI control engine.
    Falls back to persisted bot rows only if live payload is unavailable.
    """
    try:
        eng = _get_live_control_engine()
        payload = eng.get_payload()
        if not payload.get("signals"):
            payload = eng.recompute_once()
        return payload
    except Exception:
        with get_connection() as conn:
            bots = fetch_bots(conn)
        signals: list[dict[str, object]] = []
        for b in bots:
            state = str(b.get("control_state") or "MONITOR")
            signals.append(
                {
                    "name": b["name"],
                    "control_state": state,
                    "control_active": bool(b.get("control_active", True)),
                    "effective_capital": float(b.get("effective_capital") or 0.0),
                    "entriesEnabled": bool(b.get("control_active", True)),
                    "target_volume": int(float(b.get("effective_capital") or 0.0)),
                    "recommended_action": recommended_action_for_state(state),
                    "reasoning": ["fallback_from_db_snapshot"],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        return {"count": len(signals), "signals": signals, "live_engine": False}


@app.get("/control/signals")
def control_signals() -> dict[str, object]:
    return get_control_signals()


def _collect_fund_engine_inputs() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """DB + paper only — avoids recursion via operator/report/fund_status."""
    with get_connection() as conn:
        runs = fetch_performance_run_log(conn)
        factory = fetch_factory_strategies(conn)
    paper = list(get_paper_status().get("running_paper_bots", []) or [])
    return runs, factory, paper


def _build_fund_engine_bundle(
    total_capital: float | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    runs, factory, paper = _collect_fund_engine_inputs()
    perf_rows = build_strategies_performance(factory, runs, paper)
    tc = float(total_capital) if total_capital is not None else float(DEFAULT_TOTAL_CAPITAL)
    return build_fund_simulation(perf_rows=perf_rows, paper_items=paper, total_capital=tc)


def _load_fund_rebalance_history() -> list[dict[str, Any]]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, "fund_engine_rebalance_history") or "[]"
    try:
        hist = json.loads(raw)
    except json.JSONDecodeError:
        hist = []
    return hist if isinstance(hist, list) else []


def get_fund_status() -> dict[str, object]:
    with get_connection() as conn:
        bots = fetch_bots(conn)
    out: dict[str, Any] = evaluate_fund_status(bots)
    st, _, _ = _build_fund_engine_bundle()
    out["fund_engine"] = st
    return out


def get_fund_allocation_status() -> dict[str, object]:
    """Fund Engine snapshot (simulated capital only)."""
    st, _, _ = _build_fund_engine_bundle()
    return dict(st)


def get_fund_portfolio() -> dict[str, object]:
    _, strategies, meta = _build_fund_engine_bundle()
    hist = _load_fund_rebalance_history()
    last = hist[0].get("rebalanced_at") if hist else None
    return {
        "strategies": strategies,
        "last_rebalance_at": last,
        "rebalance_history": hist[:20],
        "meta": meta,
    }


def post_fund_rebalance() -> dict[str, object]:
    status, strategies, meta = _build_fund_engine_bundle()
    ts = datetime.now(timezone.utc).isoformat()
    entry = {
        "rebalanced_at": ts,
        "allocated_capital": status["allocated_capital"],
        "free_capital": status["free_capital"],
        "strategy_count": len(strategies),
        "risk_score": status["risk_score"],
    }
    hist = _load_fund_rebalance_history()
    hist.insert(0, entry)
    hist = hist[:30]
    with get_connection() as conn:
        set_alert_engine_state(conn, "fund_engine_rebalance_history", json.dumps(hist))
    return {
        "ok": True,
        "rebalanced_at": ts,
        **status,
        "strategies": strategies,
        "meta": meta,
    }


def get_fund_signals() -> dict[str, object]:
    """
    Compact portfolio-level signal payload for lightweight consumers.
    """
    status = get_fund_status()
    summary = status.get("summary", {})
    counts = status.get("state_counts", {})
    return {
        "portfolio_state": status.get("portfolio_state", "NORMAL"),
        "recommended_action": status.get(
            "recommended_portfolio_action", "KEEP_RUNNING"
        ),
        "active_bot_count": int(summary.get("active_bot_count", 0)),
        "kill_bot_count": int(counts.get("kill_bot_count", 0)),
        "reduce_bot_count": int(counts.get("reduce_bot_count", 0)),
        "reasoning": status.get("reasoning", ""),
    }


@app.get("/fund/status")
def fund_status() -> dict[str, object]:
    return get_fund_status()


@app.get("/fund/signals")
def fund_signals() -> dict[str, object]:
    return get_fund_signals()


@app.get("/fund/allocation/status")
def fund_allocation_status() -> dict[str, object]:
    """Simulated fund capital (strategy performance–based). Bot-level fund mode remains GET /fund/status."""
    return get_fund_allocation_status()


@app.get("/fund/portfolio")
def fund_portfolio() -> dict[str, object]:
    return get_fund_portfolio()


@app.post("/fund/rebalance")
def fund_rebalance() -> dict[str, object]:
    return post_fund_rebalance()


def get_factory_strategies() -> dict[str, object]:
    with get_connection() as conn:
        items = fetch_factory_strategies(conn)
    return {"count": len(items), "strategies": items}


def generate_factory_strategies(count: int = 12, seed: int | None = None) -> dict[str, object]:
    generated = generate_candidates(count=count, seed=seed)
    with get_connection() as conn:
        insert_factory_strategies(conn, generated)
        all_items = fetch_factory_strategies(conn)
    return {
        "generated_count": len(generated),
        "total_count": len(all_items),
        "strategies": generated,
    }


def get_factory_top(limit: int = 5) -> dict[str, object]:
    with get_connection() as conn:
        items = fetch_factory_strategies(conn)
    top = rank_candidates(items, limit=limit)
    return {"count": len(top), "top": top}


def evolve_factory_strategies(
    top_n: int = 5,
    children_per_parent: int = 2,
    crossover_rate: float = 0.3,
    seed: int | None = None,
) -> dict[str, object]:
    with get_connection() as conn:
        existing = fetch_factory_strategies(conn)
        evolved = evolve_candidates(
            existing=existing,
            top_n=top_n,
            children_per_parent=children_per_parent,
            crossover_rate=crossover_rate,
            seed=seed,
        )
        if evolved:
            insert_factory_strategies(conn, evolved)
        all_items = fetch_factory_strategies(conn)
    return {
        "evolved_count": len(evolved),
        "total_count": len(all_items),
        "strategies": evolved,
        "note": "Evolved strategies are candidates only; human review required before any live bot usage.",
    }


def get_factory_lineage(strategy_id: str) -> dict[str, object]:
    with get_connection() as conn:
        node = fetch_factory_strategy_by_id(conn, strategy_id)
        if node is None:
            return {"strategy_id": strategy_id, "found": False, "lineage": []}
        lineage = [node]
        parent = node.get("parent_strategy_id")
        hops = 0
        while parent and hops < 20:
            parent_node = fetch_factory_strategy_by_id(conn, str(parent))
            if parent_node is None:
                break
            lineage.append(parent_node)
            parent = parent_node.get("parent_strategy_id")
            hops += 1
    return {"strategy_id": strategy_id, "found": True, "lineage": lineage}


@app.get("/factory/strategies")
def factory_strategies() -> dict[str, object]:
    return get_factory_strategies()


@app.post("/factory/generate")
def factory_generate(count: int = 12, seed: int | None = None) -> dict[str, object]:
    return generate_factory_strategies(count=count, seed=seed)


@app.get("/factory/top")
def factory_top(limit: int = 5) -> dict[str, object]:
    return get_factory_top(limit=limit)


@app.post("/factory/evolve")
def factory_evolve(
    top_n: int = 5,
    children_per_parent: int = 2,
    crossover_rate: float = 0.3,
    seed: int | None = None,
) -> dict[str, object]:
    return evolve_factory_strategies(
        top_n=top_n,
        children_per_parent=children_per_parent,
        crossover_rate=crossover_rate,
        seed=seed,
    )


@app.get("/factory/lineage/{strategy_id}")
def factory_lineage(strategy_id: str) -> dict[str, object]:
    return get_factory_lineage(strategy_id)


def get_evolution_candidates() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf = build_strategies_performance(factory, runs, paper)
    out = build_evolution_candidates_payload(factory, perf)
    return out


def post_evolution_run(
    seed: int | None = None,
    max_weak: int = 5,
    max_strong: int = 5,
) -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf = build_strategies_performance(factory, runs, paper)
    use_seed = 42 if seed is None else int(seed)
    created, skipped = run_evolution_batch(
        factory,
        perf,
        seed=use_seed,
        max_weak=max_weak,
        max_strong=max_strong,
    )
    if created:
        with get_connection() as conn:
            insert_factory_strategies(conn, created)
    return {
        "created_variants": [variant_summary_row(c) for c in created],
        "skipped": skipped,
        "evolution_only": True,
        "demo_simulation_only": True,
        "seed": use_seed,
    }


def get_evolution_lineage() -> dict[str, object]:
    with get_connection() as conn:
        factory = fetch_factory_strategies(conn)
    return {
        "lineage": build_lineage_payload(factory),
        "evolution_only": True,
        "demo_simulation_only": True,
    }


@app.get("/evolution/candidates")
def evolution_candidates() -> dict[str, object]:
    return get_evolution_candidates()


@app.post("/evolution/run")
def evolution_run(
    seed: int | None = None,
    max_weak: int = 5,
    max_strong: int = 5,
) -> dict[str, object]:
    return post_evolution_run(seed=seed, max_weak=max_weak, max_strong=max_strong)


@app.get("/evolution/lineage")
def evolution_lineage() -> dict[str, object]:
    return get_evolution_lineage()


@app.get("/evolution_loop/status")
def evolution_loop_status() -> dict[str, object]:
    return get_continuous_evolution_loop_status()


@app.post("/evolution_loop/start")
def evolution_loop_start(
    interval_sec: int = 120,
    max_loops_per_hour: int = 12,
    max_weak: int = 5,
    max_strong: int = 5,
) -> dict[str, object]:
    return start_continuous_evolution_loop(
        interval_sec=interval_sec,
        max_loops_per_hour=max_loops_per_hour,
        max_weak=max_weak,
        max_strong=max_strong,
    )


@app.post("/evolution_loop/pause")
def evolution_loop_pause() -> dict[str, object]:
    return pause_continuous_evolution_loop()


@app.post("/evolution_loop/run_once")
def evolution_loop_run_once() -> dict[str, object]:
    return run_continuous_evolution_loop_once()


def deploy_paper_bots(max_bots: int = MAX_PAPER_BOTS) -> dict[str, object]:
    with get_connection() as conn:
        all_strategies = fetch_factory_strategies(conn)
        existing_paper = fetch_paper_bots(conn)
        running_ids = {
            x["strategy_id"]
            for x in existing_paper
            # Capacity should track active paper load only.
            # PAPER_SUCCESS rows are historical outcomes and should not consume slots.
            if x.get("status") == "PAPER_RUNNING"
        }
        selected = select_paper_candidates(
            strategies=all_strategies,
            running_strategy_ids=running_ids,
            max_bots=max_bots,
        )

        deployed: list[dict[str, object]] = []
        for s in selected:
            sim = simulate_paper_metrics(
                strategy_id=s["strategy_id"],
                fitness_score=float(s.get("fitness_score", 0.0)),
            )
            paper_item = {
                "strategy_id": s["strategy_id"],
                "family": s["family"],
                "status": sim["status"],
                "paper_profit": sim["paper_profit"],
                "paper_drawdown": sim["paper_drawdown"],
                "paper_win_rate": sim["paper_win_rate"],
                "paper_trades": sim["paper_trades"],
                "deployed_at": sim["last_updated"],
                "last_updated": sim["last_updated"],
                "sim_note": str(sim.get("sim_note") or "paper simulation only; no live trading"),
            }
            upsert_paper_bot(conn, paper_item)
            # Mark strategy as approved/running in paper lifecycle
            next_status = "APPROVED_FOR_PAPER"
            if sim["status"] in {"PAPER_RUNNING", "PAPER_REJECTED", "PAPER_SUCCESS"}:
                next_status = sim["status"]
            update_factory_strategy_status(conn, s["strategy_id"], next_status)
            deployed.append(
                {
                    "strategy_id": s["strategy_id"],
                    "family": s["family"],
                    "status": sim["status"],
                    "paper_profit": sim["paper_profit"],
                    "paper_drawdown": sim["paper_drawdown"],
                    "paper_win_rate": sim["paper_win_rate"],
                    "paper_trades": sim["paper_trades"],
                }
            )
        all_paper = fetch_paper_bots(conn)
    return {
        "max_paper_bots": max_bots,
        "deployed_count": len(deployed),
        "running_count": sum(1 for x in all_paper if x["status"] == "PAPER_RUNNING"),
        "paper_bots": deployed,
        "note": "Paper mode only. No live trading or real money deployment.",
    }


def get_paper_status() -> dict[str, object]:
    with get_connection() as conn:
        items = fetch_paper_bots(conn)
    return {
        "count": len(items),
        "running_paper_bots": items,
        "summary": {
            "profit": round(sum(float(i.get("paper_profit", 0.0)) for i in items), 2),
            "drawdown": round(sum(float(i.get("paper_drawdown", 0.0)) for i in items), 2),
            "win_rate": round(
                (
                    sum(float(i.get("paper_win_rate", 0.0)) for i in items) / len(items)
                    if items
                    else 0.0
                ),
                3,
            ),
            "trades": int(sum(int(i.get("paper_trades", 0)) for i in items)),
        },
    }


def _build_feedback_payload(items: list[dict[str, object]]) -> dict[str, object]:
    results: list[dict[str, object]] = []
    for x in items:
        fb = evaluate_paper_feedback(x)
        results.append(
            {
                "strategy_id": x.get("strategy_id"),
                "family": x.get("family"),
                "current_paper_status": x.get("status"),
                "feedback_score": fb["feedback_score"],
                "promotion_score": fb["promotion_score"],
                "rejection_flag": fb["rejection_flag"],
                "action": fb["action"],
                "target_status": fb["target_status"],
                "reasoning": fb["reasoning"],
            }
        )
    return {"count": len(results), "results": results}


def get_paper_feedback_preview() -> dict[str, object]:
    with get_connection() as conn:
        items = fetch_paper_bots(conn)
    payload = _build_feedback_payload(items)
    payload["mode"] = "preview"
    return payload


def apply_paper_feedback() -> dict[str, object]:
    with get_connection() as conn:
        items = fetch_paper_bots(conn)
        payload = _build_feedback_payload(items)
        for r in payload["results"]:
            sid = str(r["strategy_id"])
            target = str(r["target_status"])
            update_factory_strategy_status(conn, sid, target)
            if target in {"PAPER_SUCCESS", "PAPER_REJECTED"}:
                conn.execute(
                    "UPDATE paper_bots SET status = ? WHERE strategy_id = ?",
                    (target, sid),
                )
                ended = datetime.now(timezone.utc).isoformat()
                insert_performance_run(
                    conn,
                    strategy_id=sid,
                    outcome=OUTCOME_SUCCESS if target == "PAPER_SUCCESS" else OUTCOME_FAIL,
                    duration_sec=0.0,
                    run_ended_at=ended,
                    source="paper",
                    commit=False,
                )
        conn.commit()
    payload["mode"] = "applied"
    payload["note"] = "Feedback updates strategy lifecycle only; no live deployment."
    return payload


def _continuous_evolution_performance_snapshot() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf = build_strategies_performance(factory, runs, paper)
    if not perf:
        return {
            "strategies_with_metrics": 0,
            "max_performance_score": 0.0,
            "note": "read_only",
        }
    scores = [float(x.get("performance_score", 0.0)) for x in perf]
    return {
        "strategies_with_metrics": len(perf),
        "max_performance_score": round(max(scores), 4),
        "total_runs_logged": sum(int(x.get("total_runs", 0)) for x in perf),
        "note": "read_only aggregation; no trading",
    }


def _continuous_evolution_step() -> dict[str, object]:
    eng = _get_continuous_evolution_loop_engine()
    mw, ms = eng.get_cycle_params()
    return post_evolution_run(seed=None, max_weak=mw, max_strong=ms)


def _get_continuous_evolution_loop_engine() -> ContinuousEvolutionLoopEngine:
    global _continuous_evolution_loop_engine
    if _continuous_evolution_loop_engine is None:
        _continuous_evolution_loop_engine = ContinuousEvolutionLoopEngine(
            evolution_run_fn=_continuous_evolution_step,
            paper_deploy_fn=lambda: deploy_paper_bots(max_bots=MAX_PAPER_BOTS),
            feedback_fn=apply_paper_feedback,
            performance_snapshot_fn=_continuous_evolution_performance_snapshot,
        )
    return _continuous_evolution_loop_engine


def get_continuous_evolution_loop_status() -> dict[str, object]:
    return _get_continuous_evolution_loop_engine().status()


def start_continuous_evolution_loop(
    interval_sec: int = 120,
    max_loops_per_hour: int = 12,
    max_weak: int = 5,
    max_strong: int = 5,
) -> dict[str, object]:
    eng = _get_continuous_evolution_loop_engine()
    started = eng.start(
        interval_sec=interval_sec,
        max_loops_per_hour=max_loops_per_hour,
        max_weak=max_weak,
        max_strong=max_strong,
    )
    return {**started, "status": eng.status()}


def pause_continuous_evolution_loop() -> dict[str, object]:
    eng = _get_continuous_evolution_loop_engine()
    paused = eng.pause()
    return {**paused, "status": eng.status()}


def run_continuous_evolution_loop_once() -> dict[str, object]:
    eng = _get_continuous_evolution_loop_engine()
    return eng.run_once()


def _get_autonomous_fund_engine() -> AutonomousFundEngine:
    global _autonomous_fund_engine

    def _meta_with_learning() -> dict[str, Any]:
        # Autonomous loop hook: capture each posture decision into self-learning state.
        ctrl = get_meta_ai_control_status()
        _ = post_meta_learning_update(control_status=ctrl)
        return ctrl

    if _autonomous_fund_engine is None:
        _autonomous_fund_engine = AutonomousFundEngine(
            regime_status_fn=get_market_regime_status,
            risk_status_fn=get_global_risk_status,
            memory_update_fn=post_memory_update,
            evolution_run_fn=post_evolution_run,
            continuous_run_once_fn=run_continuous_evolution_loop_once,
            paper_deploy_fn=lambda max_bots: deploy_paper_bots(max_bots=max_bots),
            performance_system_fn=get_performance_system,
            portfolio_rotation_fn=get_portfolio_allocation,
            fund_rebalance_fn=post_fund_rebalance,
            multi_runner_assign_fn=multi_runner_assign_jobs,
            meta_status_fn=_meta_with_learning,
        )
    return _autonomous_fund_engine


def get_autonomous_fund_status() -> dict[str, object]:
    return _get_autonomous_fund_engine().status()


def start_autonomous_fund_mode(
    interval_sec: int = 120,
    max_loops_per_hour: int = 20,
) -> dict[str, object]:
    eng = _get_autonomous_fund_engine()
    started = eng.start(interval_sec=interval_sec, max_loops_per_hour=max_loops_per_hour)
    return {**started, "status": eng.status()}


def pause_autonomous_fund_mode() -> dict[str, object]:
    eng = _get_autonomous_fund_engine()
    paused = eng.pause()
    return {**paused, "status": eng.status()}


def run_autonomous_fund_once() -> dict[str, object]:
    eng = _get_autonomous_fund_engine()
    return eng.run_once()


def _restore_production_state_on_startup() -> dict[str, object]:
    """Best-effort restore from last snapshot; keep safe mode semantics."""
    snap = load_snapshot_state()
    hs = load_health_state()
    if not snap:
        return {"restored": False, "reason": "no_snapshot"}
    try:
        auto = snap.get("autonomous_state") or {}
        last_state = str(auto.get("state", "") or "")
        interval = int(auto.get("interval_sec", 120) or 120)
        mph = int(auto.get("max_loops_per_hour", 20) or 20)
        if last_state == "AUTONOMOUS_RUNNING":
            start_autonomous_fund_mode(
                interval_sec=max(120, interval),  # safe resume floor
                max_loops_per_hour=max(1, mph),
            )
        hs["last_recovery_at"] = datetime.now(timezone.utc).isoformat()
        save_health_state(hs)
        return {"restored": True, "resumed_autonomous": last_state == "AUTONOMOUS_RUNNING"}
    except Exception as exc:  # pragma: no cover - defensive startup guard
        hs = record_error(hs, source="startup_restore", message=str(exc))
        save_health_state(hs)
        return {"restored": False, "error": str(exc)}


def _compute_system_health_payload() -> dict[str, object]:
    hs = load_health_state()
    autonomous = get_autonomous_fund_status()
    meta = get_meta_ai_control_status()
    evolution = get_continuous_evolution_loop_status()
    runner = get_runner_status()
    risk = get_global_risk_status()
    regime = get_market_regime_status()
    memory = get_long_term_memory_status()
    cluster = _get_cluster_status_snapshot_readonly()

    engines = build_engine_health_map(
        autonomous_status=autonomous,
        meta_status=meta,
        evolution_status=evolution,
        runner_status=runner,
        risk_status=risk,
        regime_status=regime,
        memory_status=memory,
        distributed_cluster_status=cluster,
    )

    # Auto-restart safety: only for autonomous orchestrator and only when degraded with errors.
    if engines.get("autonomous_engine") == HEALTH_DEGRADED:
        errs = autonomous.get("errors") or []
        if errs and str(autonomous.get("state", "")) != "AUTONOMOUS_RUNNING":
            try:
                st = start_autonomous_fund_mode(interval_sec=120, max_loops_per_hour=20)
                hs.setdefault("auto_restarts", []).append(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "engine": "autonomous_engine",
                        "result": "started" if st.get("started") else "skipped",
                    }
                )
                hs["auto_restarts"] = (hs.get("auto_restarts") or [])[-20:]
            except Exception as exc:
                hs = record_error(hs, source="auto_restart.autonomous", message=str(exc))
            save_health_state(hs)

    return build_system_health_payload(
        uptime_sec=time.time() - _started_at_ts,
        engines=engines,
        health_state=hs,
    )


def _system_health_cache_loop(interval_sec: int = 10) -> None:
    global _system_health_cache
    while True:
        try:
            payload = _compute_system_health_payload()
            with _system_health_cache_lock:
                _system_health_cache = dict(payload)
        except Exception:
            pass
        if _system_health_updater_stop.wait(timeout=max(3, int(interval_sec))):
            break


def start_system_health_cache_updater(interval_sec: int = 10) -> dict[str, object]:
    global _system_health_updater_thread
    if _system_health_updater_thread is not None and _system_health_updater_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _system_health_updater_stop.clear()
    _system_health_updater_thread = threading.Thread(
        target=lambda: _system_health_cache_loop(interval_sec=interval_sec),
        name="system_health_cache_updater",
        daemon=True,
    )
    _system_health_updater_thread.start()
    return {"started": True, "interval_sec": interval_sec}


def start_cockpit_status_cache_updaters(interval_sec: int = 10) -> dict[str, object]:
    return {
        "meta": start_meta_status_cache_updater(interval_sec=interval_sec),
        "risk": start_risk_status_cache_updater(interval_sec=interval_sec),
        "regime": start_regime_status_cache_updater(interval_sec=interval_sec),
        "operator": start_operator_status_cache_updater(interval_sec=interval_sec),
    }


def prime_cockpit_status_caches() -> None:
    """
    Warm health/operator caches without blocking app startup (large factory_strategies tables
    can make synchronous _compute_* take minutes).
    """
    global _system_health_cache, _operator_status_cache

    def _prime_health_async() -> None:
        global _system_health_cache
        try:
            payload = _compute_system_health_payload()
            with _system_health_cache_lock:
                _system_health_cache = dict(payload)
        except Exception:
            pass

    def _prime_operator_async() -> None:
        global _operator_status_cache
        try:
            op = _compute_operator_console_status_payload()
            with _operator_status_cache_lock:
                _operator_status_cache = dict(op)
        except Exception:
            pass

    threading.Thread(target=_prime_health_async, name="system_health_prime", daemon=True).start()
    threading.Thread(target=_prime_operator_async, name="operator_status_prime", daemon=True).start()


def get_system_health() -> dict[str, object]:
    with _system_health_cache_lock:
        cached = dict(_system_health_cache) if isinstance(_system_health_cache, dict) else None
    if cached:
        return cached
    hs = load_health_state()
    autonomous = get_autonomous_fund_status()
    evolution = get_continuous_evolution_loop_status()
    runner = get_runner_status()
    cluster = _get_cluster_status_snapshot_readonly()
    fallback_engines: dict[str, str] = {
        "autonomous_engine": HEALTH_HEALTHY
        if str(autonomous.get("state", "") or "") == "AUTONOMOUS_RUNNING"
        else HEALTH_DEGRADED,
        "meta_ai_engine": HEALTH_DEGRADED,
        "evolution_engine": HEALTH_HEALTHY
        if str(evolution.get("state", "") or "")
        in {"EVOLUTION_RUNNING", "EVOLUTION_IDLE", "EVOLUTION_PAUSED"}
        else HEALTH_DEGRADED,
        "runner_engine": HEALTH_HEALTHY
        if int((runner.get("counts") or {}).get("RUNNER_ACTIVE", 0) or 0) >= 0
        else HEALTH_DEGRADED,
        "risk_engine": HEALTH_DEGRADED,
        "regime_engine": HEALTH_DEGRADED,
        "memory_engine": HEALTH_DEGRADED,
        "distributed_cluster_engine": HEALTH_CRITICAL
        if str(cluster.get("cluster_health", "") or "") == HEALTH_CRITICAL
        else (HEALTH_DEGRADED if str(cluster.get("cluster_health", "") or "") == HEALTH_DEGRADED else HEALTH_HEALTHY),
    }
    return build_system_health_payload(
        uptime_sec=time.time() - _started_at_ts,
        engines=fallback_engines,
        health_state=hs,
    )


def get_system_errors() -> dict[str, object]:
    return build_errors_payload(load_health_state())


def post_system_snapshot() -> dict[str, object]:
    hs = load_health_state()
    snap = build_snapshot_payload(
        memory_state=get_long_term_memory_status(),
        portfolio_state=get_portfolio_allocation(),
        meta_state=get_meta_ai_control_status(),
        evolution_state=get_continuous_evolution_loop_status(),
        autonomous_state=get_autonomous_fund_status(),
    )
    save_snapshot_state(snap)
    hs["last_snapshot_at"] = snap.get("snapshot_at")
    save_health_state(hs)
    return {
        "ok": True,
        "snapshot_at": snap.get("snapshot_at"),
        "decision_layer_only": True,
        "stability_layer_only": True,
    }


def _get_auto_loop_engine() -> AutoLoopEngine:
    global _auto_loop_engine
    if _auto_loop_engine is None:
        _auto_loop_engine = AutoLoopEngine(
            generate_fn=lambda: generate_factory_strategies(count=12),
            evolve_fn=lambda: evolve_factory_strategies(
                top_n=5, children_per_parent=2, crossover_rate=0.3
            ),
            paper_deploy_fn=lambda: deploy_paper_bots(max_bots=MAX_PAPER_BOTS),
            feedback_fn=apply_paper_feedback,
            strategy_count_fn=lambda: get_factory_strategies()["count"],
        )
    return _auto_loop_engine


def run_auto_cycle_once() -> dict[str, object]:
    engine = _get_auto_loop_engine()
    return engine.run_cycle()


def start_auto_loop(
    interval_sec: int = 60,
    max_loops_per_hour: int = 30,
    max_strategies: int = 1000,
) -> dict[str, object]:
    engine = _get_auto_loop_engine()
    started = engine.start(
        interval_sec=interval_sec,
        max_loops_per_hour=max_loops_per_hour,
        max_strategies=max_strategies,
    )
    return {"start": started, "status": engine.status()}


def stop_auto_loop() -> dict[str, object]:
    engine = _get_auto_loop_engine()
    stopped = engine.stop()
    return {"stop": stopped, "status": engine.status()}


def get_auto_loop_status() -> dict[str, object]:
    return _get_auto_loop_engine().status()


def _build_live_safe_results() -> list[dict[str, object]]:
    with get_connection() as conn:
        papers = fetch_paper_bots(conn)
        out: list[dict[str, object]] = []
        for p in papers:
            s = fetch_factory_strategy_by_id(conn, str(p["strategy_id"]))
            if s is None:
                continue
            out.append(evaluate_live_safe_candidate(s, p))
    return out


def get_live_safe_candidates() -> dict[str, object]:
    items = _build_live_safe_results()
    candidates = [
        x
        for x in items
        if x["target_status"] in {"LIVE_SAFE_CANDIDATE", "APPROVED_FOR_LIVE_REVIEW", "LIVE_SAFE_READY"}
    ]
    return {"count": len(candidates), "candidates": candidates}


def promote_live_safe() -> dict[str, object]:
    items = _build_live_safe_results()
    with get_connection() as conn:
        for x in items:
            update_factory_strategy_status(conn, str(x["strategy_id"]), str(x["target_status"]))
    return {
        "count": len(items),
        "results": items,
        "note": "Live Safe Mode is review-only. No live trading, no broker execution, manual approval required.",
    }


def get_live_safe_status() -> dict[str, object]:
    items = _build_live_safe_results()
    counts = {
        "approved_for_live_review": sum(1 for x in items if x["target_status"] == "APPROVED_FOR_LIVE_REVIEW"),
        "live_safe_candidate": sum(1 for x in items if x["target_status"] == "LIVE_SAFE_CANDIDATE"),
        "live_safe_rejected": sum(1 for x in items if x["target_status"] == "LIVE_SAFE_REJECTED"),
        "live_safe_ready": sum(1 for x in items if x["target_status"] == "LIVE_SAFE_READY"),
    }
    return {
        "count": len(items),
        "state_counts": counts,
        "review_only": True,
        "manual_approval_required": True,
    }


def get_portfolio_allocation() -> dict[str, object]:
    live = get_live_safe_candidates()
    _, fund_strategies, _ = _build_fund_engine_bundle()
    out = build_portfolio_allocation(
        live_safe_candidates=live.get("candidates", []),
        max_percent_per_strategy=20.0,
        fund_portfolio_strategies=list(fund_strategies),
    )
    out = dict(out)
    ctx = get_aggregate_context()
    hint = format_portfolio_brain_hint(ctx)
    brain = out.get("brain")
    if isinstance(brain, dict):
        brain = dict(brain)
        if hint:
            brain["live_market_hint"] = hint
        out["brain"] = brain
    out["live_data_context"] = ctx
    return out


def get_legacy_meta_status() -> dict[str, object]:
    """Legacy Meta AI snapshot (system_health, risk_mode, …) for reports, alerts, operator console."""
    fund = get_fund_status()
    factory = get_factory_strategies().get("strategies", [])
    paper = get_paper_status()
    auto = get_auto_loop_status()
    portfolio = get_portfolio_allocation()
    out = build_meta_status(
        fund_status=fund,
        factory_strategies=factory,
        paper_status=paper,
        auto_status=auto,
        portfolio_brain=portfolio.get("brain"),
    )
    mreg = build_market_regime_payload(
        performance_system=_get_performance_system_light(),
        portfolio_allocation=portfolio,
        fund_allocation_status=get_fund_allocation_status(),
        paper_status=paper,
        global_risk_assessment=_build_global_risk_full_light(),
        meta_status=out,
        factory_strategies=list(factory),
        strategies_performance=list(get_performance_strategies_light().get("strategies", [])),
    )
    diag = out.setdefault("diagnostics", {})
    if isinstance(diag, dict):
        diag["market_regime"] = meta_regime_diagnostic(mreg)
        diag["long_term_memory"] = memory_hints_for_meta(load_memory_state())
    recs = out.setdefault("recommendations", [])
    if isinstance(recs, list):
        recs.append(advisory_line_for_meta(mreg))
    return out


def _compute_meta_ai_control_status_payload() -> dict[str, object]:
    """Top-level Meta AI Control orchestration (posture, confidence, diagnostics)."""
    fund = get_fund_status()
    factory = list(get_factory_strategies().get("strategies", []))
    paper = get_paper_status()
    auto = get_auto_loop_status()
    portfolio = get_portfolio_allocation()
    legacy = build_meta_status(
        fund_status=fund,
        factory_strategies=factory,
        paper_status=paper,
        auto_status=auto,
        portfolio_brain=portfolio.get("brain"),
    )
    mem = build_memory_status_payload(load_memory_state(), factory)
    learning = build_learning_insights_payload(load_learning_state())
    out = build_meta_ai_control_status(
        global_risk_full=_build_global_risk_full(),
        regime_status=get_market_regime_status(),
        portfolio_allocation=portfolio,
        memory_payload=mem,
        performance_system=get_performance_system(),
        multi_runner_status=get_multi_runner_status(),
        fund_allocation_status=get_fund_allocation_status(),
        capital_status=get_capital_status(),
        fund_status=fund,
        legacy_meta=legacy,
        learning_insights=learning,
    )
    out = dict(out)
    diag = out.setdefault("diagnostics", {})
    if isinstance(diag, dict):
        diag["live_data"] = get_aggregate_context()
    return out


def _meta_status_fallback_payload() -> dict[str, object]:
    return {
        "system_posture": "HOLD",
        "confidence": 0.0,
        "reasoning": ["Meta status cache warming"],
        "recommendations": ["Maintain conservative posture until fresh diagnostics are available."],
        "diagnostics": {"cache_state": "warming"},
        "decision_layer_only": True,
        "demo_simulation_only": True,
    }


def _meta_status_cache_loop(interval_sec: int = 10) -> None:
    global _meta_status_cache
    while True:
        try:
            payload = _compute_meta_ai_control_status_payload()
            with _meta_status_cache_lock:
                _meta_status_cache = dict(payload)
        except Exception:
            pass
        if _meta_status_updater_stop.wait(timeout=max(3, int(interval_sec))):
            break


def start_meta_status_cache_updater(interval_sec: int = 10) -> dict[str, object]:
    global _meta_status_updater_thread
    if _meta_status_updater_thread is not None and _meta_status_updater_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _meta_status_updater_stop.clear()
    _meta_status_updater_thread = threading.Thread(
        target=lambda: _meta_status_cache_loop(interval_sec=interval_sec),
        name="meta_status_cache_updater",
        daemon=True,
    )
    _meta_status_updater_thread.start()
    return {"started": True, "interval_sec": interval_sec}


def get_meta_ai_control_status() -> dict[str, object]:
    with _meta_status_cache_lock:
        cached = dict(_meta_status_cache) if isinstance(_meta_status_cache, dict) else None
    if cached:
        return cached
    return _meta_status_fallback_payload()


def get_meta_ai_recommendations() -> dict[str, object]:
    return build_meta_recommendations_payload(get_meta_ai_control_status())


def get_meta_learning_status() -> dict[str, object]:
    return build_learning_status_payload(load_learning_state())


def get_meta_learning_insights() -> dict[str, object]:
    return build_learning_insights_payload(load_learning_state())


def post_meta_learning_update(
    control_status: dict[str, Any] | None = None,
) -> dict[str, object]:
    ctrl = control_status or get_meta_ai_control_status()
    return run_learning_update_cycle(
        control_status=ctrl,
        global_risk_status=get_global_risk_status(),
        regime_status=get_market_regime_status(),
        memory_status=get_long_term_memory_status(),
        performance_system=get_performance_system(),
        multi_runner_status=get_multi_runner_status(),
        fund_status=get_fund_status(),
        fund_allocation_status=get_fund_allocation_status(),
    )


def _compute_market_regime_status_payload() -> dict[str, object]:
    fund = get_fund_status()
    factory = get_factory_strategies().get("strategies", [])
    paper = get_paper_status()
    auto = get_auto_loop_status()
    portfolio = get_portfolio_allocation()
    light_meta = build_meta_status(
        fund_status=fund,
        factory_strategies=factory,
        paper_status=paper,
        auto_status=auto,
        portfolio_brain=portfolio.get("brain"),
    )
    payload = build_market_regime_payload(
        performance_system=get_performance_system(),
        portfolio_allocation=portfolio,
        fund_allocation_status=get_fund_allocation_status(),
        paper_status=paper,
        global_risk_assessment=_build_global_risk_full(),
        meta_status=light_meta,
        factory_strategies=list(factory),
        strategies_performance=list(get_performance_strategies().get("strategies", [])),
    )
    hint = memory_hint_for_regime_engine(load_memory_state())
    if hint:
        payload["long_term_memory_hint"] = hint
    ctx = get_aggregate_context()
    payload["live_data_context"] = ctx
    line = format_regime_reason_line(ctx)
    if line:
        rr = payload.get("regime_reasoning")
        if isinstance(rr, list):
            payload["regime_reasoning"] = list(rr) + [line]
        elif isinstance(rr, str) and rr.strip():
            payload["regime_reasoning"] = [rr, line]
        else:
            payload["regime_reasoning"] = [line]
    return payload


def _regime_status_fallback_payload() -> dict[str, object]:
    return {
        "current_regime": "UNKNOWN",
        "confidence_score": 0.0,
        "regime_reasoning": ["Regime cache warming"],
        "recommendations": ["Operate conservatively until full regime payload is refreshed."],
        "decision_layer_only": True,
        "demo_simulation_only": True,
        "live_data_context": get_aggregate_context(),
    }


def _regime_status_cache_loop(interval_sec: int = 10) -> None:
    global _regime_status_cache, _regime_status_cache_written_at
    while True:
        try:
            payload = _compute_market_regime_status_payload()
            with _regime_status_cache_lock:
                _regime_status_cache = dict(payload)
                _regime_status_cache_written_at = datetime.now(timezone.utc).isoformat()
        except Exception:
            pass
        if _regime_status_updater_stop.wait(timeout=max(3, int(interval_sec))):
            break


def start_regime_status_cache_updater(interval_sec: int = 10) -> dict[str, object]:
    global _regime_status_updater_thread
    if _regime_status_updater_thread is not None and _regime_status_updater_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _regime_status_updater_stop.clear()
    _regime_status_updater_thread = threading.Thread(
        target=lambda: _regime_status_cache_loop(interval_sec=interval_sec),
        name="regime_status_cache_updater",
        daemon=True,
    )
    _regime_status_updater_thread.start()
    return {"started": True, "interval_sec": interval_sec}


def get_market_regime_status() -> dict[str, object]:
    global _regime_status_cache, _regime_status_cache_written_at
    with _regime_status_cache_lock:
        cached = dict(_regime_status_cache) if isinstance(_regime_status_cache, dict) else None
    if cached:
        try:
            live = _compute_market_regime_status_payload()
            cached_regime = str(cached.get("current_regime", "") or "")
            live_regime = str(live.get("current_regime", "") or "")
            # Freshness safeguard: if regime label diverges, refresh immediately.
            if cached_regime != live_regime:
                with _regime_status_cache_lock:
                    _regime_status_cache = dict(live)
                    _regime_status_cache_written_at = datetime.now(timezone.utc).isoformat()
                return live
            return cached
        except Exception:
            return cached
    return _regime_status_fallback_payload()


def get_debug_regime_parity() -> dict[str, object]:
    with _regime_status_cache_lock:
        cached = dict(_regime_status_cache) if isinstance(_regime_status_cache, dict) else None
        cache_written = _regime_status_cache_written_at

    fund = get_fund_status()
    factory = list(get_factory_strategies().get("strategies", []))
    paper = get_paper_status()
    auto = get_auto_loop_status()
    portfolio = get_portfolio_allocation()
    light_meta = build_meta_status(
        fund_status=fund,
        factory_strategies=factory,
        paper_status=paper,
        auto_status=auto,
        portfolio_brain=portfolio.get("brain"),
    )
    perf = get_performance_system()
    fund_alloc = get_fund_allocation_status()
    risk_full = _build_global_risk_full()
    strategies_perf = list(get_performance_strategies().get("strategies", []))

    fam_perf = regime_debug_engine._family_metric_avgs(factory, strategies_perf, "performance_score")
    mom = fam_perf.get("MOMENTUM")
    mr = fam_perf.get("MEAN_REVERSION")
    mom_edge = (mom - mr) if mom is not None and mr is not None else 0.0
    features = regime_debug_engine._compute_features(
        performance_system=perf,
        portfolio_allocation=portfolio,
        fund_allocation_status=fund_alloc,
        paper_status=paper,
        global_risk_assessment=risk_full,
        meta_status=light_meta,
    )
    scores = regime_debug_engine._score_regime_candidates(features, {"momentum_minus_mr": mom_edge})
    live_payload = _compute_market_regime_status_payload()
    comps = risk_full.get("components") or {}

    return {
        "cached_regime": None if not cached else cached.get("current_regime"),
        "cached_confidence": None if not cached else cached.get("confidence_score"),
        "live_recompute_regime": live_payload.get("current_regime"),
        "live_recompute_confidence": live_payload.get("confidence_score"),
        "score_breakdown": {
            "trending_score": round(float(scores.get("TRENDING", 0.0) or 0.0), 6),
            "ranging_score": round(float(scores.get("RANGING", 0.0) or 0.0), 6),
            "transitional_score": round(float(scores.get("TRANSITIONAL", 0.0) or 0.0), 6),
            "chaotic_score": round(float(scores.get("CHAOTIC", 0.0) or 0.0), 6),
        },
        "inputs": {
            "global_risk_score": features.get("global_risk_score"),
            "pipeline_risk": features.get("pipeline_risk"),
            "capital_risk": float(comps.get("capital_risk", 0.0) or 0.0),
            "drawdown_risk": features.get("drawdown_risk"),
            "runner_risk": features.get("runner_risk"),
            "stressful_component_count": features.get("stressful_component_count"),
        },
        "last_cache_write_timestamp": cache_written,
    }


def get_market_regime_recommendations() -> dict[str, object]:
    return build_regime_recommendations_response(get_market_regime_status())


def get_long_term_memory_bundle() -> dict[str, Any]:
    return {
        "regime_status": get_market_regime_status(),
        "global_risk_full": _build_global_risk_full(),
        "strategies_performance": list(get_performance_strategies().get("strategies", [])),
        "factory_strategies": list(get_factory_strategies().get("strategies", [])),
        "evolution_candidates": get_evolution_candidates(),
        "evolution_lineage": get_evolution_lineage(),
        "continuous_evolution_status": get_continuous_evolution_loop_status(),
        "auto_loop_status": get_auto_loop_status(),
    }


def get_long_term_memory_status() -> dict[str, object]:
    state = load_memory_state()
    factory = list(get_factory_strategies().get("strategies", []))
    return build_memory_status_payload(state, factory)


def get_memory_strategy_view() -> dict[str, object]:
    state = load_memory_state()
    factory = list(get_factory_strategies().get("strategies", []))
    return {
        "strategies": aggregate_strategy_memory(state, factory),
        "decision_layer_only": True,
        "learning_memory_only": True,
    }


def get_memory_family_view() -> dict[str, object]:
    state = load_memory_state()
    factory = list(get_factory_strategies().get("strategies", []))
    strat = aggregate_strategy_memory(state, factory)
    return {
        "families": aggregate_family_memory(strat),
        "decision_layer_only": True,
        "learning_memory_only": True,
    }


def get_memory_regime_view() -> dict[str, object]:
    state = load_memory_state()
    factory = list(get_factory_strategies().get("strategies", []))
    strat = aggregate_strategy_memory(state, factory)
    return {
        "regimes": aggregate_regime_memory(state, strat),
        "decision_layer_only": True,
        "learning_memory_only": True,
    }


def post_memory_update() -> dict[str, object]:
    return run_memory_update_cycle(get_long_term_memory_bundle())


def get_capital_status() -> dict[str, object]:
    with get_connection() as conn:
        account = fetch_account(conn)
    portfolio = get_portfolio_allocation()
    fund = get_fund_status()
    return build_capital_status(
        account=account,
        portfolio_allocation=portfolio,
        fund_status=fund,
    )


def _build_global_risk_full() -> dict[str, Any]:
    return build_global_risk_assessment(
        portfolio_allocation=get_portfolio_allocation(),
        fund_allocation_status=get_fund_allocation_status(),
        performance_system=get_performance_system(),
        multi_runner_status=get_multi_runner_status(),
        recovery_status=get_recovery_engine_status(),
        capital_status=get_capital_status(),
        review_status=get_review_status(),
        paper_status=get_paper_status(),
        factory_candidate_count=int(get_factory_strategies().get("count", 0)),
    )


def _build_global_risk_full_light() -> dict[str, Any]:
    """Global risk using runner/paper log performance only (no operator console / meta recursion)."""
    return build_global_risk_assessment(
        portfolio_allocation=get_portfolio_allocation(),
        fund_allocation_status=get_fund_allocation_status(),
        performance_system=_get_performance_system_light(),
        multi_runner_status=get_multi_runner_status(),
        recovery_status=get_recovery_engine_status(),
        capital_status=get_capital_status(),
        review_status=get_review_status(),
        paper_status=get_paper_status(),
        factory_candidate_count=int(get_factory_strategies().get("count", 0)),
    )


def _compute_global_risk_status_payload() -> dict[str, object]:
    out = dict(build_global_risk_status_payload(_build_global_risk_full()))
    out["live_data_context"] = get_aggregate_context()
    return out


def _risk_status_fallback_payload() -> dict[str, object]:
    return {
        "global_risk_score": 1.0,
        "risk_level": "CRITICAL",
        "risk_action": "PAUSE_NEW_ENTRIES",
        "components": {},
        "risk_reasoning": ["Risk cache warming"],
        "decision_layer_only": True,
        "demo_simulation_only": True,
        "live_data_context": get_aggregate_context(),
    }


def _risk_status_cache_loop(interval_sec: int = 10) -> None:
    global _risk_status_cache
    while True:
        try:
            payload = _compute_global_risk_status_payload()
            with _risk_status_cache_lock:
                _risk_status_cache = dict(payload)
        except Exception:
            pass
        if _risk_status_updater_stop.wait(timeout=max(3, int(interval_sec))):
            break


def start_risk_status_cache_updater(interval_sec: int = 10) -> dict[str, object]:
    global _risk_status_updater_thread
    if _risk_status_updater_thread is not None and _risk_status_updater_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _risk_status_updater_stop.clear()
    _risk_status_updater_thread = threading.Thread(
        target=lambda: _risk_status_cache_loop(interval_sec=interval_sec),
        name="risk_status_cache_updater",
        daemon=True,
    )
    _risk_status_updater_thread.start()
    return {"started": True, "interval_sec": interval_sec}


def get_global_risk_status() -> dict[str, object]:
    with _risk_status_cache_lock:
        cached = dict(_risk_status_cache) if isinstance(_risk_status_cache, dict) else None
    if cached:
        return cached
    return _risk_status_fallback_payload()


def get_global_risk_alerts() -> dict[str, object]:
    return build_global_risk_alerts_payload(_build_global_risk_full())


def _collect_alerts_list() -> list[dict[str, object]]:
    meta = get_legacy_meta_status()
    capital = get_capital_status()
    fund = get_fund_status()
    report = get_report_summary()
    portfolio = get_portfolio_allocation()
    paper = get_paper_status()
    review = get_review_status()
    demo = get_demo_status()
    executor = get_executor_status()
    runner = get_runner_status()
    runner_jobs = get_runner_jobs(limit=50)
    auto = get_auto_loop_status()
    operator = get_operator_console_status()

    ex_counts = executor.get("counts", {}) or {}
    ex_ready = int(ex_counts.get("EXECUTOR_READY", 0))
    jobs = runner_jobs.get("jobs", []) or []
    elig = sum(1 for j in jobs if j.get("eligible"))
    runner_stale = False
    with get_connection() as conn:
        last_nonempty = get_alert_engine_state(conn, "runner_last_nonempty_jobs_at")
        if elig > 0:
            set_alert_engine_state(
                conn,
                "runner_last_nonempty_jobs_at",
                datetime.now(timezone.utc).isoformat(),
            )
        elif ex_ready > 0 and last_nonempty:
            try:
                raw_ts = last_nonempty.replace("Z", "+00:00")
                t = datetime.fromisoformat(raw_ts)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - t).total_seconds()
                if age > 900:
                    runner_stale = True
            except ValueError:
                pass
        ack_ids = fetch_acknowledged_alert_ids(conn)

    raw = build_alerts(
        meta_status=meta,
        capital_status=capital,
        fund_status=fund,
        report_summary=report,
        portfolio_allocation=portfolio,
        paper_status=paper,
        review_status=review,
        demo_status=demo,
        executor_status=executor,
        runner_status=runner,
        runner_jobs=runner_jobs,
        auto_status=auto,
        operator_console=operator,
        runner_stale_no_jobs=runner_stale,
    )
    return apply_acknowledgements(raw, ack_ids)


def get_alerts() -> dict[str, object]:
    final = _collect_alerts_list()
    active_n = sum(1 for a in final if a.get("active", True))
    return {"count": active_n, "alerts": final}


def get_alerts_summary() -> dict[str, object]:
    return build_alerts_summary(_collect_alerts_list())


def acknowledge_alert(alert_id: str) -> dict[str, object]:
    with get_connection() as conn:
        acknowledge_alert_id(conn, alert_id)
    return {"ok": True, "alert_id": alert_id}


def _recovery_handle_auto_loop() -> dict[str, object]:
    cleared = _get_auto_loop_engine().clear_last_error()
    once = run_auto_cycle_once()
    return {
        "ok": bool(once.get("ok", True)),
        "steps": [{"op": "clear_last_error", **cleared}, {"op": "run_auto_cycle_once", **once}],
    }


def _recovery_handle_runner_failed() -> dict[str, object]:
    with get_connection() as conn:
        strategies = fetch_factory_strategies(conn)
    failed = [s for s in strategies if str(s.get("runner_status")) == RUNNER_FAILED][:2]
    if not failed:
        return {"ok": True, "steps": [], "message": "no_failed_runner_rows"}
    steps: list[dict[str, object]] = []
    for s in failed:
        sid = str(s["strategy_id"])
        paused = pause_executor_item(sid, note="auto_recovery: pause before runner retry")
        steps.append({"op": "pause_executor", "strategy_id": sid, **paused})
        if not paused.get("ok"):
            continue
        rp = float(s.get("runner_priority", 0.0) or 0.0)
        with get_connection() as conn:
            update_factory_strategy_runner(
                conn,
                sid,
                RUNNER_PENDING,
                "auto_recovery: reset failed job",
                runner_id="",
                runner_started_at=None,
                runner_completed_at=None,
                runner_priority=rp if rp > 0 else None,
            )
        steps.append({"op": "reset_runner_pending", "strategy_id": sid, "ok": True})
        prep = prepare_executor_item(sid, note="auto_recovery: re-prepare executor")
        steps.append({"op": "prepare_executor", **prep})
        if not prep.get("ok"):
            continue
        ack = ack_runner_job(sid, runner_id="auto_recovery", note="auto_recovery: retry ack")
        steps.append({"op": "ack_runner", **ack})
        if ack.get("ok"):
            st = start_runner_job(sid, note="auto_recovery: retry start")
            steps.append({"op": "start_runner", **st})
    return {"ok": True, "steps": steps}


def _recovery_handle_stuck_runners() -> dict[str, object]:
    jobs = list(get_runner_jobs(limit=200).get("jobs", []) or [])
    now = datetime.now(timezone.utc)
    stuck_ids: list[str] = []
    for j in jobs:
        if str(j.get("runner_status", "")) != RUNNER_ACTIVE:
            continue
        ts = j.get("runner_started_at")
        if not ts:
            continue
        try:
            raw = str(ts).replace("Z", "+00:00")
            t = datetime.fromisoformat(raw)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if (now - t).total_seconds() >= 3600:
                stuck_ids.append(str(j["strategy_id"]))
        except ValueError:
            continue
    steps: list[dict[str, object]] = []
    for sid in stuck_ids[:2]:
        pr = pause_runner_job(sid, note="auto_recovery: unstick long-running runner")
        steps.append({"op": "pause_runner", "strategy_id": sid, **pr})
        if pr.get("ok"):
            st = start_runner_job(sid, note="auto_recovery: restart runner")
            steps.append({"op": "start_runner", "strategy_id": sid, **st})
    return {"ok": True, "steps": steps}


def _recovery_handle_no_runner_jobs() -> dict[str, object]:
    steps: list[dict[str, object]] = []
    demo_c = get_demo_candidates(limit=20).get("candidates", [])
    elig_demo = next((x for x in demo_c if x.get("eligible")), None)
    if elig_demo:
        sid = str(elig_demo["strategy_id"])
        q = queue_demo_candidate(sid, note="auto_recovery: queue demo for runner flow")
        steps.append({"op": "queue_demo", "strategy_id": sid, **q})
        if q.get("ok"):
            ad = assign_demo_candidate(
                sid,
                assignee="auto_recovery",
                note="auto_recovery: assign demo slot",
            )
            steps.append({"op": "assign_demo", "strategy_id": sid, **ad})
    ex_c = get_executor_candidates(limit=20).get("candidates", [])
    ex_elig = next((x for x in ex_c if x.get("eligible")), None)
    if ex_elig:
        sid = str(ex_elig["strategy_id"])
        pr = prepare_executor_item(sid, note="auto_recovery: prepare executor")
        steps.append({"op": "prepare_executor", "strategy_id": sid, **pr})
    return {"ok": True, "steps": steps}


def _recovery_handle_pipeline_stalled() -> dict[str, object]:
    gen = generate_factory_strategies(count=8, seed=991)
    ev = evolve_factory_strategies(
        top_n=3,
        children_per_parent=1,
        crossover_rate=0.4,
        seed=992,
    )
    return {
        "ok": True,
        "steps": [{"op": "factory_generate", **gen}, {"op": "factory_evolve", **ev}],
    }


def _recovery_handle_no_paper_success() -> dict[str, object]:
    dep = deploy_paper_bots(max_bots=min(5, MAX_PAPER_BOTS))
    fb = apply_paper_feedback()
    return {"ok": True, "steps": [{"op": "deploy_paper", **dep}, {"op": "apply_feedback", **fb}]}


def _recovery_handle_capital_full() -> dict[str, object]:
    with get_connection() as conn:
        set_alert_engine_state(
            conn,
            "auto_recovery_capital_full_noted",
            datetime.now(timezone.utc).isoformat(),
        )
    return {
        "ok": True,
        "steps": [
            {
                "op": "note_only",
                "message": "No capital deployment; recorded observation timestamp only.",
            }
        ],
    }


def _build_recovery_handlers() -> dict[str, object]:
    from . import auto_recovery_engine as are

    return {
        "get_active_alerts": lambda: [a for a in _collect_alerts_list() if a.get("active", True)],
        "get_runner_jobs": lambda: list(get_runner_jobs(limit=200).get("jobs", []) or []),
        f"recover_{are.AUTO_LOOP_ERROR}": _recovery_handle_auto_loop,
        f"recover_{are.RUNNER_FAILED}": _recovery_handle_runner_failed,
        f"recover_{are.RUNNER_STUCK}": _recovery_handle_stuck_runners,
        f"recover_{are.NO_RUNNER_JOBS}": _recovery_handle_no_runner_jobs,
        f"recover_{are.PIPELINE_STALLED}": _recovery_handle_pipeline_stalled,
        f"recover_{are.NO_PAPER_SUCCESS}": _recovery_handle_no_paper_success,
        f"recover_{are.CAPITAL_FULL}": _recovery_handle_capital_full,
    }


def get_recovery_engine_status() -> dict[str, object]:
    with get_connection() as conn:
        return load_recovery_status(conn)


def run_recovery_engine() -> dict[str, object]:
    handlers = _build_recovery_handlers()
    with get_connection() as conn:
        persist_running(conn)
    result: dict[str, object] = {}
    ok = True
    err = ""
    try:
        result = execute_recovery(handlers)
    except Exception as exc:  # pragma: no cover - defensive
        ok = False
        err = str(exc)
        result = {
            "ok": False,
            "error": err,
            "triggers": [],
            "actions": [],
            "last_action": "exception",
            "last_result": err,
        }
    la = str(result.get("last_action", "none"))
    lr = str(result.get("last_result", err)) if ok else err
    with get_connection() as conn:
        persist_finished(
            conn,
            success=ok,
            last_action=la,
            last_result=lr or "failed",
            detail=result if ok else {"error": err},
        )
    out = get_recovery_engine_status()
    out["triggers_fired"] = result.get("triggers", [])
    out["actions_log"] = result.get("actions", [])
    out["run_ok"] = ok and bool(result.get("ok", True))
    return out


def _collect_performance_inputs() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    str | None,
]:
    with get_connection() as conn:
        runs = fetch_performance_run_log(conn)
        factory = fetch_factory_strategies(conn)
        hist = get_alert_engine_state(conn, "auto_recovery_history")
    paper = list(get_paper_status().get("running_paper_bots", []) or [])
    operator = get_operator_console_status()
    pipeline = operator.get("pipeline", {}) or {}
    return runs, factory, paper, pipeline, hist


def _collect_performance_inputs_light() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    str | None,
]:
    """Same as _collect_performance_inputs but without operator console (avoids meta↔operator recursion)."""
    with get_connection() as conn:
        runs = fetch_performance_run_log(conn)
        factory = fetch_factory_strategies(conn)
        hist = get_alert_engine_state(conn, "auto_recovery_history")
    paper = list(get_paper_status().get("running_paper_bots", []) or [])
    pipeline = {
        "total_candidates": 0,
        "paper_success": 0,
        "demo_queued": 0,
        "live_safe_ready": 0,
        "paper_running": 0,
    }
    return runs, factory, paper, pipeline, hist


def _get_performance_system_light() -> dict[str, object]:
    runs, factory, paper, pipeline, hist = _collect_performance_inputs_light()
    _ = paper
    return build_system_performance(
        run_rows=runs,
        operator_pipeline=pipeline,
        recovery_history_json=hist,
        factory_strategies=factory,
    )


def get_performance_strategies() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    full = build_strategies_performance(factory, runs, paper)
    slim = [
        {
            "strategy_id": x["strategy_id"],
            "total_runs": x["total_runs"],
            "success_rate": x["success_rate"],
            "avg_duration": x["avg_duration"],
            "performance_score": x["performance_score"],
            "last_run": x["last_run"],
        }
        for x in full
    ]
    return {"count": len(slim), "strategies": slim}


def get_performance_strategies_light() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs_light()
    full = build_strategies_performance(factory, runs, paper)
    slim = [
        {
            "strategy_id": x["strategy_id"],
            "total_runs": x["total_runs"],
            "success_rate": x["success_rate"],
            "avg_duration": x["avg_duration"],
            "performance_score": x["performance_score"],
            "last_run": x["last_run"],
        }
        for x in full
    ]
    return {"count": len(slim), "strategies": slim}


def get_performance_system() -> dict[str, object]:
    runs, factory, paper, pipeline, hist = _collect_performance_inputs()
    _ = paper
    return build_system_performance(
        run_rows=runs,
        operator_pipeline=pipeline,
        recovery_history_json=hist,
        factory_strategies=factory,
    )


def get_performance_top() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    full = build_strategies_performance(factory, runs, paper)
    top = build_top_strategies(full, 10)
    return {"count": len(top), "strategies": top}


def bump_review_priority_smart(strategy_id: str, delta: float) -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        rs = str(s.get("review_status", REVIEW_PENDING))
        new_p = min(100.0, float(s.get("review_priority", 0.0)) + float(delta))
        update_factory_strategy_review(
            conn=conn,
            strategy_id=strategy_id,
            review_status=rs,
            review_note=str(s.get("review_note", "")),
            reviewer=str(s.get("reviewer", "")),
            reviewed_at=s.get("reviewed_at"),
            review_priority=new_p,
        )
    return {"ok": True, "strategy_id": strategy_id, "review_priority": new_p}


def _fetch_strategy_snapshot(strategy_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        return fetch_factory_strategy_by_id(conn, strategy_id)


def get_promotion_candidates() -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf_rows = build_strategies_performance(factory, runs, paper)
    with get_connection() as conn:
        hist = fetch_promotion_history(conn, limit=25)
    return build_promotion_candidates_response(factory, perf_rows, recent_history=hist)


def run_smart_promotion_engine() -> dict[str, object]:
    promoted: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf_rows = build_strategies_performance(factory, runs, paper)
    perf_by_id = {str(r["strategy_id"]): r for r in perf_rows}
    review_pending_states = {REVIEW_PENDING, REVIEW_UNDER, REVIEW_NEEDS_TESTING}

    for s in factory:
        sid = str(s.get("strategy_id", ""))
        if not sid:
            continue
        row = perf_by_id.get(sid) or zero_perf_row(sid)
        tier = highest_tier_for_row(row)
        if tier is None:
            continue
        rank = TIER_RANK[tier]
        s_live: dict[str, Any] = dict(s)
        actions: list[str] = []
        partial_skip: list[str] = []

        def log_row(action_taken: str, detail: str = "") -> None:
            with get_connection() as conn:
                insert_promotion_history(
                    conn,
                    strategy_id=sid,
                    target_tier=tier,
                    performance_score=float(row["performance_score"]),
                    success_rate=float(row["success_rate"]),
                    stability_score=stability_score_for_row(row),
                    activity_score=activity_score_for_row(row),
                    action_taken=action_taken,
                    detail=detail,
                )

        if rank == TIER_RANK[PROMOTE_TO_REVIEW]:
            bump = min(25.0, 5.0 + float(row["performance_score"]) * 15.0)
            br = bump_review_priority_smart(sid, bump)
            if br.get("ok"):
                actions.append("bump_review_priority")
                log_row("bump_review_priority")
                promoted.append(
                    {
                        "strategy_id": sid,
                        "target_tier": tier,
                        "actions": actions,
                        "performance_score": row["performance_score"],
                        "success_rate": row["success_rate"],
                    }
                )
            else:
                skipped.append(
                    {
                        "strategy_id": sid,
                        "target_tier": tier,
                        "reason": str(br.get("error", "bump_failed")),
                    }
                )
            continue

        if rank >= TIER_RANK[PROMOTE_TO_DEMO]:
            rstat = str(s_live.get("review_status", ""))
            if rstat in review_pending_states:
                ar = approve_review_candidate(sid, reviewer="smart_promotion_engine")
                if ar.get("ok"):
                    actions.append("approve_review")
                    log_row("approve_review")
                    snap = _fetch_strategy_snapshot(sid)
                    if snap:
                        s_live = dict(snap)
                else:
                    partial_skip.append(f"approve_review:{ar.get('error')}")

        if rank >= TIER_RANK[PROMOTE_TO_EXECUTOR]:
            ex_ok, ex_reason = is_executor_eligible(s_live)
            ex_st = str(s_live.get("executor_status", ""))
            if ex_st in {EXECUTOR_READY, EXECUTOR_RUNNING, EXECUTOR_PAUSED}:
                actions.append("executor_already_advanced")
            elif ex_ok:
                pr = prepare_executor_item(
                    sid,
                    target="demo_runner",
                    note="smart_promotion_engine: prepare only (no execution).",
                )
                if pr.get("ok"):
                    actions.append("prepare_executor")
                    log_row("prepare_executor")
                else:
                    partial_skip.append(f"prepare_executor:{pr.get('error')}")
            else:
                partial_skip.append(f"executor_not_eligible:{ex_reason}")

        if rank >= TIER_RANK[PROMOTE_TO_RUNNER]:
            actions.append("runner_recommended_decision_only")
            log_row("runner_recommended_decision_only", "no_ack_no_start")

        if actions:
            promoted.append(
                {
                    "strategy_id": sid,
                    "target_tier": tier,
                    "actions": actions,
                    "performance_score": row["performance_score"],
                    "success_rate": row["success_rate"],
                    "partial_skips": partial_skip,
                }
            )
        else:
            skipped.append(
                {
                    "strategy_id": sid,
                    "target_tier": tier,
                    "reason": ";".join(partial_skip) if partial_skip else "no_action_needed",
                }
            )

    return {"promoted": promoted, "skipped": skipped}


def run_bootstrap_demo_flow(max_queue: int = 50) -> dict[str, object]:
    runs, factory, paper, _, _ = _collect_performance_inputs()
    perf_rows = build_strategies_performance(factory, runs, paper)
    perf_by_id = {str(r["strategy_id"]): r for r in perf_rows}
    queued: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    # Prioritize newest generations first for bootstrap sequencing.
    ordered = sorted(factory, key=lambda s: int(s.get("generation", 0) or 0), reverse=True)
    note = build_bootstrap_demo_note()
    now_iso = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        for s in ordered:
            if len(queued) >= int(max_queue):
                break
            sid = str(s.get("strategy_id", ""))
            if not sid:
                continue
            row = perf_by_id.get(sid) or zero_perf_row(sid)
            ok, reason = should_enter_bootstrap(s, row)
            if not ok:
                skipped.append({"strategy_id": sid, "reason": reason})
                continue

            demo_priority = float(s.get("demo_priority", 0.0) or 0.0)
            if demo_priority <= 0:
                demo_priority = float(s.get("review_priority", 0.0) or 0.0)
            if demo_priority <= 0:
                demo_priority = 1.0

            update_factory_strategy_demo(
                conn=conn,
                strategy_id=sid,
                demo_status=DEMO_QUEUE,
                demo_note=note,
                demo_assignee=BOOTSTRAP_ASSIGNEE,
                demo_assigned_at=now_iso,
                demo_priority=demo_priority,
            )
            queued.append(
                {
                    "strategy_id": sid,
                    "generation": int(s.get("generation", 0) or 0),
                    "risk_allocation_pct": 0.1,
                    "max_paper_trades": 20,
                    "demo_only": True,
                }
            )

    return {
        "queued_count": len(queued),
        "queued": queued,
        "skipped_count": len(skipped),
        "demo_only": True,
        "safe_allocation": {"risk_allocation_pct": 0.1, "max_paper_trades": 20},
    }


def get_report_summary() -> dict[str, object]:
    meta = get_legacy_meta_status()
    portfolio = get_portfolio_allocation()
    capital = get_capital_status()
    fund = get_fund_status()
    live = get_live_safe_status()
    factory_top = get_factory_top(limit=5)
    paper = get_paper_status()
    feedback = get_paper_feedback_preview()
    total = int(get_factory_strategies().get("count", 0))
    return build_report_summary(
        meta_status=meta,
        portfolio_allocation=portfolio,
        capital_status=capital,
        fund_status=fund,
        live_safe_status=live,
        factory_top=factory_top,
        paper_status=paper,
        feedback_preview=feedback,
        total_strategies=total,
    )


def get_report_daily() -> dict[str, object]:
    return build_report_daily(get_report_summary())


def get_review_candidates(limit: int = 25) -> dict[str, object]:
    with get_connection() as conn:
        strategies = fetch_factory_strategies(conn)
        paper = fetch_paper_bots(conn)
    feedback = get_paper_feedback_preview().get("results", [])
    payload = build_review_candidates(
        strategies=strategies,
        paper_bots=paper,
        feedback_results=feedback,
        limit=limit,
    )
    return payload


def _set_review_status(
    strategy_id: str,
    review_status: str,
    review_note: str = "",
    reviewer: str = "operator",
) -> dict[str, object]:
    with get_connection() as conn:
        strategy = fetch_factory_strategy_by_id(conn, strategy_id)
        if strategy is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        current_priority = float(strategy.get("review_priority", 0.0))
        update_factory_strategy_review(
            conn=conn,
            strategy_id=strategy_id,
            review_status=review_status,
            review_note=review_note,
            reviewer=reviewer,
            reviewed_at=datetime.now(timezone.utc).isoformat(),
            review_priority=current_priority,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "review_status": updated.get("review_status", review_status),
        "review_note": updated.get("review_note", review_note),
        "reviewer": updated.get("reviewer", reviewer),
        "reviewed_at": updated.get("reviewed_at"),
        "note": "Review desk update only. No deployment or execution.",
    }


def approve_review_candidate(strategy_id: str, reviewer: str = "operator") -> dict[str, object]:
    return _set_review_status(
        strategy_id=strategy_id,
        review_status=REVIEW_APPROVED,
        review_note="Approved for demo-review progression.",
        reviewer=reviewer,
    )


def reject_review_candidate(
    strategy_id: str, note: str, reviewer: str = "operator"
) -> dict[str, object]:
    return _set_review_status(
        strategy_id=strategy_id,
        review_status=REVIEW_REJECTED,
        review_note=note or "Rejected by human review.",
        reviewer=reviewer,
    )


def flag_review_candidate(
    strategy_id: str, note: str = "", reviewer: str = "operator"
) -> dict[str, object]:
    return _set_review_status(
        strategy_id=strategy_id,
        review_status=REVIEW_NEEDS_TESTING,
        review_note=note or "Needs more testing before progression.",
        reviewer=reviewer,
    )


def start_review_candidate(
    strategy_id: str, note: str = "", reviewer: str = "operator"
) -> dict[str, object]:
    return _set_review_status(
        strategy_id=strategy_id,
        review_status=REVIEW_UNDER,
        review_note=note or "Under active human review.",
        reviewer=reviewer,
    )


def get_review_status() -> dict[str, object]:
    candidates = get_review_candidates(limit=200).get("candidates", [])
    return build_review_status_payload(candidates)


def get_demo_candidates(limit: int = 25) -> dict[str, object]:
    with get_connection() as conn:
        strategies = fetch_factory_strategies(conn)
        paper = fetch_paper_bots(conn)
    feedback = get_paper_feedback_preview().get("results", [])
    return build_demo_candidates(
        strategies=strategies,
        paper_bots=paper,
        feedback_results=feedback,
        limit=limit,
    )


def queue_demo_candidate(
    strategy_id: str, note: str = "Queued for controlled demo deployment review."
) -> dict[str, object]:
    feedback_by_id = {
        str(x.get("strategy_id")): x for x in get_paper_feedback_preview().get("results", [])
    }
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        promotion = float(feedback_by_id.get(strategy_id, {}).get("promotion_score", 0.0))
        eligible, reason = is_demo_eligible(s, promotion)
        if not eligible:
            return {
                "ok": False,
                "error": "not_demo_eligible",
                "strategy_id": strategy_id,
                "reason": reason,
            }
        current_priority = float(s.get("demo_priority", 0.0))
        if current_priority <= 0:
            current_priority = float(s.get("review_priority", 0.0))
        update_factory_strategy_demo(
            conn=conn,
            strategy_id=strategy_id,
            demo_status=DEMO_QUEUE,
            demo_note=note,
            demo_assignee=str(s.get("demo_assignee", "")),
            demo_assigned_at=s.get("demo_assigned_at"),
            demo_priority=current_priority,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "demo_status": updated.get("demo_status", DEMO_QUEUE),
        "demo_note": updated.get("demo_note", note),
        "queue_only": True,
        "note": "Placed in demo queue only. No broker execution.",
    }


def assign_demo_candidate(
    strategy_id: str,
    assignee: str,
    note: str = "Assigned for demo slot preparation.",
) -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        if str(s.get("demo_status", "")) not in {DEMO_QUEUE, DEMO_ASSIGNED}:
            return {
                "ok": False,
                "error": "invalid_demo_state",
                "strategy_id": strategy_id,
                "current_demo_status": s.get("demo_status", ""),
            }
        update_factory_strategy_demo(
            conn=conn,
            strategy_id=strategy_id,
            demo_status=DEMO_ASSIGNED,
            demo_note=note,
            demo_assignee=assignee,
            demo_assigned_at=datetime.now(timezone.utc).isoformat(),
            demo_priority=float(s.get("demo_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "demo_status": updated.get("demo_status", DEMO_ASSIGNED),
        "demo_assignee": updated.get("demo_assignee", assignee),
        "demo_assigned_at": updated.get("demo_assigned_at"),
        "queue_only": True,
    }


def pause_demo_candidate(
    strategy_id: str,
    note: str = "Paused in demo workflow.",
) -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        update_factory_strategy_demo(
            conn=conn,
            strategy_id=strategy_id,
            demo_status=DEMO_PAUSED,
            demo_note=note,
            demo_assignee=str(s.get("demo_assignee", "")),
            demo_assigned_at=s.get("demo_assigned_at"),
            demo_priority=float(s.get("demo_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "demo_status": updated.get("demo_status", DEMO_PAUSED),
        "demo_note": updated.get("demo_note", note),
        "queue_only": True,
    }


def reject_demo_candidate(
    strategy_id: str,
    note: str = "Rejected from demo queue.",
) -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        update_factory_strategy_demo(
            conn=conn,
            strategy_id=strategy_id,
            demo_status=DEMO_REJECTED,
            demo_note=note,
            demo_assignee=str(s.get("demo_assignee", "")),
            demo_assigned_at=s.get("demo_assigned_at"),
            demo_priority=float(s.get("demo_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "demo_status": updated.get("demo_status", DEMO_REJECTED),
        "demo_note": updated.get("demo_note", note),
        "queue_only": True,
    }


def get_demo_status() -> dict[str, object]:
    candidates = get_demo_candidates(limit=200).get("candidates", [])
    return build_demo_status(candidates)


def get_operator_console_status() -> dict[str, object]:
    with _operator_status_cache_lock:
        cached = dict(_operator_status_cache) if isinstance(_operator_status_cache, dict) else None
    if cached:
        return cached
    return _operator_status_fallback_payload()


def _compute_operator_console_status_payload() -> dict[str, object]:
    meta = get_legacy_meta_status()
    auto = get_auto_loop_status()
    report = get_report_summary()
    factory = get_factory_strategies()
    paper = get_paper_status()
    live = get_live_safe_status()
    review = get_review_status()
    demo = get_demo_status()
    capital = get_capital_status()
    portfolio = get_portfolio_allocation()
    return build_operator_console_status(
        meta_status=meta,
        auto_status=auto,
        report_summary=report,
        factory_strategies=factory,
        paper_status=paper,
        live_safe_status=live,
        review_status=review,
        demo_status=demo,
        capital_status=capital,
        portfolio_allocation=portfolio,
    )


def _operator_status_fallback_payload() -> dict[str, object]:
    return {
        "system_health": "DEGRADED",
        "pipeline": {
            "total_candidates": 0,
            "paper_running": 0,
            "review_pending": 0,
            "demo_running": 0,
        },
        "capital": {
            "equity": 0.0,
            "allocated": 0.0,
            "free": 0.0,
            "risk_usage": 0.0,
            "growth_rate": 0.0,
        },
        "risk_flags": ["Operator status cache warming"],
        "portfolio": {
            "allocation_count": 0,
            "total_allocated_percent": 0.0,
            "brain_rotate_in": [],
            "brain_rotate_out": [],
            "brain_top_priorities": [],
        },
        "loops_completed": 0,
        "last_cycle_at": None,
        "decision_layer_only": True,
    }


def _operator_status_cache_loop(interval_sec: int = 10) -> None:
    global _operator_status_cache
    while True:
        try:
            payload = _compute_operator_console_status_payload()
            with _operator_status_cache_lock:
                _operator_status_cache = dict(payload)
        except Exception:
            pass
        if _operator_status_updater_stop.wait(timeout=max(3, int(interval_sec))):
            break


def start_operator_status_cache_updater(interval_sec: int = 10) -> dict[str, object]:
    global _operator_status_updater_thread
    if _operator_status_updater_thread is not None and _operator_status_updater_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _operator_status_updater_stop.clear()
    _operator_status_updater_thread = threading.Thread(
        target=lambda: _operator_status_cache_loop(interval_sec=interval_sec),
        name="operator_status_cache_updater",
        daemon=True,
    )
    _operator_status_updater_thread.start()
    return {"started": True, "interval_sec": interval_sec}


def get_executor_candidates(limit: int = 25) -> dict[str, object]:
    with get_connection() as conn:
        strategies = fetch_factory_strategies(conn)
    feedback = get_paper_feedback_preview().get("results", [])
    return build_executor_candidates(strategies=strategies, feedback_results=feedback, limit=limit)


def prepare_executor_item(
    strategy_id: str,
    target: str = "demo_runner",
    note: str = "Prepared for controlled demo executor integration.",
) -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        eligible, reason = is_executor_eligible(s)
        if not eligible:
            return {"ok": False, "error": "not_executor_eligible", "strategy_id": strategy_id, "reason": reason}
        priority = float(s.get("executor_priority", 0.0)) or float(s.get("demo_priority", 0.0))
        update_factory_strategy_executor(
            conn=conn,
            strategy_id=strategy_id,
            executor_status=EXECUTOR_READY,
            executor_note=note,
            executor_target=target,
            executor_assigned_at=datetime.now(timezone.utc).isoformat(),
            executor_priority=priority,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "executor_status": updated.get("executor_status", EXECUTOR_READY),
        "executor_target": updated.get("executor_target", target),
        "adapter_only": True,
        "note": "Prepared only. No broker execution.",
    }


def start_executor_item(strategy_id: str, note: str = "Executor marked as running (simulated).") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        if str(s.get("executor_status", "")) not in {EXECUTOR_READY, EXECUTOR_PAUSED, EXECUTOR_RUNNING}:
            return {
                "ok": False,
                "error": "invalid_executor_state",
                "strategy_id": strategy_id,
                "current_executor_status": s.get("executor_status", ""),
            }
        update_factory_strategy_executor(
            conn=conn,
            strategy_id=strategy_id,
            executor_status=EXECUTOR_RUNNING,
            executor_note=note,
            executor_target=str(s.get("executor_target", "demo_runner")),
            executor_assigned_at=s.get("executor_assigned_at"),
            executor_priority=float(s.get("executor_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "executor_status": updated.get("executor_status", EXECUTOR_RUNNING), "adapter_only": True}


def pause_executor_item(strategy_id: str, note: str = "Executor paused.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        update_factory_strategy_executor(
            conn=conn,
            strategy_id=strategy_id,
            executor_status=EXECUTOR_PAUSED,
            executor_note=note,
            executor_target=str(s.get("executor_target", "")),
            executor_assigned_at=s.get("executor_assigned_at"),
            executor_priority=float(s.get("executor_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "executor_status": updated.get("executor_status", EXECUTOR_PAUSED), "adapter_only": True}


def stop_executor_item(strategy_id: str, note: str = "Executor stopped.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        update_factory_strategy_executor(
            conn=conn,
            strategy_id=strategy_id,
            executor_status=EXECUTOR_STOPPED,
            executor_note=note,
            executor_target=str(s.get("executor_target", "")),
            executor_assigned_at=s.get("executor_assigned_at"),
            executor_priority=float(s.get("executor_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "executor_status": updated.get("executor_status", EXECUTOR_STOPPED), "adapter_only": True}


def get_executor_status() -> dict[str, object]:
    candidates = get_executor_candidates(limit=200).get("candidates", [])
    return build_executor_status(candidates)


def get_runner_jobs(limit: int = 25) -> dict[str, object]:
    cap = min(max(int(limit) * 25, 500), 3000)
    with get_connection() as conn:
        strategies = fetch_factory_strategies_for_runner_pipeline(conn, limit=cap)
        sids = [str(s["strategy_id"]) for s in strategies]
        items = fetch_paper_bots_for_strategies(conn, sids) if sids else []
    feedback = _build_feedback_payload(items)["results"]
    return build_runner_jobs(strategies=strategies, feedback_results=feedback, limit=limit)


def ack_runner_job(strategy_id: str, runner_id: str, note: str = "Job acknowledged by runner.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        eligible, reason = is_runner_eligible(s)
        current = str(s.get("runner_status", ""))
        if not eligible and current not in {RUNNER_ACKNOWLEDGED, RUNNER_ACTIVE, RUNNER_PAUSED}:
            return {"ok": False, "error": "not_runner_eligible", "strategy_id": strategy_id, "reason": reason}
        priority = float(s.get("runner_priority", 0.0)) or float(s.get("executor_priority", 0.0))
        update_factory_strategy_runner(
            conn=conn,
            strategy_id=strategy_id,
            runner_status=RUNNER_ACKNOWLEDGED,
            runner_note=note,
            runner_id=runner_id,
            runner_started_at=s.get("runner_started_at"),
            runner_completed_at=s.get("runner_completed_at"),
            runner_priority=priority,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "runner_status": updated.get("runner_status", RUNNER_ACKNOWLEDGED), "runner_id": updated.get("runner_id", runner_id), "bridge_only": True}


def start_runner_job(strategy_id: str, note: str = "Runner job active.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        if str(s.get("runner_status", "")) not in {RUNNER_ACKNOWLEDGED, RUNNER_PAUSED, RUNNER_ACTIVE}:
            return {"ok": False, "error": "invalid_runner_state", "strategy_id": strategy_id, "current_runner_status": s.get("runner_status", "")}
        update_factory_strategy_runner(
            conn=conn,
            strategy_id=strategy_id,
            runner_status=RUNNER_ACTIVE,
            runner_note=note,
            runner_id=str(s.get("runner_id", "")),
            runner_started_at=datetime.now(timezone.utc).isoformat(),
            runner_completed_at=s.get("runner_completed_at"),
            runner_priority=float(s.get("runner_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "runner_status": updated.get("runner_status", RUNNER_ACTIVE), "bridge_only": True}


def pause_runner_job(strategy_id: str, note: str = "Runner job paused.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        update_factory_strategy_runner(
            conn=conn,
            strategy_id=strategy_id,
            runner_status=RUNNER_PAUSED,
            runner_note=note,
            runner_id=str(s.get("runner_id", "")),
            runner_started_at=s.get("runner_started_at"),
            runner_completed_at=s.get("runner_completed_at"),
            runner_priority=float(s.get("runner_priority", 0.0)),
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "runner_status": updated.get("runner_status", RUNNER_PAUSED), "bridge_only": True}


def complete_runner_job(strategy_id: str, note: str = "Runner job completed.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        if str(s.get("runner_status", "")) not in {RUNNER_ACTIVE, RUNNER_PAUSED}:
            return {"ok": False, "error": "invalid_runner_state", "strategy_id": strategy_id, "current_runner_status": s.get("runner_status", "")}
        ended = datetime.now(timezone.utc).isoformat()
        dur = duration_sec_between(str(s.get("runner_started_at") or ""), ended)
        update_factory_strategy_runner(
            conn=conn,
            strategy_id=strategy_id,
            runner_status=RUNNER_COMPLETED,
            runner_note=note,
            runner_id=str(s.get("runner_id", "")),
            runner_started_at=s.get("runner_started_at"),
            runner_completed_at=ended,
            runner_priority=float(s.get("runner_priority", 0.0)),
        )
        insert_performance_run(
            conn,
            strategy_id=strategy_id,
            outcome=OUTCOME_SUCCESS,
            duration_sec=dur,
            run_ended_at=ended,
            source=SOURCE_RUNNER,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "runner_status": updated.get("runner_status", RUNNER_COMPLETED), "bridge_only": True}


def fail_runner_job(strategy_id: str, note: str = "Runner job failed.") -> dict[str, object]:
    with get_connection() as conn:
        s = fetch_factory_strategy_by_id(conn, strategy_id)
        if s is None:
            return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
        ended = datetime.now(timezone.utc).isoformat()
        dur = duration_sec_between(str(s.get("runner_started_at") or ""), ended)
        update_factory_strategy_runner(
            conn=conn,
            strategy_id=strategy_id,
            runner_status=RUNNER_FAILED,
            runner_note=note,
            runner_id=str(s.get("runner_id", "")),
            runner_started_at=s.get("runner_started_at"),
            runner_completed_at=ended,
            runner_priority=float(s.get("runner_priority", 0.0)),
        )
        insert_performance_run(
            conn,
            strategy_id=strategy_id,
            outcome=OUTCOME_FAIL,
            duration_sec=dur,
            run_ended_at=ended,
            source=SOURCE_RUNNER,
        )
        updated = fetch_factory_strategy_by_id(conn, strategy_id) or {}
    return {"ok": True, "strategy_id": strategy_id, "runner_status": updated.get("runner_status", RUNNER_FAILED), "bridge_only": True}


def assign_fleet_runner_to_strategy(
    conn: sqlite3.Connection,
    strategy_id: str,
    fleet_runner_id: str,
    *,
    assignment_reason: str = "",
) -> dict[str, object]:
    """Set demo runner_id on an eligible strategy; orchestration only (no ack/start)."""
    s = fetch_factory_strategy_by_id(conn, strategy_id)
    if s is None:
        return {"ok": False, "error": "strategy_not_found", "strategy_id": strategy_id}
    fleet_runner_id = str(fleet_runner_id).strip()
    if not fleet_runner_id:
        return {"ok": False, "error": "invalid_runner_id", "strategy_id": strategy_id}
    rid_existing = str(s.get("runner_id", "") or "").strip()
    if rid_existing and rid_existing != fleet_runner_id:
        return {
            "ok": False,
            "error": "already_assigned_to_other_runner",
            "strategy_id": strategy_id,
            "current_runner_id": rid_existing,
        }
    eligible, elig_reason = is_runner_eligible(s)
    state = str(s.get("runner_status", "") or "")
    job_runner_status = state if state in RUNNER_STATES else ""
    job_like = {
        "eligible": eligible,
        "runner_id": rid_existing,
        "runner_status": job_runner_status,
    }
    if not job_needs_fleet_assignment(job_like):
        return {
            "ok": False,
            "error": "not_eligible_for_fleet_assignment",
            "strategy_id": strategy_id,
            "eligibility_reason": elig_reason,
            "runner_status": state,
        }
    priority = float(s.get("runner_priority", 0.0)) or compute_runner_priority(s)
    old_note = str(s.get("runner_note", "") or "").strip()
    suffix = f"[fleet:{fleet_runner_id}]"
    if assignment_reason:
        suffix += f" {assignment_reason[:200]}"
    new_note = (old_note + " " + suffix).strip() if old_note else suffix
    update_factory_strategy_runner(
        conn=conn,
        strategy_id=str(strategy_id),
        runner_status=RUNNER_PENDING,
        runner_note=new_note,
        runner_id=fleet_runner_id,
        runner_started_at=s.get("runner_started_at"),
        runner_completed_at=s.get("runner_completed_at"),
        runner_priority=priority,
    )
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "runner_id": fleet_runner_id,
        "runner_status": RUNNER_PENDING,
        "assignment_only": True,
    }


def get_multi_runner_status() -> dict[str, object]:
    now = datetime.now(timezone.utc)
    with get_connection() as conn:
        runner_rows = fetch_multi_runner_runners(conn)
        strategies = fetch_factory_strategies(conn)
    runners_out: list[dict[str, object]] = []
    for r in runner_rows:
        rid = str(r["runner_id"])
        ac = count_assigned_jobs_for_runner(strategies, rid)
        disp, tag = effective_runner_status(r, now=now, assigned_jobs_count=ac)
        cap = max(1, int(r.get("runner_capacity", 1) or 1))
        load_rep = int(r.get("current_load", 0) or 0)
        used = max(load_rep, ac)
        runners_out.append(
            {
                "runner_id": rid,
                "runner_capacity": cap,
                "current_load": load_rep,
                "assigned_jobs_count": ac,
                "effective_used_slots": used,
                "spare_capacity": max(0, cap - used),
                "last_seen_at": r.get("last_seen_at"),
                "runner_health": r.get("runner_health"),
                "db_runner_status": r.get("runner_status"),
                "runner_status": disp,
                "status_explain": tag,
            }
        )
    summary = build_fleet_summary(runners_out)
    unassigned_n = 0
    assigned_n = 0
    feedback = get_paper_feedback_preview().get("results", [])
    jobs = build_runner_jobs(strategies=strategies, feedback_results=feedback, limit=500).get("jobs", []) or []
    for j in jobs:
        if str(j.get("runner_id", "") or "").strip():
            assigned_n += 1
        else:
            unassigned_n += 1
    return {
        "fleet_summary": summary,
        "jobs_summary": {
            "total_runner_jobs_view": len(jobs),
            "with_runner_id": assigned_n,
            "unassigned_queue": unassigned_n,
        },
        "runners": runners_out,
        "assignment_only": True,
        "demo_simulation_only": True,
        "updated_at": now.isoformat(),
    }


def get_multi_runner_jobs(limit: int = 200) -> dict[str, object]:
    jobs_payload = get_runner_jobs(limit=limit)
    jobs = list(jobs_payload.get("jobs", []) or [])
    grouped = group_jobs_by_runner(jobs)
    return {
        **grouped,
        "count": len(jobs),
        "limit": limit,
        "assignment_only": True,
        "demo_simulation_only": True,
    }


def multi_runner_register_runner(runner_id: str, capacity: int = 4) -> dict[str, object]:
    rid = str(runner_id).strip()
    if not rid:
        return {"ok": False, "error": "invalid_runner_id"}
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        upsert_multi_runner_register(conn, runner_id=rid, runner_capacity=max(1, int(capacity)), last_seen_at=now)
        row = fetch_multi_runner_by_id(conn, rid)
    return {"ok": True, "runner": row, "demo_simulation_only": True}


def cluster_register(
    runner_id: str,
    hostname: str = "",
    ip: str = "",
    capacity: int = 4,
    current_load: int = 0,
    version: str = "",
    region: str = "global",
) -> dict[str, object]:
    # keep existing multi_runner registry in sync
    base = multi_runner_register_runner(runner_id=runner_id, capacity=capacity)
    state = load_cluster_state()
    reg = cluster_register_runner(
        state,
        runner_id=runner_id,
        hostname=hostname,
        ip=ip,
        capacity=capacity,
        current_load=current_load,
        version=version,
        region=region,
    )
    save_cluster_state(state)
    return {"ok": bool(reg.get("ok")), "runner": reg.get("runner"), "multi_runner_register": base}


def multi_runner_heartbeat_runner(runner_id: str, current_load: int = 0) -> dict[str, object]:
    rid = str(runner_id).strip()
    now = datetime.now(timezone.utc).isoformat()
    load_v = max(0, int(current_load))
    with get_connection() as conn:
        existing = fetch_multi_runner_by_id(conn, rid)
        if existing is None:
            return {"ok": False, "error": "runner_not_found", "runner_id": rid}
        cap = max(1, int(existing.get("runner_capacity", 1) or 1))
        load_v = min(load_v, cap)
        st = RUNNER_IDLE if load_v <= 0 else RUNNER_BUSY
        update_multi_runner_heartbeat(
            conn,
            runner_id=rid,
            current_load=load_v,
            runner_status=st,
            last_seen_at=now,
        )
        row = fetch_multi_runner_by_id(conn, rid)
    return {"ok": True, "runner": row, "demo_simulation_only": True}


def cluster_heartbeat(
    runner_id: str,
    current_load: int = 0,
    version: str = "",
) -> dict[str, object]:
    base = multi_runner_heartbeat_runner(runner_id=runner_id, current_load=current_load)
    state = load_cluster_state()
    hb = cluster_heartbeat_runner(
        state,
        runner_id=runner_id,
        current_load=current_load,
        version=version or None,
    )
    save_cluster_state(state)
    return {"ok": bool(hb.get("ok")), "runner": hb.get("runner"), "multi_runner_heartbeat": base}


def _runner_targets_by_id() -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for row in _RUNNER_HEARTBEAT_TARGETS:
        rid = str(row.get("runner_id") or "").strip()
        if rid:
            out[rid] = dict(row)
    return out


def _stabilize_runner_heartbeats_once() -> dict[str, object]:
    targets = _runner_targets_by_id()
    runners_payload = get_cluster_runners()
    rows = list(runners_payload.get("runners", []) or [])
    by_id = {str(r.get("runner_id") or ""): r for r in rows if isinstance(r, dict)}
    actions: list[dict[str, object]] = []

    # Ensure expected runners exist (auto-register if missing).
    for rid, cfg in targets.items():
        if rid in by_id:
            continue
        reg = cluster_register(
            runner_id=rid,
            hostname=str(cfg.get("hostname") or ""),
            ip=str(cfg.get("ip") or ""),
            capacity=int(cfg.get("capacity") or 4),
            current_load=0,
            version="heartbeat-stabilizer",
            region=str(cfg.get("region") or "global"),
        )
        actions.append({"runner_id": rid, "action": "register", "ok": bool(reg.get("ok"))})

    # Refresh liveness for known targets.
    rows = list(get_cluster_runners().get("runners", []) or [])
    for r in rows:
        rid = str(r.get("runner_id") or "")
        if rid not in targets:
            continue
        hb = cluster_heartbeat(
            runner_id=rid,
            current_load=int(r.get("current_load") or 0),
            version="heartbeat-stabilizer",
        )
        actions.append({"runner_id": rid, "action": "heartbeat", "ok": bool(hb.get("ok"))})

    return {"ok": True, "actions": actions, "updated_at": datetime.now(timezone.utc).isoformat()}


def _runner_heartbeat_stabilizer_loop(interval_sec: int = 10) -> None:
    while not _runner_heartbeat_stop.wait(timeout=max(3, int(interval_sec))):
        try:
            _stabilize_runner_heartbeats_once()
        except Exception:
            # Safety loop only; never crash backend from stabilizer failure.
            continue


def start_runner_heartbeat_stabilizer(interval_sec: int = 10) -> dict[str, object]:
    global _runner_heartbeat_thread
    if _runner_heartbeat_thread is not None and _runner_heartbeat_thread.is_alive():
        return {"started": False, "reason": "already_running"}
    _runner_heartbeat_stop.clear()
    _runner_heartbeat_thread = threading.Thread(
        target=lambda: _runner_heartbeat_stabilizer_loop(interval_sec=interval_sec),
        name="runner_heartbeat_stabilizer",
        daemon=True,
    )
    _runner_heartbeat_thread.start()
    # Kick an immediate pass so startup does not wait for first interval.
    _stabilize_runner_heartbeats_once()
    return {"started": True, "interval_sec": interval_sec}


def multi_runner_offline_runner(runner_id: str) -> dict[str, object]:
    rid = str(runner_id).strip()
    if not rid:
        return {"ok": False, "error": "invalid_runner_id"}
    with get_connection() as conn:
        set_multi_runner_offline(conn, runner_id=rid)
        row = fetch_multi_runner_by_id(conn, rid)
    return {"ok": True, "runner": row, "demo_simulation_only": True}


def cluster_offline(runner_id: str) -> dict[str, object]:
    base = multi_runner_offline_runner(runner_id=runner_id)
    state = load_cluster_state()
    off = cluster_mark_runner_offline(state, runner_id=runner_id)
    # Estimate failover recommendations from current runner jobs view.
    queued = list(get_multi_runner_jobs(limit=500).get("unassigned_queue", []) or [])
    reassign_plan = estimate_failover_reassignments(
        state,
        failed_runner_id=str(runner_id),
        queued_jobs=queued,
    )
    save_cluster_state(state)
    reassigned = multi_runner_assign_jobs()
    return {
        "ok": bool(off.get("ok")),
        "runner": off.get("runner"),
        "failover_reassign_plan": reassign_plan,
        "multi_runner_offline": base,
        "multi_runner_reassign": reassigned,
    }


def _get_cluster_status_snapshot_readonly() -> dict[str, object]:
    """In-memory offline detection + payload; no SQLite write (avoids blocking health/runner paths)."""
    state = load_cluster_state()
    _ = apply_offline_detection(state, stale_seconds=180)
    return build_cluster_status_payload(state)


def get_cluster_status() -> dict[str, object]:
    state = load_cluster_state()
    det = apply_offline_detection(state, stale_seconds=180)
    if int(det.get("changed") or 0) > 0:
        try:
            save_cluster_state(state)
        except sqlite3.OperationalError:
            # Still return computed payload; avoid blocking callers on lock contention.
            pass
    return build_cluster_status_payload(state)


def get_cluster_runners() -> dict[str, object]:
    state = load_cluster_state()
    det = apply_offline_detection(state, stale_seconds=180)
    if int(det.get("changed") or 0) > 0:
        try:
            save_cluster_state(state)
        except sqlite3.OperationalError:
            pass
    return build_cluster_runners_payload(state)


def get_demo_execution_playbook_status() -> dict[str, object]:
    st = load_playbook_state()
    return build_playbook_status_payload(
        state=st,
        system_health=get_system_health(),
        risk_status=get_global_risk_status(),
        meta_status=get_meta_ai_control_status(),
        runner_status=get_runner_status(),
        cluster_status=get_cluster_status(),
        autonomous_status=get_autonomous_fund_status(),
    )


def post_demo_execution_playbook_start() -> dict[str, object]:
    st = start_playbook(load_playbook_state())
    save_playbook_state(st)
    out = get_demo_execution_playbook_status()
    out["action"] = "start"
    return out


def post_demo_execution_playbook_next() -> dict[str, object]:
    st = load_playbook_state()
    status = build_playbook_status_payload(
        state=st,
        system_health=get_system_health(),
        risk_status=get_global_risk_status(),
        meta_status=get_meta_ai_control_status(),
        runner_status=get_runner_status(),
        cluster_status=get_cluster_status(),
        autonomous_status=get_autonomous_fund_status(),
    )
    nxt, advanced, reason = next_playbook_phase(st, str(status.get("readiness", "BLOCKED")))
    if advanced:
        save_playbook_state(nxt)
    out = build_playbook_status_payload(
        state=nxt,
        system_health=get_system_health(),
        risk_status=get_global_risk_status(),
        meta_status=get_meta_ai_control_status(),
        runner_status=get_runner_status(),
        cluster_status=get_cluster_status(),
        autonomous_status=get_autonomous_fund_status(),
    )
    out["action"] = "next"
    out["advanced"] = advanced
    out["reason"] = reason
    return out


def post_demo_execution_playbook_reset() -> dict[str, object]:
    st = reset_playbook()
    save_playbook_state(st)
    out = get_demo_execution_playbook_status()
    out["action"] = "reset"
    return out


def get_demo_playbook_checks() -> dict[str, object]:
    st = load_playbook_state()
    system = get_system_health()
    runner = get_runner_status()
    system_for_checks = dict(system)
    system_for_checks["runner_status"] = runner
    return build_phase_gate_checklist(
        playbook_state=st,
        system_status=system_for_checks,
        meta_status=get_meta_ai_control_status(),
        risk_status=get_global_risk_status(),
        cluster_status=get_cluster_status(),
        autonomous_status=get_autonomous_fund_status(),
    )


def multi_runner_assign_jobs() -> dict[str, object]:
    now = datetime.now(timezone.utc)
    with get_connection() as conn:
        strategies = fetch_factory_strategies(conn)
        feedback = get_paper_feedback_preview().get("results", [])
        jobs_full = build_runner_jobs(strategies=strategies, feedback_results=feedback, limit=500)["jobs"]
        runner_rows = fetch_multi_runner_runners(conn)
        assigned_plan, skipped_plan = plan_balanced_assignments(
            jobs_full, runner_rows, now=now, strategies=strategies
        )
        applied: list[dict[str, object]] = []
        apply_skipped: list[dict[str, object]] = []
        for item in assigned_plan:
            strategies = fetch_factory_strategies(conn)
            sid = str(item["strategy_id"])
            target_rid = str(item["runner_id"])
            rrow = fetch_multi_runner_by_id(conn, target_rid)
            if rrow is None:
                apply_skipped.append(
                    {"strategy_id": sid, "reason": "runner_not_registered", "runner_id": target_rid}
                )
                continue
            ac = count_assigned_jobs_for_runner(strategies, target_rid)
            disp, tag = effective_runner_status(rrow, now=now, assigned_jobs_count=ac)
            cap = max(1, int(rrow.get("runner_capacity", 1) or 1))
            load_rep = int(rrow.get("current_load", 0) or 0)
            used = max(load_rep, ac)
            spare = cap - used
            if not is_assignable_effective(disp) or spare <= 0:
                apply_skipped.append(
                    {
                        "strategy_id": sid,
                        "reason": "apply_phase_runner_unavailable",
                        "runner_id": target_rid,
                        "display_status": disp,
                        "status_explain": tag,
                        "spare_capacity": spare,
                    }
                )
                continue
            res = assign_fleet_runner_to_strategy(
                conn,
                sid,
                target_rid,
                assignment_reason=str(item.get("reason", "")),
            )
            if res.get("ok"):
                applied.append({**item, **res})
            else:
                apply_skipped.append(
                    {
                        "strategy_id": sid,
                        "runner_id": target_rid,
                        "reason": res.get("error"),
                        "detail": {k: v for k, v in res.items() if k != "ok"},
                    }
                )
    return {
        "assigned": applied,
        "skipped": skipped_plan + apply_skipped,
        "assignment_only": True,
        "demo_simulation_only": True,
        "updated_at": now.isoformat(),
    }


def get_runner_status() -> dict[str, object]:
    jobs = get_runner_jobs(limit=200).get("jobs", [])
    return build_runner_status_payload(jobs)


@app.post("/paper/deploy")
def paper_deploy(max_bots: int = MAX_PAPER_BOTS) -> dict[str, object]:
    return deploy_paper_bots(max_bots=max_bots)


@app.get("/paper/status")
def paper_status() -> dict[str, object]:
    return get_paper_status()


@app.get("/paper/feedback")
def paper_feedback_preview() -> dict[str, object]:
    return get_paper_feedback_preview()


@app.post("/paper/feedback")
def paper_feedback_apply() -> dict[str, object]:
    return apply_paper_feedback()


@app.post("/auto/start")
def auto_start(
    interval_sec: int = 60,
    max_loops_per_hour: int = 30,
    max_strategies: int = 1000,
) -> dict[str, object]:
    return start_auto_loop(
        interval_sec=interval_sec,
        max_loops_per_hour=max_loops_per_hour,
        max_strategies=max_strategies,
    )


@app.post("/auto/stop")
def auto_stop() -> dict[str, object]:
    return stop_auto_loop()


@app.get("/auto/status")
def auto_status() -> dict[str, object]:
    return get_auto_loop_status()


@app.get("/autonomous/status")
def autonomous_status() -> dict[str, object]:
    return get_autonomous_fund_status()


@app.post("/autonomous/start")
def autonomous_start(
    interval_sec: int = 120,
    max_loops_per_hour: int = 20,
) -> dict[str, object]:
    return start_autonomous_fund_mode(
        interval_sec=interval_sec,
        max_loops_per_hour=max_loops_per_hour,
    )


@app.post("/autonomous/pause")
def autonomous_pause() -> dict[str, object]:
    return pause_autonomous_fund_mode()


@app.post("/autonomous/run_once")
def autonomous_run_once() -> dict[str, object]:
    return run_autonomous_fund_once()


@app.get("/system/health")
def system_health() -> dict[str, object]:
    return get_system_health()


@app.get("/system/errors")
def system_errors() -> dict[str, object]:
    return get_system_errors()


@app.post("/system/snapshot")
def system_snapshot() -> dict[str, object]:
    return post_system_snapshot()


@app.get("/live_safe/candidates")
def live_safe_candidates() -> dict[str, object]:
    return get_live_safe_candidates()


@app.post("/live_safe/promote")
def live_safe_promote() -> dict[str, object]:
    return promote_live_safe()


@app.get("/live_safe/status")
def live_safe_status() -> dict[str, object]:
    return get_live_safe_status()


@app.get("/live/status")
def live_data_status() -> dict[str, object]:
    return get_status_payload()


@app.get("/live/market")
def live_data_market() -> dict[str, object]:
    return get_market_payload()


@app.get("/live/symbol/{symbol}")
def live_data_symbol(symbol: str) -> dict[str, object]:
    return get_symbol_payload(symbol)


@app.get("/live/runner-feed")
def live_runner_sim_feed() -> dict[str, object]:
    """Latest prices/vol/trend per symbol for runner simulation (paper-only)."""
    return get_runner_live_sim_feed()


@app.post("/live/ingestion/refresh")
def live_ingestion_refresh_now() -> dict[str, object]:
    """Force one ingestion cycle (same as background loop tick)."""
    out: dict[str, object] = dict(live_data_refresh_once())
    try:
        merged_df, merged_meta = build_merged_daily_panel()
        out["merged_panel_rows"] = int(len(merged_df))
        out["merged_panel_path"] = str(merged_panel_path().resolve())
        out["merged_panel_meta"] = merged_meta
    except Exception as exc:
        out["merged_panel_error"] = str(exc)
    return out


@app.get("/data/pipeline/validation")
def data_pipeline_validation() -> dict[str, object]:
    """Historical + live + merge coverage (real data pipeline health)."""
    return validate_data_pipeline()


@app.get("/research/investor_snapshot")
def research_investor_snapshot() -> dict[str, object]:
    """Read-only latest client research JSON for investor UI (no execution)."""
    return get_investor_research_snapshot()


@app.get("/portfolio/allocation")
def portfolio_allocation() -> dict[str, object]:
    return get_portfolio_allocation()


@app.get("/risk/status")
def risk_status() -> dict[str, object]:
    return get_global_risk_status()


@app.get("/risk/alerts")
def risk_alerts() -> dict[str, object]:
    return get_global_risk_alerts()


@app.get("/regime/status")
def regime_status() -> dict[str, object]:
    return get_market_regime_status()


@app.get("/regime/recommendations")
def regime_recommendations() -> dict[str, object]:
    return get_market_regime_recommendations()


@app.get("/debug/regime/parity")
def debug_regime_parity() -> dict[str, object]:
    return get_debug_regime_parity()


@app.get("/memory/status")
def memory_status() -> dict[str, object]:
    return get_long_term_memory_status()


@app.get("/memory/strategy")
def memory_strategy() -> dict[str, object]:
    return get_memory_strategy_view()


@app.get("/memory/family")
def memory_family() -> dict[str, object]:
    return get_memory_family_view()


@app.get("/memory/regime")
def memory_regime() -> dict[str, object]:
    return get_memory_regime_view()


@app.post("/memory/update")
def memory_update() -> dict[str, object]:
    return post_memory_update()


@app.get("/meta/status")
def meta_status() -> dict[str, object]:
    return get_meta_ai_control_status()


@app.get("/meta/recommendations")
def meta_recommendations() -> dict[str, object]:
    return get_meta_ai_recommendations()


@app.get("/meta/learning/status")
def meta_learning_status() -> dict[str, object]:
    return get_meta_learning_status()


@app.get("/meta/learning/insights")
def meta_learning_insights() -> dict[str, object]:
    return get_meta_learning_insights()


@app.post("/meta/learning/update")
def meta_learning_update() -> dict[str, object]:
    return post_meta_learning_update()


@app.get("/report/summary")
def report_summary() -> dict[str, object]:
    return get_report_summary()


@app.get("/report/daily")
def report_daily() -> dict[str, object]:
    return get_report_daily()


@app.get("/review/candidates")
def review_candidates(limit: int = 25) -> dict[str, object]:
    return get_review_candidates(limit=limit)


@app.post("/review/approve")
def review_approve(strategy_id: str, reviewer: str = "operator") -> dict[str, object]:
    return approve_review_candidate(strategy_id=strategy_id, reviewer=reviewer)


@app.post("/review/reject")
def review_reject(
    strategy_id: str,
    note: str = "Rejected by review desk.",
    reviewer: str = "operator",
) -> dict[str, object]:
    return reject_review_candidate(strategy_id=strategy_id, note=note, reviewer=reviewer)


@app.post("/review/flag")
def review_flag(
    strategy_id: str,
    note: str = "Needs more testing.",
    reviewer: str = "operator",
) -> dict[str, object]:
    return flag_review_candidate(strategy_id=strategy_id, note=note, reviewer=reviewer)


@app.get("/review/status")
def review_status() -> dict[str, object]:
    return get_review_status()


@app.get("/demo/candidates")
def demo_candidates(limit: int = 25) -> dict[str, object]:
    return get_demo_candidates(limit=limit)


@app.post("/demo/queue")
def demo_queue(
    strategy_id: str,
    note: str = "Queued for controlled demo deployment review.",
) -> dict[str, object]:
    return queue_demo_candidate(strategy_id=strategy_id, note=note)


@app.post("/demo/assign")
def demo_assign(
    strategy_id: str,
    assignee: str = "demo_operator",
    note: str = "Assigned for demo slot preparation.",
) -> dict[str, object]:
    return assign_demo_candidate(strategy_id=strategy_id, assignee=assignee, note=note)


@app.post("/demo/pause")
def demo_pause(
    strategy_id: str,
    note: str = "Paused in demo workflow.",
) -> dict[str, object]:
    return pause_demo_candidate(strategy_id=strategy_id, note=note)


@app.post("/demo/reject")
def demo_reject(
    strategy_id: str,
    note: str = "Rejected from demo queue.",
) -> dict[str, object]:
    return reject_demo_candidate(strategy_id=strategy_id, note=note)


@app.get("/demo/status")
def demo_status() -> dict[str, object]:
    return get_demo_status()


@app.get("/demo/playbook/status")
def demo_playbook_status() -> dict[str, object]:
    return get_demo_execution_playbook_status()


@app.get("/demo/playbook/checks")
def demo_playbook_checks() -> dict[str, object]:
    return get_demo_playbook_checks()


@app.post("/demo/playbook/start")
def demo_playbook_start() -> dict[str, object]:
    return post_demo_execution_playbook_start()


@app.post("/demo/playbook/next")
def demo_playbook_next() -> dict[str, object]:
    return post_demo_execution_playbook_next()


@app.post("/demo/playbook/reset")
def demo_playbook_reset() -> dict[str, object]:
    return post_demo_execution_playbook_reset()


@app.get("/operator/status")
def operator_status() -> dict[str, object]:
    return get_operator_console_status()


@app.get("/alerts")
def alerts_list() -> dict[str, object]:
    return get_alerts()


@app.get("/alerts/summary")
def alerts_summary() -> dict[str, object]:
    return get_alerts_summary()


@app.post("/alerts/ack")
def alerts_acknowledge(alert_id: str) -> dict[str, object]:
    return acknowledge_alert(alert_id)


@app.get("/recovery/status")
def recovery_status() -> dict[str, object]:
    return get_recovery_engine_status()


@app.post("/recovery/run")
def recovery_run() -> dict[str, object]:
    return run_recovery_engine()


@app.get("/performance/strategies")
def performance_strategies() -> dict[str, object]:
    return get_performance_strategies()


@app.get("/performance/system")
def performance_system() -> dict[str, object]:
    return get_performance_system()


@app.get("/performance/top")
def performance_top() -> dict[str, object]:
    return get_performance_top()


@app.get("/promotion/candidates")
def promotion_candidates() -> dict[str, object]:
    return get_promotion_candidates()


@app.post("/promotion/run")
def promotion_run() -> dict[str, object]:
    bootstrap = run_bootstrap_demo_flow()
    promotion = run_smart_promotion_engine()
    return {**promotion, "bootstrap": bootstrap}


@app.get("/executor/candidates")
def executor_candidates(limit: int = 25) -> dict[str, object]:
    return get_executor_candidates(limit=limit)


@app.post("/executor/prepare")
def executor_prepare(
    strategy_id: str,
    target: str = "demo_runner",
    note: str = "Prepared for controlled demo executor integration.",
) -> dict[str, object]:
    return prepare_executor_item(strategy_id=strategy_id, target=target, note=note)


@app.post("/executor/start")
def executor_start(
    strategy_id: str,
    note: str = "Executor marked as running (simulated).",
) -> dict[str, object]:
    return start_executor_item(strategy_id=strategy_id, note=note)


@app.post("/executor/pause")
def executor_pause(
    strategy_id: str,
    note: str = "Executor paused.",
) -> dict[str, object]:
    return pause_executor_item(strategy_id=strategy_id, note=note)


@app.post("/executor/stop")
def executor_stop(
    strategy_id: str,
    note: str = "Executor stopped.",
) -> dict[str, object]:
    return stop_executor_item(strategy_id=strategy_id, note=note)


@app.get("/executor/status")
def executor_status() -> dict[str, object]:
    return get_executor_status()


@app.get("/runner/jobs")
def runner_jobs(limit: int = 25) -> dict[str, object]:
    return get_runner_jobs(limit=limit)


@app.post("/runner/ack")
def runner_ack(
    strategy_id: str,
    runner_id: str = "demo_runner",
    note: str = "Job acknowledged by runner.",
) -> dict[str, object]:
    return ack_runner_job(strategy_id=strategy_id, runner_id=runner_id, note=note)


@app.post("/runner/start")
def runner_start(
    strategy_id: str,
    note: str = "Runner job active.",
) -> dict[str, object]:
    return start_runner_job(strategy_id=strategy_id, note=note)


@app.post("/runner/pause")
def runner_pause(
    strategy_id: str,
    note: str = "Runner job paused.",
) -> dict[str, object]:
    return pause_runner_job(strategy_id=strategy_id, note=note)


@app.post("/runner/complete")
def runner_complete(
    strategy_id: str,
    note: str = "Runner job completed.",
) -> dict[str, object]:
    return complete_runner_job(strategy_id=strategy_id, note=note)


@app.post("/runner/fail")
def runner_fail(
    strategy_id: str,
    note: str = "Runner job failed.",
) -> dict[str, object]:
    return fail_runner_job(strategy_id=strategy_id, note=note)


@app.get("/runner/status")
def runner_status() -> dict[str, object]:
    return get_runner_status()


@app.get("/multi_runner/status")
def multi_runner_status() -> dict[str, object]:
    return get_multi_runner_status()


@app.get("/multi_runner/jobs")
def multi_runner_jobs(limit: int = 200) -> dict[str, object]:
    return get_multi_runner_jobs(limit=limit)


@app.post("/multi_runner/register")
def multi_runner_register(runner_id: str, capacity: int = 4) -> dict[str, object]:
    return multi_runner_register_runner(runner_id=runner_id, capacity=capacity)


@app.post("/multi_runner/heartbeat")
def multi_runner_heartbeat(runner_id: str, current_load: int = 0) -> dict[str, object]:
    return multi_runner_heartbeat_runner(runner_id=runner_id, current_load=current_load)


@app.post("/multi_runner/offline")
def multi_runner_offline(runner_id: str) -> dict[str, object]:
    return multi_runner_offline_runner(runner_id=runner_id)


@app.post("/multi_runner/assign")
def multi_runner_assign() -> dict[str, object]:
    return multi_runner_assign_jobs()


@app.get("/cluster/status")
def cluster_status() -> dict[str, object]:
    return get_cluster_status()


@app.get("/cluster/runners")
def cluster_runners() -> dict[str, object]:
    return get_cluster_runners()


@app.post("/cluster/register")
def cluster_register_endpoint(
    runner_id: str,
    hostname: str = "",
    ip: str = "",
    capacity: int = 4,
    current_load: int = 0,
    version: str = "",
    region: str = "global",
) -> dict[str, object]:
    return cluster_register(
        runner_id=runner_id,
        hostname=hostname,
        ip=ip,
        capacity=capacity,
        current_load=current_load,
        version=version,
        region=region,
    )


@app.post("/cluster/heartbeat")
def cluster_heartbeat_endpoint(
    runner_id: str,
    current_load: int = 0,
    version: str = "",
) -> dict[str, object]:
    return cluster_heartbeat(
        runner_id=runner_id,
        current_load=current_load,
        version=version,
    )


@app.post("/cluster/offline")
def cluster_offline_endpoint(runner_id: str) -> dict[str, object]:
    return cluster_offline(runner_id=runner_id)


@app.get("/capital/status")
def capital_status() -> dict[str, object]:
    return get_capital_status()


@app.get("/brain", response_model=BrainOut)
def get_brain() -> BrainOut:
    with get_connection() as conn:
        bots = fetch_bots(conn)

    profits = [bot["profit"] for bot in bots]
    avg_win_rate = (
        sum(bot["win_rate"] for bot in bots) / len(bots)
        if bots
        else 0.0
    )
    regime, message = detect_regime(profits=profits, avg_win_rate=avg_win_rate)
    return BrainOut(regime=regime, message=message)
