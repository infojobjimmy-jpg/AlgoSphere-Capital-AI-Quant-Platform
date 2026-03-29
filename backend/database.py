import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .config import DATA_DIR, DB_PATH


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    # WAL + busy timeout reduce "database is locked" under concurrent API threads.
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_bot_control_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(bots)").fetchall()
    col_names = {row[1] for row in rows}
    if "control_state" not in col_names:
        conn.execute(
            "ALTER TABLE bots ADD COLUMN control_state TEXT NOT NULL DEFAULT 'MONITOR'"
        )
    if "control_active" not in col_names:
        conn.execute(
            "ALTER TABLE bots ADD COLUMN control_active INTEGER NOT NULL DEFAULT 1"
        )
    if "alloc_multiplier" not in col_names:
        conn.execute(
            "ALTER TABLE bots ADD COLUMN alloc_multiplier REAL NOT NULL DEFAULT 1.0"
        )
    if "control_reason" not in col_names:
        conn.execute(
            "ALTER TABLE bots ADD COLUMN control_reason TEXT NOT NULL DEFAULT ''"
        )


def _ensure_factory_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(factory_strategies)").fetchall()
    col_names = {row[1] for row in rows}
    if "parent_strategy_id" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN parent_strategy_id TEXT")
    if "generation" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN generation INTEGER NOT NULL DEFAULT 0")
    if "mutation_note" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN mutation_note TEXT NOT NULL DEFAULT ''")
    if "origin_type" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN origin_type TEXT NOT NULL DEFAULT 'GENERATED'")
    if "review_status" not in col_names:
        conn.execute(
            "ALTER TABLE factory_strategies ADD COLUMN review_status TEXT NOT NULL DEFAULT 'PENDING_REVIEW'"
        )
    if "review_note" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN review_note TEXT NOT NULL DEFAULT ''")
    if "reviewer" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN reviewer TEXT NOT NULL DEFAULT ''")
    if "reviewed_at" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN reviewed_at TEXT")
    if "review_priority" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN review_priority REAL NOT NULL DEFAULT 0.0")
    if "demo_status" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN demo_status TEXT NOT NULL DEFAULT ''")
    if "demo_note" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN demo_note TEXT NOT NULL DEFAULT ''")
    if "demo_assignee" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN demo_assignee TEXT NOT NULL DEFAULT ''")
    if "demo_assigned_at" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN demo_assigned_at TEXT")
    if "demo_priority" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN demo_priority REAL NOT NULL DEFAULT 0.0")
    if "executor_status" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN executor_status TEXT NOT NULL DEFAULT ''")
    if "executor_note" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN executor_note TEXT NOT NULL DEFAULT ''")
    if "executor_target" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN executor_target TEXT NOT NULL DEFAULT ''")
    if "executor_assigned_at" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN executor_assigned_at TEXT")
    if "executor_priority" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN executor_priority REAL NOT NULL DEFAULT 0.0")
    if "runner_status" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_status TEXT NOT NULL DEFAULT ''")
    if "runner_note" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_note TEXT NOT NULL DEFAULT ''")
    if "runner_id" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_id TEXT NOT NULL DEFAULT ''")
    if "runner_started_at" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_started_at TEXT")
    if "runner_completed_at" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_completed_at TEXT")
    if "runner_priority" not in col_names:
        conn.execute("ALTER TABLE factory_strategies ADD COLUMN runner_priority REAL NOT NULL DEFAULT 0.0")


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                balance REAL NOT NULL DEFAULT 10000,
                risk_limit REAL NOT NULL DEFAULT 0.02
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                profit REAL NOT NULL,
                drawdown REAL NOT NULL,
                win_rate REAL NOT NULL,
                trades INTEGER NOT NULL,
                score REAL NOT NULL,
                risk_level TEXT NOT NULL,
                capital_alloc REAL NOT NULL,
                decision TEXT NOT NULL,
                control_state TEXT NOT NULL DEFAULT 'MONITOR',
                control_active INTEGER NOT NULL DEFAULT 1,
                alloc_multiplier REAL NOT NULL DEFAULT 1.0,
                control_reason TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS factory_strategies (
                strategy_id TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                parameters TEXT NOT NULL,
                fitness_score REAL NOT NULL,
                expected_drawdown REAL NOT NULL,
                expected_win_rate REAL NOT NULL,
                risk_profile TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                parent_strategy_id TEXT,
                generation INTEGER NOT NULL DEFAULT 0,
                mutation_note TEXT NOT NULL DEFAULT '',
                origin_type TEXT NOT NULL DEFAULT 'GENERATED',
                review_status TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
                review_note TEXT NOT NULL DEFAULT '',
                reviewer TEXT NOT NULL DEFAULT '',
                reviewed_at TEXT,
                review_priority REAL NOT NULL DEFAULT 0.0,
                demo_status TEXT NOT NULL DEFAULT '',
                demo_note TEXT NOT NULL DEFAULT '',
                demo_assignee TEXT NOT NULL DEFAULT '',
                demo_assigned_at TEXT,
                demo_priority REAL NOT NULL DEFAULT 0.0,
                executor_status TEXT NOT NULL DEFAULT '',
                executor_note TEXT NOT NULL DEFAULT '',
                executor_target TEXT NOT NULL DEFAULT '',
                executor_assigned_at TEXT,
                executor_priority REAL NOT NULL DEFAULT 0.0,
                runner_status TEXT NOT NULL DEFAULT '',
                runner_note TEXT NOT NULL DEFAULT '',
                runner_id TEXT NOT NULL DEFAULT '',
                runner_started_at TEXT,
                runner_completed_at TEXT,
                runner_priority REAL NOT NULL DEFAULT 0.0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_bots (
                strategy_id TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                status TEXT NOT NULL,
                paper_profit REAL NOT NULL DEFAULT 0.0,
                paper_drawdown REAL NOT NULL DEFAULT 0.0,
                paper_win_rate REAL NOT NULL DEFAULT 0.0,
                paper_trades INTEGER NOT NULL DEFAULT 0,
                deployed_at TEXT NOT NULL,
                last_updated TEXT NOT NULL,
                sim_note TEXT NOT NULL DEFAULT ''
            )
            """
        )
        _ensure_bot_control_columns(conn)
        _ensure_factory_columns(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_acknowledgements (
                alert_id TEXT PRIMARY KEY,
                acknowledged_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_engine_state (
                state_key TEXT PRIMARY KEY,
                state_value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                duration_sec REAL NOT NULL DEFAULT 0.0,
                run_ended_at TEXT NOT NULL,
                source TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_performance_run_log_strategy ON performance_run_log(strategy_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_performance_run_log_ended ON performance_run_log(run_ended_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS promotion_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                target_tier TEXT NOT NULL,
                performance_score REAL NOT NULL,
                success_rate REAL NOT NULL,
                stability_score REAL NOT NULL,
                activity_score REAL NOT NULL,
                action_taken TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_promotion_history_created ON promotion_history(created_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS multi_runner_runners (
                runner_id TEXT PRIMARY KEY,
                runner_capacity INTEGER NOT NULL DEFAULT 4,
                current_load INTEGER NOT NULL DEFAULT 0,
                runner_status TEXT NOT NULL DEFAULT 'RUNNER_OFFLINE',
                last_seen_at TEXT,
                runner_health TEXT NOT NULL DEFAULT 'GOOD'
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO account (id, balance, risk_limit)
            VALUES (1, 10000, 0.02)
            """
        )
        conn.commit()


def fetch_account(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, balance, risk_limit FROM account WHERE id = 1"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO account (id, balance, risk_limit) VALUES (1, 10000, 0.02)"
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, balance, risk_limit FROM account WHERE id = 1"
        ).fetchone()
    return dict(row)


def fetch_bots(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_bot_control_columns(conn)
    conn.commit()
    rows = conn.execute(
        """
        SELECT id, name, profit, drawdown, win_rate, trades, score, risk_level, capital_alloc, decision,
               control_state, control_active, alloc_multiplier, control_reason
        FROM bots
        ORDER BY score DESC, profit DESC
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        mult = float(d.get("alloc_multiplier") or 1.0)
        base = float(d.get("capital_alloc") or 0.0)
        d["effective_capital"] = round(base * mult, 2)
        d["control_active"] = bool(d.get("control_active", 1))
        out.append(d)
    return out


def insert_factory_strategies(
    conn: sqlite3.Connection, strategies: list[dict[str, Any]]
) -> None:
    rows = [
        (
            s["strategy_id"],
            s["family"],
            json.dumps(s["parameters"]),
            s["fitness_score"],
            s["expected_drawdown"],
            s["expected_win_rate"],
            s["risk_profile"],
            s["status"],
            s["created_at"],
            s.get("parent_strategy_id"),
            int(s.get("generation", 0)),
            s.get("mutation_note", ""),
            s.get("origin_type", "GENERATED"),
            s.get("review_status", "PENDING_REVIEW"),
            s.get("review_note", ""),
            s.get("reviewer", ""),
            s.get("reviewed_at"),
            float(s.get("review_priority", 0.0)),
            s.get("demo_status", ""),
            s.get("demo_note", ""),
            s.get("demo_assignee", ""),
            s.get("demo_assigned_at"),
            float(s.get("demo_priority", 0.0)),
            s.get("executor_status", ""),
            s.get("executor_note", ""),
            s.get("executor_target", ""),
            s.get("executor_assigned_at"),
            float(s.get("executor_priority", 0.0)),
            s.get("runner_status", ""),
            s.get("runner_note", ""),
            s.get("runner_id", ""),
            s.get("runner_started_at"),
            s.get("runner_completed_at"),
            float(s.get("runner_priority", 0.0)),
        )
        for s in strategies
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO factory_strategies (
            strategy_id, family, parameters, fitness_score, expected_drawdown,
            expected_win_rate, risk_profile, status, created_at,
            parent_strategy_id, generation, mutation_note, origin_type,
            review_status, review_note, reviewer, reviewed_at, review_priority,
            demo_status, demo_note, demo_assignee, demo_assigned_at, demo_priority,
            executor_status, executor_note, executor_target, executor_assigned_at, executor_priority,
            runner_status, runner_note, runner_id, runner_started_at, runner_completed_at, runner_priority
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def fetch_factory_strategies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_factory_columns(conn)
    conn.commit()
    rows = conn.execute(
        """
        SELECT strategy_id, family, parameters, fitness_score, expected_drawdown,
               expected_win_rate, risk_profile, status, created_at,
               parent_strategy_id, generation, mutation_note, origin_type,
               review_status, review_note, reviewer, reviewed_at, review_priority,
               demo_status, demo_note, demo_assignee, demo_assigned_at, demo_priority,
               executor_status, executor_note, executor_target, executor_assigned_at, executor_priority,
               runner_status, runner_note, runner_id, runner_started_at, runner_completed_at, runner_priority
        FROM factory_strategies
        ORDER BY fitness_score DESC, expected_win_rate DESC, expected_drawdown ASC
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        d["parameters"] = json.loads(d["parameters"])
        out.append(d)
    return out


def fetch_factory_strategies_for_runner_pipeline(
    conn: sqlite3.Connection, *, limit: int = 500
) -> list[dict[str, Any]]:
    """
    Subset of factory rows needed for runner job/status aggregation.
    Avoids loading the entire strategies table on hot paths (SQLite contention).
    """
    _ensure_factory_columns(conn)
    conn.commit()
    lim = max(1, min(int(limit), 5000))
    rows = conn.execute(
        """
        SELECT strategy_id, family, parameters, fitness_score, expected_drawdown,
               expected_win_rate, risk_profile, status, created_at,
               parent_strategy_id, generation, mutation_note, origin_type,
               review_status, review_note, reviewer, reviewed_at, review_priority,
               demo_status, demo_note, demo_assignee, demo_assigned_at, demo_priority,
               executor_status, executor_note, executor_target, executor_assigned_at, executor_priority,
               runner_status, runner_note, runner_id, runner_started_at, runner_completed_at, runner_priority
        FROM factory_strategies
        WHERE runner_status IN (
            'RUNNER_PENDING', 'RUNNER_ACKNOWLEDGED', 'RUNNER_ACTIVE',
            'RUNNER_PAUSED', 'RUNNER_COMPLETED', 'RUNNER_FAILED'
        )
        OR (
            executor_status = 'EXECUTOR_READY'
            AND demo_status IN ('DEMO_ASSIGNED', 'DEMO_RUNNING', 'DEMO_PAUSED')
            AND review_status IN ('APPROVED_FOR_DEMO', 'UNDER_REVIEW')
            AND IFNULL(risk_profile, 'MEDIUM') != 'HIGH'
            AND IFNULL(status, '') NOT IN ('LIVE_SAFE_REJECTED', 'PAPER_REJECTED')
        )
        ORDER BY
            CASE WHEN runner_priority > 0 THEN runner_priority ELSE 0 END DESC,
            fitness_score DESC,
            expected_win_rate DESC,
            expected_drawdown ASC
        LIMIT ?
        """,
        (lim,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        d["parameters"] = json.loads(d["parameters"])
        out.append(d)
    return out


def fetch_paper_bots_for_strategies(
    conn: sqlite3.Connection, strategy_ids: list[str]
) -> list[dict[str, Any]]:
    if not strategy_ids:
        return []
    placeholders = ",".join("?" * len(strategy_ids))
    rows = conn.execute(
        f"""
        SELECT strategy_id, family, status, paper_profit, paper_drawdown,
               paper_win_rate, paper_trades, deployed_at, last_updated, sim_note
        FROM paper_bots
        WHERE strategy_id IN ({placeholders})
        """,
        strategy_ids,
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_factory_strategy_by_id(conn: sqlite3.Connection, strategy_id: str) -> dict[str, Any] | None:
    _ensure_factory_columns(conn)
    conn.commit()
    row = conn.execute(
        """
        SELECT strategy_id, family, parameters, fitness_score, expected_drawdown,
               expected_win_rate, risk_profile, status, created_at,
               parent_strategy_id, generation, mutation_note, origin_type,
               review_status, review_note, reviewer, reviewed_at, review_priority,
               demo_status, demo_note, demo_assignee, demo_assigned_at, demo_priority,
               executor_status, executor_note, executor_target, executor_assigned_at, executor_priority,
               runner_status, runner_note, runner_id, runner_started_at, runner_completed_at, runner_priority
        FROM factory_strategies
        WHERE strategy_id = ?
        """,
        (strategy_id,),
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["parameters"] = json.loads(d["parameters"])
    return d


def update_factory_strategy_status(conn: sqlite3.Connection, strategy_id: str, status: str) -> None:
    conn.execute(
        "UPDATE factory_strategies SET status = ? WHERE strategy_id = ?",
        (status, strategy_id),
    )
    conn.commit()


def update_factory_strategy_review(
    conn: sqlite3.Connection,
    strategy_id: str,
    review_status: str,
    review_note: str = "",
    reviewer: str = "",
    reviewed_at: str | None = None,
    review_priority: float | None = None,
) -> None:
    if review_priority is None:
        conn.execute(
            """
            UPDATE factory_strategies
            SET review_status = ?, review_note = ?, reviewer = ?, reviewed_at = ?
            WHERE strategy_id = ?
            """,
            (review_status, review_note, reviewer, reviewed_at, strategy_id),
        )
    else:
        conn.execute(
            """
            UPDATE factory_strategies
            SET review_status = ?, review_note = ?, reviewer = ?, reviewed_at = ?, review_priority = ?
            WHERE strategy_id = ?
            """,
            (review_status, review_note, reviewer, reviewed_at, review_priority, strategy_id),
        )
    conn.commit()


def update_factory_strategy_demo(
    conn: sqlite3.Connection,
    strategy_id: str,
    demo_status: str,
    demo_note: str = "",
    demo_assignee: str = "",
    demo_assigned_at: str | None = None,
    demo_priority: float | None = None,
) -> None:
    if demo_priority is None:
        conn.execute(
            """
            UPDATE factory_strategies
            SET demo_status = ?, demo_note = ?, demo_assignee = ?, demo_assigned_at = ?
            WHERE strategy_id = ?
            """,
            (demo_status, demo_note, demo_assignee, demo_assigned_at, strategy_id),
        )
    else:
        conn.execute(
            """
            UPDATE factory_strategies
            SET demo_status = ?, demo_note = ?, demo_assignee = ?, demo_assigned_at = ?, demo_priority = ?
            WHERE strategy_id = ?
            """,
            (demo_status, demo_note, demo_assignee, demo_assigned_at, demo_priority, strategy_id),
        )
    conn.commit()


def update_factory_strategy_executor(
    conn: sqlite3.Connection,
    strategy_id: str,
    executor_status: str,
    executor_note: str = "",
    executor_target: str = "",
    executor_assigned_at: str | None = None,
    executor_priority: float | None = None,
) -> None:
    if executor_priority is None:
        conn.execute(
            """
            UPDATE factory_strategies
            SET executor_status = ?, executor_note = ?, executor_target = ?, executor_assigned_at = ?
            WHERE strategy_id = ?
            """,
            (executor_status, executor_note, executor_target, executor_assigned_at, strategy_id),
        )
    else:
        conn.execute(
            """
            UPDATE factory_strategies
            SET executor_status = ?, executor_note = ?, executor_target = ?, executor_assigned_at = ?, executor_priority = ?
            WHERE strategy_id = ?
            """,
            (
                executor_status,
                executor_note,
                executor_target,
                executor_assigned_at,
                executor_priority,
                strategy_id,
            ),
        )
    conn.commit()


def update_factory_strategy_runner(
    conn: sqlite3.Connection,
    strategy_id: str,
    runner_status: str,
    runner_note: str = "",
    runner_id: str = "",
    runner_started_at: str | None = None,
    runner_completed_at: str | None = None,
    runner_priority: float | None = None,
) -> None:
    if runner_priority is None:
        conn.execute(
            """
            UPDATE factory_strategies
            SET runner_status = ?, runner_note = ?, runner_id = ?, runner_started_at = ?, runner_completed_at = ?
            WHERE strategy_id = ?
            """,
            (runner_status, runner_note, runner_id, runner_started_at, runner_completed_at, strategy_id),
        )
    else:
        conn.execute(
            """
            UPDATE factory_strategies
            SET runner_status = ?, runner_note = ?, runner_id = ?, runner_started_at = ?, runner_completed_at = ?, runner_priority = ?
            WHERE strategy_id = ?
            """,
            (
                runner_status,
                runner_note,
                runner_id,
                runner_started_at,
                runner_completed_at,
                runner_priority,
                strategy_id,
            ),
        )
    conn.commit()


def upsert_paper_bot(conn: sqlite3.Connection, item: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO paper_bots (
            strategy_id, family, status, paper_profit, paper_drawdown,
            paper_win_rate, paper_trades, deployed_at, last_updated, sim_note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(strategy_id) DO UPDATE SET
            family = excluded.family,
            status = excluded.status,
            paper_profit = excluded.paper_profit,
            paper_drawdown = excluded.paper_drawdown,
            paper_win_rate = excluded.paper_win_rate,
            paper_trades = excluded.paper_trades,
            last_updated = excluded.last_updated,
            sim_note = excluded.sim_note
        """,
        (
            item["strategy_id"],
            item["family"],
            item["status"],
            item["paper_profit"],
            item["paper_drawdown"],
            item["paper_win_rate"],
            item["paper_trades"],
            item["deployed_at"],
            item["last_updated"],
            item.get("sim_note", ""),
        ),
    )
    conn.commit()


def fetch_paper_bots(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT strategy_id, family, status, paper_profit, paper_drawdown,
               paper_win_rate, paper_trades, deployed_at, last_updated, sim_note
        FROM paper_bots
        ORDER BY last_updated DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_acknowledged_alert_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT alert_id FROM alert_acknowledgements").fetchall()
    return {str(r[0]) for r in rows}


def acknowledge_alert_id(conn: sqlite3.Connection, alert_id: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO alert_acknowledgements (alert_id, acknowledged_at)
        VALUES (?, ?)
        """,
        (alert_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def get_alert_engine_state(conn: sqlite3.Connection, state_key: str) -> str | None:
    row = conn.execute(
        "SELECT state_value FROM alert_engine_state WHERE state_key = ?",
        (state_key,),
    ).fetchone()
    return str(row[0]) if row else None


def set_alert_engine_state(conn: sqlite3.Connection, state_key: str, state_value: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO alert_engine_state (state_key, state_value)
        VALUES (?, ?)
        """,
        (state_key, state_value),
    )
    conn.commit()


def insert_performance_run(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
    outcome: str,
    duration_sec: float,
    run_ended_at: str,
    source: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO performance_run_log (strategy_id, outcome, duration_sec, run_ended_at, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        (strategy_id, outcome, float(duration_sec), run_ended_at, source),
    )
    if commit:
        conn.commit()


def fetch_performance_run_log(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, strategy_id, outcome, duration_sec, run_ended_at, source
        FROM performance_run_log
        ORDER BY run_ended_at ASC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def insert_promotion_history(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
    target_tier: str,
    performance_score: float,
    success_rate: float,
    stability_score: float,
    activity_score: float,
    action_taken: str,
    detail: str = "",
    created_at: str | None = None,
) -> None:
    ts = created_at or datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO promotion_history (
            strategy_id, target_tier, performance_score, success_rate,
            stability_score, activity_score, action_taken, detail, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            strategy_id,
            target_tier,
            float(performance_score),
            float(success_rate),
            float(stability_score),
            float(activity_score),
            action_taken,
            detail,
            ts,
        ),
    )
    conn.commit()


def fetch_promotion_history(conn: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, strategy_id, target_tier, performance_score, success_rate,
               stability_score, activity_score, action_taken, detail, created_at
        FROM promotion_history
        ORDER BY id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_multi_runner_runners(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT runner_id, runner_capacity, current_load, runner_status, last_seen_at, runner_health
        FROM multi_runner_runners
        ORDER BY runner_id ASC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_multi_runner_by_id(conn: sqlite3.Connection, runner_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT runner_id, runner_capacity, current_load, runner_status, last_seen_at, runner_health
        FROM multi_runner_runners
        WHERE runner_id = ?
        """,
        (runner_id,),
    ).fetchone()
    return dict(row) if row else None


def upsert_multi_runner_register(
    conn: sqlite3.Connection,
    *,
    runner_id: str,
    runner_capacity: int,
    last_seen_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO multi_runner_runners (
            runner_id, runner_capacity, current_load, runner_status, last_seen_at, runner_health
        )
        VALUES (?, ?, 0, 'RUNNER_IDLE', ?, 'GOOD')
        ON CONFLICT(runner_id) DO UPDATE SET
            runner_capacity = excluded.runner_capacity,
            runner_status = 'RUNNER_IDLE',
            last_seen_at = excluded.last_seen_at,
            runner_health = 'GOOD'
        """,
        (runner_id, max(1, int(runner_capacity)), last_seen_at),
    )
    conn.commit()


def update_multi_runner_heartbeat(
    conn: sqlite3.Connection,
    *,
    runner_id: str,
    current_load: int,
    runner_status: str,
    last_seen_at: str,
) -> None:
    conn.execute(
        """
        UPDATE multi_runner_runners
        SET current_load = ?, runner_status = ?, last_seen_at = ?, runner_health = 'GOOD'
        WHERE runner_id = ?
        """,
        (max(0, int(current_load)), runner_status, last_seen_at, runner_id),
    )
    conn.commit()


def set_multi_runner_offline(conn: sqlite3.Connection, *, runner_id: str) -> None:
    conn.execute(
        """
        UPDATE multi_runner_runners
        SET runner_status = 'RUNNER_OFFLINE', current_load = 0, runner_health = 'GOOD'
        WHERE runner_id = ?
        """,
        (runner_id,),
    )
    conn.commit()
