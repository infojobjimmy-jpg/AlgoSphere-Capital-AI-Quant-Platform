"""One-shot FP Markets demo prep snapshot (demo-only; no broker execution)."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone

import requests

BASE = "http://127.0.0.1:8000"
TIMEOUT = 120
DEFAULT_MAX_STRATEGIES = 8000
IDS = [
    "7b069722db16463daf2adcc5b71ab05e",
    "10f805c8abeb4b348230f9974c21f257",
    "0f800a85119944aa8ddf3318c2f4256e",
]
RISK_PCT = 0.03


def get(path: str, **kw: object) -> dict:
    r = requests.get(BASE + path, timeout=TIMEOUT, **kw)
    r.raise_for_status()
    return r.json()


def post(path: str, **kw: object) -> dict:
    r = requests.post(BASE + path, timeout=TIMEOUT, **kw)
    r.raise_for_status()
    return r.json()


def fx_hours_remaining_week(now: datetime) -> tuple[float, str]:
    dow = now.weekday()
    if dow >= 5:
        nxt = now + timedelta(days=1)
        nxt = nxt.replace(hour=0, minute=0, second=0, microsecond=0)
        guard = 0
        while nxt.weekday() != 0 and guard < 10:
            nxt += timedelta(days=1)
            guard += 1
        return (nxt - now).total_seconds() / 3600.255, "closed_weekend_next_open_utc"
    if dow == 4 and now.hour >= 22:
        nxt = now + timedelta(days=1)
        nxt = nxt.replace(hour=0, minute=0, second=0, microsecond=0)
        guard = 0
        while nxt.weekday() != 0 and guard < 10:
            nxt += timedelta(days=1)
            guard += 1
        return (nxt - now).total_seconds() / 3600.0, "closed_post_fri_22utc"
    days_to_fri = (4 - dow) % 7
    fri_close = (now + timedelta(days=days_to_fri)).replace(
        hour=22, minute=0, second=0, microsecond=0
    )
    if fri_close <= now:
        fri_close += timedelta(days=7)
    return (fri_close - now).total_seconds() / 3600.0, "until_fri_22utc"


def main() -> None:
    now = datetime.now(timezone.utc)
    rem_h, rem_note = fx_hours_remaining_week(now)

    prep: dict = {
        "demo_only": True,
        "live_trading": False,
        "risk_per_strategy_pct": RISK_PCT,
        "strategy_ids": IDS,
    }

    try:
        auto_before = get("/auto/status")
    except Exception as e:
        print(json.dumps({"error": "backend_unreachable", "detail": str(e)}))
        sys.exit(1)

    if not auto_before.get("running"):
        try:
            prep["auto_loop_start"] = post(
                "/auto/start",
                params={
                    "interval_sec": 60,
                    "max_loops_per_hour": 30,
                    "max_strategies": DEFAULT_MAX_STRATEGIES,
                },
            )
        except Exception as e:
            prep["auto_loop_start"] = {"error": str(e)}
    else:
        prep["auto_loop_start"] = {"note": "already_running"}

    auto_full = get("/auto/status")
    prep["auto_loop_status"] = {
        k: auto_full.get(k)
        for k in (
            "running",
            "interval_sec",
            "max_loops_per_hour",
            "max_strategies",
            "loops_completed",
            "last_cycle_at",
            "last_error",
        )
    }

    try:
        pb = get("/demo/playbook/status")
        st = str(pb.get("state") or "").upper()
        if st in {"", "PLAYBOOK_IDLE", "NONE"}:
            prep["demo_playbook_start"] = post("/demo/playbook/start")
        else:
            prep["demo_playbook_start"] = {
                "note": "playbook_not_idle_skipped",
                "state": pb.get("state"),
            }
    except Exception as e:
        prep["demo_playbook_start"] = {"error": str(e)}
    prep["demo_playbook_status"] = get("/demo/playbook/status")

    sig = get("/control/signals")
    signals = sig.get("signals") or []
    prep["signals_summary"] = {
        "signals_generated": len(signals),
        "live_engine": sig.get("live_engine"),
        "bot_names": [s.get("name") for s in signals if isinstance(s, dict)],
        "fp_markets_note": (
            "External cBots historically consume `/control/signals` (e.g. `ctrader_first_live_bot`). "
            "The three factory strategy_ids are not emitted as separate control rows unless wired."
        ),
    }

    paper = get("/paper/status")
    perf = get("/performance/strategies")
    perf_by = {str(x.get("strategy_id")): x for x in (perf.get("strategies") or [])}
    runner = get("/runner/jobs", params={"limit": 500})
    jobs_by = {str(j.get("strategy_id")): j for j in (runner.get("jobs") or [])}
    paper_rows = {str(x.get("strategy_id")): x for x in (paper.get("running_paper_bots") or [])}

    active_markers = frozenset(
        {
            "RUNNER_ACTIVE",
            "EXECUTOR_RUNNING",
            "DEMO_ASSIGNED",
            "PAPER_RUNNING",
            "APPROVED_FOR_DEMO",
        }
    )

    per_sid: dict[str, dict] = {}
    for sid in IDS:
        row: dict = {"strategy_id": sid}
        try:
            lin = get(f"/factory/lineage/{sid}")
            row["found"] = lin.get("found")
            if lin.get("lineage"):
                n0 = lin["lineage"][0]
                row["status"] = n0.get("status")
                row["family"] = n0.get("family")
                row["symbol"] = n0.get("symbol")
        except Exception as e:
            row["lineage_error"] = str(e)
        pr = perf_by.get(sid, {})
        row["performance"] = {
            k: pr.get(k)
            for k in (
                "paper_trades",
                "performance_score",
                "success_rate",
                "paper_win_rate",
                "paper_drawdown",
                "total_runs",
            )
        }
        row["paper_bot"] = paper_rows.get(sid)
        row["runner_job"] = jobs_by.get(sid)
        st = str(row.get("status") or "")
        rj = row.get("runner_job") or {}
        rstat = str(rj.get("status") or rj.get("runner_status") or "")
        paper_st = str((row.get("paper_bot") or {}).get("status") or "")
        row["active_for_demo"] = (
            st in active_markers
            or "RUNNER" in st.upper()
            or rstat.upper() in {"ACTIVE", "RUNNING"}
            or paper_st == "PAPER_RUNNING"
        )
        per_sid[sid] = row

    prep["strategies"] = per_sid
    prep["paper_trades_selected_total"] = sum(
        int(
            (per_sid[s].get("paper_bot") or {}).get("paper_trades")
            or per_sid[s].get("performance", {}).get("paper_trades")
            or 0
        )
        for s in IDS
    )

    prep["broker_execution"] = {
        "trades_executed_on_fp_markets": None,
        "note": (
            "Algo Sphere does not ingest cTrader fills; confirm executed trades "
            "in FP Markets demo account history."
        ),
    }

    sh = get("/system/health")
    try:
        cs = get("/cluster/status")
    except Exception:
        cs = {}
    try:
        al = get("/alerts/summary")
    except Exception as e:
        al = {"error": str(e)}

    prep["system_stability"] = {
        "system_health": sh.get("system_health"),
        "engines_critical": [
            e
            for e in (sh.get("engines") or [])
            if isinstance(e, dict)
            and str(e.get("health", "")).upper() in {"CRITICAL", "FAILED"}
        ],
        "cluster_health": cs.get("cluster_health") if isinstance(cs, dict) else None,
        "alerts_open": al.get("open_count") or al.get("count"),
        "alert_recent": (al.get("recent") or al.get("alerts") or [])[:5],
    }

    prep["session_window"] = {
        "utc_now": now.isoformat(),
        "approx_remaining_liquid_fx_hours": round(rem_h, 2),
        "window_note": rem_note,
        "operational_cap": (
            f"{RISK_PCT}% equity per strategy on demo only; size down to micro lots in cTrader."
        ),
    }

    print(json.dumps(prep, indent=2, default=str))


if __name__ == "__main__":
    main()
