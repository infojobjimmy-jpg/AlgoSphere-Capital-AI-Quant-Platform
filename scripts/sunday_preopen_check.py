#!/usr/bin/env python3
"""
Sunday 16:00 pre-open readiness: live status + one weekend evolution cycle (demo/paper only).
Load env from repo-root sunday_open.env before importing backend when run standalone.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_FILE = ROOT / "sunday_open.env"
API = os.environ.get("ALGO_SPHERE_PREOPEN_API", "http://127.0.0.1:8000")


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ[k.strip()] = v.strip()


def _post_refresh() -> dict:
    req = Request(
        f"{API}/live/ingestion/refresh",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    with urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def _get_live_status() -> dict:
    with urlopen(f"{API}/live/status", timeout=30) as r:
        return json.loads(r.read().decode())


def main() -> int:
    _load_env_file(ENV_FILE)

    from backend.database import fetch_factory_strategies, get_connection
    from backend.live_data_engine import LIVE_OFFLINE, live_ohlc_extension_path
    from backend.weekend_evolution_engine import run_weekend_cycle

    out: dict = {"env_file": str(ENV_FILE)}

    # Force refresh if API up
    try:
        out["ingestion_refresh"] = _post_refresh()
        time.sleep(0.5)
    except (OSError, HTTPError, URLError) as e:
        out["ingestion_refresh_error"] = str(e)

    try:
        st = _get_live_status()
        out["live_status"] = st
    except (OSError, HTTPError, URLError) as e:
        out["live_status_error"] = str(e)
        st = {}

    ext_path = live_ohlc_extension_path()
    rows = 0
    if ext_path.is_file():
        try:
            rows = max(0, sum(1 for _ in ext_path.open(encoding="utf-8")) - 1)
        except OSError:
            rows = 0
    out["ohlc_live_extension_rows_disk"] = rows

    report_dir = ROOT / "data" / "weekend_reports"
    prior_reports = list(report_dir.glob("weekend_report_*.json")) if report_dir.is_dir() else []

    with get_connection() as conn:
        existing = fetch_factory_strategies(conn)

    report = run_weekend_cycle(
        existing,
        min_generate=800,
        seed=42,
        prefer_synthetic_history=False,
        synthetic_bars=504,
        top_portfolio_n=20,
        max_correlation=0.78,
        enforce_portfolio_quotas=True,
        portfolio_weighting="risk_parity",
        cycle_index=len(prior_reports) + 1,
    )

    from backend.weekend_evolution_engine import save_report

    saved = save_report(report, report_dir)
    out["weekend_report_saved"] = str(saved)
    out["preopen_cycle_index"] = report.get("cycle_index")
    out["evaluated_count"] = report.get("evaluated_count")
    out["history_source"] = report.get("history_source")
    out["bars_used"] = report.get("bars_used")

    risk = report.get("risk_profile") or {}
    port = report.get("diversified_portfolio") or []
    by_id = {s["strategy_id"]: s for s in report.get("strategies_for_db", [])}

    weighted_ret = 0.0
    for row in port:
        w = float(row.get("weight") or 0)
        bt = row.get("backtest") or {}
        weighted_ret += w * float(bt.get("total_return") or 0.0)

    safest = sorted(
        port,
        key=lambda r: (
            float(by_id.get(r["strategy_id"], {}).get("expected_drawdown", 9999)),
            -float(r.get("fitness_score") or 0),
        ),
    )[:3]
    safest_out = []
    for r in safest:
        sid = r["strategy_id"]
        full = by_id.get(sid, {})
        safest_out.append(
            {
                "strategy_id": sid,
                "family": r.get("family"),
                "fitness_score": r.get("fitness_score"),
                "expected_drawdown": full.get("expected_drawdown"),
                "risk_profile": full.get("risk_profile"),
                "weight": r.get("weight"),
            }
        )

    ing_ok = bool(st.get("ingestion_thread_running"))
    health = str(st.get("data_health") or "")
    lt = bool(st.get("live_testing_mode"))
    qm = bool(risk.get("quotas_met"))
    div = float(risk.get("diversification_score") or 0)
    ac = risk.get("avg_pairwise_abs_correlation")
    ac_f = float(ac) if ac is not None else 1.0

    reasons: list[str] = []
    if not ing_ok:
        reasons.append("ingestion_thread_not_running")
    if health == LIVE_OFFLINE:
        reasons.append("live_data_offline")
    if not lt:
        reasons.append("live_testing_mode_off")
    if not qm:
        reasons.append("portfolio_quotas_not_met")
    if div < 50:
        reasons.append("diversification_score_low")
    if ac is not None and ac_f > 0.88:
        reasons.append("avg_pairwise_correlation_high")

    go = len(reasons) == 0

    summary = {
        "live_ingestion_running": ing_ok,
        "live_testing_mode": lt,
        "symbols_active": st.get("symbols_tracked"),
        "ohlc_live_extension_rows": st.get("ohlc_live_extension_rows", rows),
        "weekend_evolution_cycles_completed_this_run": 1,
        "weekend_report_files_total": len(prior_reports) + 1,
        "total_evaluated_strategies": report.get("evaluated_count"),
        "best_final_diversified_portfolio": port,
        "quotas_met": qm,
        "diversification_score": risk.get("diversification_score"),
        "avg_pairwise_abs_correlation": risk.get("avg_pairwise_abs_correlation"),
        "expected_drawdown_portfolio_mean": risk.get("aggregate_expected_drawdown_mean"),
        "expected_return_portfolio_weighted_total_return": round(weighted_ret, 4),
        "fp_markets_demo_go_no_go": "GO" if go else "NO-GO",
        "go_no_go_reasons": reasons,
        "top_3_safest_strategies_demo": safest_out,
        "demo_paper_only": True,
        "no_live_trading": True,
    }

    print(json.dumps({**summary, "detail_file": str(saved)}, indent=2))
    return 0 if go else 1


if __name__ == "__main__":
    raise SystemExit(main())
