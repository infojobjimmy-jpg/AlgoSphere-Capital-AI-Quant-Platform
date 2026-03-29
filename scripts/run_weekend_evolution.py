#!/usr/bin/env python3
"""
Weekend strategy evolution runner: generate, mutate, historical replay, diversify.

Demo / simulation only — no live trading, no broker execution.
Stop with Ctrl+C (or SIGTERM). Use --once for a single cycle.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import DATA_DIR
from backend.database import fetch_factory_strategies, get_connection, insert_factory_strategies
from backend.weekend_evolution_engine import run_weekend_cycle, save_report

STOP = False


def _request_stop(*_args: object) -> None:
    global STOP
    STOP = True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Algo Sphere weekend evolution (demo-only, runs until stopped)."
    )
    parser.add_argument(
        "--min-generate",
        type=int,
        default=1000,
        help="Minimum new strategies per cycle (default 1000+)",
    )
    parser.add_argument("--sleep-sec", type=float, default=180.0, help="Pause between cycles")
    parser.add_argument("--once", action="store_true", help="Run a single cycle then exit")
    parser.add_argument("--no-db", action="store_true", help="Do not write strategies to SQLite")
    parser.add_argument(
        "--historical-csv",
        type=Path,
        default=None,
        help="OHLC CSV with a 'close' column (or single numeric column). "
        "If omitted, uses data/ohlc_history.csv when present.",
    )
    parser.add_argument(
        "--no-auto-csv",
        action="store_true",
        help="Do not load data/ohlc_history.csv; use synthetic unless --historical-csv is set.",
    )
    parser.add_argument("--synthetic-bars", type=int, default=504, help="Bars when no CSV is used")
    parser.add_argument("--portfolio-n", type=int, default=20, help="Diversified portfolio size")
    parser.add_argument("--max-correlation", type=float, default=0.78)
    parser.add_argument(
        "--family-cap",
        type=int,
        default=None,
        help="Optional lower per-family cap (hard cap is 4 unless set lower for experiments)",
    )
    parser.add_argument(
        "--weighting",
        choices=("equal", "risk_parity"),
        default="risk_parity",
        help="Portfolio weights (default: inverse vol on segment PnLs)",
    )
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    report_dir = DATA_DIR / "weekend_reports"
    cycle = 0
    print(
        "Weekend evolution mode: DEMO ONLY — no execution. "
        f"Reports -> {report_dir}. Ctrl+C to stop.",
        flush=True,
    )

    while not STOP:
        cycle += 1
        with get_connection() as conn:
            existing = fetch_factory_strategies(conn)

        report = run_weekend_cycle(
            existing,
            min_generate=args.min_generate,
            seed=args.seed,
            historical_csv=args.historical_csv,
            prefer_synthetic_history=args.no_auto_csv and args.historical_csv is None,
            synthetic_bars=args.synthetic_bars,
            top_portfolio_n=args.portfolio_n,
            max_correlation=args.max_correlation,
            family_cap=args.family_cap,
            portfolio_weighting=args.weighting,
            cycle_index=cycle,
        )

        if not args.no_db:
            with get_connection() as conn:
                insert_factory_strategies(conn, report["strategies_for_db"])

        saved = save_report(report, report_dir)
        port = report.get("diversified_portfolio") or []
        risk = report.get("risk_profile") or {}
        print(f"\n--- Cycle {cycle} ---", flush=True)
        print(f"Evaluated: {report.get('evaluated_count')} | Saved report: {saved}", flush=True)
        print(f"Diversified portfolio (n={len(port)}):", flush=True)
        for row in port[:5]:
            print(
                f"  {row['strategy_id'][:10]}… {row['family']} "
                f"score={row['fitness_score']} w={row.get('weight')}",
                flush=True,
            )
        if len(port) > 5:
            print(f"  … +{len(port) - 5} more", flush=True)
        print(
            f"Risk: mix={risk.get('family_mix')} | avg|corr|={risk.get('avg_pairwise_abs_correlation')} "
            f"| diversification_score={risk.get('diversification_score')} "
            f"| quotas_met={risk.get('quotas_met')}",
            flush=True,
        )

        if args.once or STOP:
            break
        time.sleep(max(0.0, args.sleep_sec))

    print("Weekend evolution stopped.", flush=True)


if __name__ == "__main__":
    main()
