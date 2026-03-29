#!/usr/bin/env python3
"""
Client-demo research bundle: long-history data (optional fetch), weekend cycle with
research ranking, presentation portfolios, and a readable Markdown + JSON summary.

Demo / paper / research only — no live trading, no broker execution routing changes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import DATA_DIR
from backend.weekend_evolution_engine import run_weekend_cycle, save_report


def _run_fetch() -> None:
    fetch_py = ROOT / "scripts" / "fetch_ohlc_history.py"
    subprocess.run(
        [sys.executable, str(fetch_py), "--period", "10y", "--interval", "1d"],
        cwd=str(ROOT),
        check=True,
    )


def _fmt_portfolio(rows: list[dict], label: str) -> list[str]:
    lines = [f"### {label}", ""]
    if not rows:
        lines.append("_None._")
        lines.append("")
        return lines
    for i, r in enumerate(rows, 1):
        bt = r.get("backtest") or {}
        lines.append(
            f"{i}. **{r.get('family')}** — `{r.get('strategy_id')}`  \n"
            f"   Weight {float(r.get('weight', 0)):.4f} · "
            f"Research score {r.get('fitness_score')} · "
            f"Replay return {bt.get('total_return')} · "
            f"Max DD (replay) {bt.get('max_drawdown_pct')}%"
        )
    lines.append("")
    return lines


def _write_markdown(report: dict[str, Any], path: Path, meta: dict | None) -> None:
    pres = report.get("presentation") or {}
    verdict = (pres.get("client_demo_verdict") or {})
    hist_meta = report.get("history_meta") or meta
    symbols = (hist_meta or {}).get("symbols_included") or report.get("panel_symbols") or []
    years = (hist_meta or {}).get("years_approx")
    d0 = (hist_meta or {}).get("date_start")
    d1 = (hist_meta or {}).get("date_end")

    if years is not None:
        span_line = f"- **Approx. span:** ~{years} years (`{d0}` → `{d1}`)"
    elif d0 and d1:
        span_line = f"- **Date range:** `{d0}` → `{d1}`"
    else:
        span_line = "- **Date range:** _unknown — run `scripts/fetch_ohlc_history.py`_"

    lines: list[str] = [
        "# AlgoSphere — client research summary",
        "",
        f"_Generated UTC: {report.get('timestamp_utc')}_",
        "",
        "## Scope",
        "",
        "- **Mode:** Research / historical replay on a **daily equal-weight basket** (and optional aligned symbol panel).",
        "- **Not:** Live trading, guaranteed performance, or access to proprietary execution.",
        "",
        "## Data",
        "",
        f"- **Symbols (fetch):** {', '.join(symbols) if symbols else '_see meta or run fetch_'}",
        span_line,
        f"- **Bars used in replay:** {report.get('bars_used')}",
        f"- **History source:** `{report.get('history_source')}`",
        "",
        "### Limitations (read before sharing)",
        "",
    ]
    for lim in report.get("research_limitations") or []:
        lines.append(f"- {lim}")
    lines.append("")

    eval_lines = [
        "## Evaluation",
        "",
        f"- **Strategies evaluated:** {report.get('evaluated_count')}",
        f"- **Ranking:** `{report.get('ranking_mode')}` composite (drawdown, stability, segment robustness; return is damped).",
    ]
    if pres.get("family_mix_evaluated"):
        eval_lines.append(
            "- **Family mix (all candidates):** ```json\n"
            + json.dumps(pres["family_mix_evaluated"], indent=2)
            + "\n```"
        )
    eval_lines.append("")
    lines += eval_lines

    lines += [
        "## Presentation portfolios",
        "",
        "### Best growth book (proxy)",
        "",
        f"- **Weighted replay return proxy:** {pres.get('growth_weighted_return_proxy')}",
        f"- **Avg |correlation|:** {(pres.get('growth_risk_profile') or {}).get('avg_pairwise_abs_correlation')}",
        "",
    ]
    lines += _fmt_portfolio(pres.get("growth_portfolio") or [], "Growth portfolio")

    lines += [
        "### Demo-safe book (proxy)",
        "",
        f"- **Weighted replay return proxy:** {pres.get('demo_safe_weighted_return_proxy')}",
        f"- **Avg |correlation|:** {(pres.get('demo_safe_risk_profile') or {}).get('avg_pairwise_abs_correlation')}",
        f"- **Diversification score (presentation heuristic):** {pres.get('demo_safe_diversification_score')}",
        "",
    ]
    lines += _fmt_portfolio(pres.get("demo_safe_portfolio") or [], "Demo-safe portfolio")

    lines += [
        "## Candidate shortlists",
        "",
        "### Top 5 safest (by stored expected drawdown, then score)",
        "",
    ]
    for row in pres.get("top_5_safest_candidates") or []:
        lines.append(
            f"- **{row.get('family')}** `{row.get('strategy_id')}` — "
            f"DD~{row.get('expected_drawdown')} · "
            f"research {row.get('research_composite')} · "
            f"replay max DD {row.get('max_drawdown_pct')}%"
        )
    lines.append("")
    lines.append("### Top 5 strongest growth (by replay total return, then score)")
    lines.append("")
    for row in pres.get("top_5_strongest_growth_candidates") or []:
        lines.append(
            f"- **{row.get('family')}** `{row.get('strategy_id')}` — "
            f"return {row.get('total_return')} · "
            f"research {row.get('research_composite')} · "
            f"replay max DD {row.get('max_drawdown_pct')}%"
        )
    lines.append("")

    go = verdict.get("demo_paper_go_no_go", "NO-GO")
    lines += [
        "## Demo / paper GO / NO-GO",
        "",
        f"**{go}**",
        "",
    ]
    if verdict.get("reasons"):
        lines.append("Reasons:")
        for r in verdict["reasons"]:
            lines.append(f"- {r}")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(x for x in lines if x is not None), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Client research report (demo-only).")
    parser.add_argument("--refresh-data", action="store_true", help="Run Yahoo fetch (10y daily) first")
    parser.add_argument("--min-generate", type=int, default=900, help="Strategies per cycle")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DATA_DIR / "research_reports",
        help="Directory for JSON + Markdown output",
    )
    args = parser.parse_args()

    if args.refresh_data:
        _run_fetch()

    report = run_weekend_cycle(
        [],
        min_generate=args.min_generate,
        seed=args.seed,
        prefer_synthetic_history=False,
        synthetic_bars=900,
        top_portfolio_n=20,
        max_correlation=0.72,
        portfolio_weighting="risk_parity",
        ranking_mode="research",
        include_presentation_portfolios=True,
        presentation_top_n=8,
        presentation_max_correlation=0.60,
        enforce_portfolio_quotas=True,
        cycle_index=0,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = args.out_dir / f"client_research_{stamp}.json"
    md_path = args.out_dir / f"client_research_{stamp}.md"

    slim = {k: v for k, v in report.items() if k != "strategies_for_db"}
    slim["evaluated_strategy_count"] = len(report.get("strategies_for_db", []))
    json_path.write_text(json.dumps(slim, indent=2), encoding="utf-8")

    _write_markdown(report, md_path, report.get("history_meta"))

    weekend_json = save_report(report, DATA_DIR / "weekend_reports")

    print("Wrote:", json_path)
    print("Wrote:", md_path)
    print("Weekend JSON:", weekend_json)
    print(
        "Verdict:",
        (report.get("presentation") or {}).get("client_demo_verdict", {}).get("demo_paper_go_no_go"),
    )


if __name__ == "__main__":
    main()
