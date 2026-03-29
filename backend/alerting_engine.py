"""
Local-first alerting: aggregates signals from existing modules only.
No external messaging; decision support for operators.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def _alert_id(category: str, rule_code: str) -> str:
    raw = f"{category}:{rule_code}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_alerts(
    *,
    meta_status: dict[str, Any],
    capital_status: dict[str, Any],
    fund_status: dict[str, Any],
    report_summary: dict[str, Any],
    portfolio_allocation: dict[str, Any],
    paper_status: dict[str, Any],
    review_status: dict[str, Any],
    demo_status: dict[str, Any],
    executor_status: dict[str, Any],
    runner_status: dict[str, Any],
    runner_jobs: dict[str, Any],
    auto_status: dict[str, Any],
    operator_console: dict[str, Any] | None = None,
    runner_stale_no_jobs: bool = False,
) -> list[dict[str, Any]]:
    """Produce alert dicts. Caller filters by acknowledged ids."""
    alerts: list[dict[str, Any]] = []
    now = _now_iso()

    def add(
        rule_code: str,
        category: str,
        severity: str,
        title: str,
        message: str,
        source: str,
        action: str,
    ) -> None:
        alerts.append(
            {
                "alert_id": _alert_id(category, rule_code),
                "rule_code": rule_code,
                "category": category,
                "severity": severity,
                "title": title,
                "message": message,
                "source": source,
                "created_at": now,
                "active": True,
                "recommended_operator_action": action,
            }
        )

    # --- SYSTEM / Meta ---
    health = str(meta_status.get("system_health", "GOOD"))
    if health == "CRITICAL":
        add(
            "meta_health_critical",
            "SYSTEM",
            "CRITICAL",
            "System health critical",
            "Meta AI reports CRITICAL system health.",
            "Meta AI",
            "Stop auto loop if running; review fund, paper, and capital; investigate errors.",
        )
    elif health == "WARNING":
        add(
            "meta_health_warning",
            "SYSTEM",
            "WARNING",
            "System health degraded",
            "Meta AI reports WARNING system health.",
            "Meta AI",
            "Review reporting summary and operator console; tighten risk if needed.",
        )

    risk_mode = str(meta_status.get("risk_mode", "NORMAL"))
    if risk_mode == "DEFENSIVE":
        add(
            "meta_risk_defensive",
            "RISK",
            "WARNING",
            "Defensive risk mode",
            "Meta AI is in DEFENSIVE risk mode.",
            "Meta AI",
            "Reduce pipeline aggressiveness; prioritize review of weak strategies.",
        )

    # --- Auto loop ---
    last_err = auto_status.get("last_error")
    if last_err is not None and str(last_err).strip():
        add(
            "auto_loop_error",
            "SYSTEM",
            "CRITICAL",
            "Auto loop reported an error",
            f"last_error: {last_err}",
            "Auto Loop Engine",
            "Inspect auto loop logs; consider POST /auto/stop until resolved.",
        )

    # --- Capital ---
    total_cap = float(capital_status.get("total_capital", 0.0))
    free = float(capital_status.get("free", 0.0))
    risk_usage = float(capital_status.get("risk_usage", 0.0))
    allocated = float(capital_status.get("allocated", 0.0))

    if total_cap > 0 and free <= 0.01 and allocated >= total_cap * 0.99:
        add(
            "capital_fully_allocated",
            "CAPITAL",
            "WARNING",
            "Capital fully allocated",
            "Simulated free capital is near zero with high allocation.",
            "Capital Engine",
            "Review portfolio allocation and live-safe exposure; do not assume broker capacity.",
        )

    if risk_usage >= 0.95:
        add(
            "capital_risk_usage_critical",
            "CAPITAL",
            "CRITICAL",
            "Very high simulated risk usage",
            f"risk_usage={risk_usage:.4f}",
            "Capital Engine",
            "Reduce allocation pressure; review fund mode and portfolio AI weights.",
        )
    elif risk_usage >= 0.8:
        add(
            "capital_risk_usage_high",
            "CAPITAL",
            "WARNING",
            "High simulated risk usage",
            f"risk_usage={risk_usage:.4f}",
            "Capital Engine",
            "Monitor capital status; consider defensive portfolio action.",
        )

    # --- Fund / portfolio (reporting) ---
    portfolio_state = str(report_summary.get("portfolio_state", "NORMAL"))
    if portfolio_state == "LOCKDOWN":
        add(
            "fund_portfolio_lockdown",
            "RISK",
            "CRITICAL",
            "Portfolio state LOCKDOWN",
            "Fund/reporting layer indicates LOCKDOWN portfolio state.",
            "Fund Mode / Reporting Engine",
            "Halt new demo/runner progression until conditions improve; operator review required.",
        )
    elif portfolio_state == "DEFENSIVE":
        add(
            "fund_portfolio_defensive",
            "RISK",
            "WARNING",
            "Portfolio state DEFENSIVE",
            "Fund/reporting layer indicates DEFENSIVE portfolio state.",
            "Fund Mode / Reporting Engine",
            "Tighten risk; defer new approvals.",
        )

    fund_ps = str(fund_status.get("portfolio_state", "NORMAL"))
    if fund_ps == "LOCKDOWN" and portfolio_state != "LOCKDOWN":
        add(
            "fund_mode_lockdown",
            "RISK",
            "CRITICAL",
            "Fund mode LOCKDOWN",
            "Fund Mode evaluation returned LOCKDOWN.",
            "Fund Mode",
            "Stop new entries; review bot control states and capital.",
        )

    # --- Reporting warnings (mirror) ---
    for w in report_summary.get("warnings", []) or []:
        msg = str(w)
        sev = "WARNING" if "critical" not in msg.lower() else "CRITICAL"
        add(
            f"report_{hashlib.sha256(msg.encode()).hexdigest()[:12]}",
            "RISK",
            sev,
            "Reporting warning",
            msg,
            "Reporting Engine",
            "Cross-check operator console and capital; follow up on root cause.",
        )

    # --- Portfolio concentration ---
    total_alloc_pct = float(portfolio_allocation.get("total_allocated_percent", 0.0))
    if total_alloc_pct >= 99.0 and len(portfolio_allocation.get("allocations", [])) > 0:
        add(
            "portfolio_fully_allocated_pct",
            "RISK",
            "WARNING",
            "Portfolio allocation at cap",
            f"total_allocated_percent={total_alloc_pct:.2f}%",
            "Portfolio AI",
            "Review diversification; max per-strategy caps may be binding.",
        )

    # --- Paper / pipeline ---
    paper_items = paper_status.get("running_paper_bots", []) or []
    paper_count = len(paper_items)
    paper_success = sum(1 for p in paper_items if p.get("status") == "PAPER_SUCCESS")
    if paper_count > 0 and paper_success == 0:
        add(
            "paper_no_success",
            "PIPELINE",
            "WARNING",
            "No paper success yet",
            f"{paper_count} paper bot row(s) but none in PAPER_SUCCESS.",
            "Paper Trading / Feedback",
            "Run or review paper feedback; evolve or reject weak candidates.",
        )

    pipeline = (operator_console or {}).get("pipeline", {})
    total_candidates = int(pipeline.get("total_candidates", 0))
    if total_candidates > 1000:
        add(
            "pipeline_candidate_overflow",
            "PIPELINE",
            "INFO",
            "Large candidate backlog",
            f"total_candidates={total_candidates}",
            "Bot Factory / Operator Console",
            "Consider pruning, review, or slowing generation.",
        )

    # --- Review ---
    review_counts = review_status.get("counts", {}) or {}
    pending = int(review_counts.get("PENDING_REVIEW", 0))
    if pending > 200:
        add(
            "review_pending_critical",
            "REVIEW",
            "CRITICAL",
            "Very high review backlog",
            f"PENDING_REVIEW={pending}",
            "Candidate Review Desk",
            "Prioritize review desk; batch approve/reject to unblock pipeline.",
        )
    elif pending > 100:
        add(
            "review_pending_warning",
            "REVIEW",
            "WARNING",
            "Elevated review backlog",
            f"PENDING_REVIEW={pending}",
            "Candidate Review Desk",
            "Schedule review sessions to reduce queue.",
        )

    # --- Demo ---
    demo_counts = demo_status.get("counts", {}) or {}
    demo_rej = int(demo_counts.get("DEMO_REJECTED", 0))
    if demo_rej > 5:
        add(
            "demo_rejected_many",
            "DEMO",
            "WARNING",
            "Many demo queue rejections",
            f"DEMO_REJECTED={demo_rej}",
            "Demo Deploy Desk",
            "Review rejection reasons; adjust eligibility or review criteria.",
        )

    # --- Executor ---
    ex_counts = executor_status.get("counts", {}) or {}
    ex_rej = int(ex_counts.get("EXECUTOR_REJECTED", 0))
    if ex_rej > 3:
        add(
            "executor_rejected_many",
            "DEMO",
            "WARNING",
            "Multiple executor rejections",
            f"EXECUTOR_REJECTED={ex_rej}",
            "Demo Executor Adapter",
            "Inspect executor notes; verify demo assignment flow.",
        )

    # --- Runner ---
    rn_counts = runner_status.get("counts", {}) or {}
    rn_failed = int(rn_counts.get("RUNNER_FAILED", 0))
    if rn_failed > 0:
        add(
            "runner_has_failures",
            "RUNNER",
            "WARNING",
            "Runner jobs failed",
            f"RUNNER_FAILED={rn_failed}",
            "Demo Runner Bridge",
            "Inspect failed runner notes; retry or clear stuck jobs after review.",
        )

    eligible_jobs = [j for j in (runner_jobs.get("jobs", []) or []) if j.get("eligible")]
    ex_ready = int(ex_counts.get("EXECUTOR_READY", 0))
    if ex_ready > 0 and len(eligible_jobs) == 0:
        add(
            "runner_no_eligible_with_executor_ready",
            "RUNNER",
            "INFO",
            "Executor ready but no eligible runner jobs",
            f"EXECUTOR_READY={ex_ready} but no eligible jobs in bridge list.",
            "Demo Runner Bridge",
            "Ensure review/demo gates satisfied; start demo runner if appropriate.",
        )

    if runner_stale_no_jobs:
        add(
            "runner_stale_no_jobs",
            "RUNNER",
            "WARNING",
            "No eligible runner jobs for extended period",
            "Executor has had ready items but no eligible runner jobs observed recently.",
            "Demo Runner Bridge",
            "Check demo runner process; verify API URL and eligibility rules.",
        )

    # Dedupe by alert_id (same rule)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for a in alerts:
        aid = str(a["alert_id"])
        if aid in seen:
            continue
        seen.add(aid)
        unique.append(a)

    severity_order = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    unique.sort(key=lambda x: (severity_order.get(str(x["severity"]), 9), x["title"]))
    return unique


def apply_acknowledgements(
    alerts: list[dict[str, Any]], acknowledged_ids: set[str]
) -> list[dict[str, Any]]:
    out = []
    for a in alerts:
        aid = str(a["alert_id"])
        if aid in acknowledged_ids:
            c = dict(a)
            c["active"] = False
            out.append(c)
        else:
            out.append(a)
    return out


def build_alerts_summary(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    active = [a for a in alerts if a.get("active", True)]
    crit = sum(1 for a in active if a.get("severity") == "CRITICAL")
    warn = sum(1 for a in active if a.get("severity") == "WARNING")
    info = sum(1 for a in active if a.get("severity") == "INFO")
    top = sorted(
        active,
        key=lambda x: (
            {"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(str(x.get("severity")), 9),
            str(x.get("title", "")),
        ),
    )[:8]
    return {
        "total_alerts": len(active),
        "critical_count": crit,
        "warning_count": warn,
        "info_count": info,
        "top_active_alerts": [
            {
                "alert_id": x.get("alert_id"),
                "severity": x.get("severity"),
                "title": x.get("title"),
                "category": x.get("category"),
                "recommended_operator_action": x.get("recommended_operator_action"),
            }
            for x in top
        ],
    }
