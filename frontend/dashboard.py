import os
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from backend.config import DEFAULT_API_URL
from frontend.investor_dashboard import render_investor_dashboard
from frontend.investor_landing import render_investor_landing
from frontend.private_access import ensure_private_access
from frontend.brand_theme import FAVICON_32_PATH, PAGE_TITLE
from frontend.public_deploy import PUBLIC_ALLOCATOR_EXPERIENCE_OPTIONS, is_public_allocator_deploy

_ROOT = Path(__file__).resolve().parents[1]
_env_file = _ROOT / ".env"
if _env_file.is_file():
    load_dotenv(_env_file)

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=str(FAVICON_32_PATH) if FAVICON_32_PATH.is_file() else None,
    layout="wide",
)

if is_public_allocator_deploy():
    api_url = (os.getenv("ALGO_SPHERE_API_URL") or DEFAULT_API_URL).rstrip("/")
    st.sidebar.caption("Allocator surface · read-only · API host set by deployment.")
else:
    api_url = st.sidebar.text_input("API URL", value=DEFAULT_API_URL)


def fetch_json(path: str) -> tuple[dict | None, str | None]:
    try:
        response = requests.get(f"{api_url}{path}", timeout=5)
        response.raise_for_status()
        return response.json(), None
    except requests.RequestException as exc:
        return None, str(exc)


def as_float(payload: dict, key: str, default: float = 0.0) -> float:
    return float(payload.get(key, default))


# -----------------------------
# AlgoSphere SaaS experience navigation (UI only)
# -----------------------------
_FULL_EXPERIENCE_OPTIONS = [
    "Landing",
    "Investor Landing",
    "Pricing & Plans",
    "Investor Dashboard",
    "Investor (private)",
    "Partner (private)",
    "Admin cockpit",
    "Client dashboard",
]

if "experience_page" not in st.session_state:
    st.session_state.experience_page = (
        "Investor Landing" if is_public_allocator_deploy() else "Landing"
    )

experience_options = (
    list(PUBLIC_ALLOCATOR_EXPERIENCE_OPTIONS)
    if is_public_allocator_deploy()
    else _FULL_EXPERIENCE_OPTIONS
)
if st.session_state.experience_page not in experience_options:
    st.session_state.experience_page = experience_options[0]

default_page = st.session_state.experience_page
experience_page = st.sidebar.selectbox(
    "AlgoSphere",
    experience_options,
    index=experience_options.index(default_page),
    key="experience_page",
)

if experience_page == "Investor Landing":
    st.sidebar.caption("Institutional allocator overview · read-only.")
    render_investor_landing(fetch_json)
    st.stop()

if experience_page == "Investor Dashboard":
    st.sidebar.caption("Investor presentation — read-only. No admin or execution controls.")
    render_investor_dashboard(fetch_json)
    st.stop()

if experience_page == "Investor (private)":
    st.sidebar.caption("Password-protected investor access · read-only.")
    ensure_private_access("investor")
    render_investor_dashboard(fetch_json, access_label="Investor (private)", partner_extensions=False)
    st.stop()

if experience_page == "Partner (private)":
    st.sidebar.caption("Password-protected partner access · read-only.")
    ensure_private_access("partner")
    render_investor_dashboard(
        fetch_json,
        access_label="Partner (private)",
        partner_extensions=True,
    )
    st.stop()

st.title("algo-sphere Pro Cockpit")
st.sidebar.caption("Local-first control cockpit. Monitoring + decision support only.")

# -----------------------------
# UI-only account/access model (no auth/billing yet)
# -----------------------------
if "account_profile_name" not in st.session_state:
    st.session_state.account_profile_name = "Demo User"
if "account_role" not in st.session_state:
    st.session_state.account_role = "Client"  # "Client" | "Admin"
if "account_plan" not in st.session_state:
    st.session_state.account_plan = "Explorer"

with st.sidebar.expander("Account (UI only)", expanded=False):
    st.text_input(
        "User profile",
        value=st.session_state.account_profile_name,
        key="account_profile_name",
    )
    st.selectbox(
        "Role",
        ["Client", "Admin"],
        index=0 if st.session_state.account_role == "Client" else 1,
        key="account_role",
    )

# Keep the existing operator/admin cockpit unchanged by default.
if experience_page == "Admin cockpit" and st.session_state.account_role != "Admin":
    st.title("AlgoSphere")
    st.error("Admin cockpit is locked for client users. Switch Role to `Admin` in the sidebar.")
    st.stop()

view_mode = "Client dashboard" if experience_page == "Client dashboard" else "Admin cockpit"


def _entitlements_for_plan(plan: str) -> set[str]:
    # Feature flags / entitlements: section visibility only (UI safety).
    return {
        "Explorer": {
            "market_regime",
            "ai_posture",
            "risk_level",
            "signals_summary",
        },
        "Trader": {
            "market_regime",
            "ai_posture",
            "risk_level",
            "signals_summary",
            "live_control",
            "active_bots_summary",
            "control_reasoning",
        },
        "Pro": {
            "market_regime",
            "ai_posture",
            "risk_level",
            "signals_summary",
            "live_control",
            "active_bots_summary",
            "control_reasoning",
            "strategy_registry",
            "strategy_scoring",
            "strategy_factory_summary",
            "performance_summary",
        },
        "Institutional": {
            "market_regime",
            "ai_posture",
            "risk_level",
            "signals_summary",
            "live_control",
            "active_bots_summary",
            "control_reasoning",
            "strategy_registry",
            "strategy_scoring",
            "strategy_factory_summary",
            "performance_summary",
            "portfolio_allocation_intelligence",
            "portfolio_brain",
            "cluster_fleet_summary",
            "advanced_strategy_insights",
        },
    }.get(str(plan), _entitlements_for_plan("Explorer"))


# -----------------------------
# Landing + Pricing pages (UI only)
# -----------------------------
PLAN_CATALOG: dict[str, dict[str, object]] = {
    "Explorer": {
        "price_placeholder": "$29/mo (placeholder)",
        "included_sections": [
            "Market Regime",
            "AI Posture",
            "Risk Level",
            "Signals Summary",
        ],
        "upgrade_path": "Open Client dashboard and select `Explorer`.",
    },
    "Trader": {
        "price_placeholder": "$79/mo (placeholder)",
        "included_sections": [
            "Everything in Explorer",
            "Live Control State",
            "Active Bots Summary",
            "Control Reasoning (sample)",
        ],
        "upgrade_path": "Open Client dashboard and select `Trader`.",
    },
    "Pro": {
        "price_placeholder": "$199/mo (placeholder)",
        "included_sections": [
            "Everything in Trader",
            "Strategy Registry (Top)",
            "Strategy Scoring (Top)",
            "Performance Summary",
        ],
        "upgrade_path": "Open Client dashboard and select `Pro`.",
    },
    "Institutional": {
        "price_placeholder": "$499/mo (placeholder)",
        "included_sections": [
            "Everything in Pro",
            "Portfolio Allocation Intelligence",
            "Portfolio Brain",
            "Cluster / Fleet Summary",
            "Advanced Strategy Insights",
        ],
        "upgrade_path": "Open Client dashboard and select `Institutional`.",
    },
}


if experience_page == "Landing":
    st.title("AlgoSphere")
    st.subheader("A decision-layer AI cockpit for safe market monitoring.")

    st.markdown("### What you get")
    st.markdown(
        "- Local-first monitoring + context snapshots (no execution).\n"
        "- Clear regime/risk posture and explainable signal summaries.\n"
        "- Tiered client access: show the right insights per subscription.\n"
    )

    st.markdown("### Why AlgoSphere is different")
    st.markdown(
        "- Recommendations are read-only (no trading or capital deployment).\n"
        "- Fast cockpit experience using cached endpoint patterns.\n"
        "- Live control logic is designed as an advisory engine, not an executor.\n"
    )

    c1, c2 = st.columns(2)
    if c1.button("See Pricing & Plans", use_container_width=True):
        st.session_state.experience_page = "Pricing & Plans"
        st.rerun()
    if c2.button("Open Client Dashboard", use_container_width=True):
        st.session_state.experience_page = "Client dashboard"
        st.rerun()

    st.caption("This onboarding UI is frontend-only. Backend and trading logic are unchanged.")
    st.stop()


if experience_page == "Pricing & Plans":
    st.title("Pricing & Plans")
    st.caption("Choose a plan to preview what your client dashboard will unlock.")
    st.divider()

    plan_order = ["Explorer", "Trader", "Pro", "Institutional"]
    cards = st.columns(4)
    for idx, plan in enumerate(plan_order):
        plan_info = PLAN_CATALOG.get(plan, {})
        included = plan_info.get("included_sections") or []
        price_placeholder = plan_info.get("price_placeholder") or "TBD"
        upgrade_path = plan_info.get("upgrade_path") or "Open Client dashboard."

        with cards[idx]:
            with st.container(border=True):
                st.subheader(plan)
                st.markdown(f"**{price_placeholder}**")
                st.caption("Monthly price placeholder")
                st.markdown("**Included sections**")
                for s in included:
                    st.write(f"- {s}")
                st.markdown("**Upgrade path**")
                st.write(upgrade_path)
                st.caption("UI-only preview. Actual gating happens in the client dashboard.")

    st.divider()
    st.markdown("Want to preview immediately? Switch to `Client dashboard` in the sidebar.")
    st.stop()


if view_mode == "Client dashboard":
    role = st.session_state.get("account_role", "Client")
    if "account_plan" not in st.session_state:
        st.session_state.account_plan = "Explorer"

    plan_selected = st.sidebar.selectbox(
        "Subscription plan",
        ["Explorer", "Trader", "Pro", "Institutional"],
        index=["Explorer", "Trader", "Pro", "Institutional"].index(
            st.session_state.account_plan
        ),
        key="client_subscription_plan",
    )
    st.session_state.account_plan = plan_selected

    # Role-based entitlement: Admin gets full client preview (equivalent to Institutional entitlements).
    effective_plan = "Institutional" if role == "Admin" else plan_selected
    # Keep `plan` for UI display consistency (client-selected plan).
    plan = plan_selected
    entitlements = _entitlements_for_plan(effective_plan)

    def can(section: str) -> bool:
        return section in entitlements

    # Read-only data fetches for client view. Avoids operator actions entirely.
    regime_status_payload, regime_status_err = (None, None)
    meta_payload, meta_err = (None, None)
    global_risk_payload, global_risk_err = (None, None)
    control_payload, control_err = (None, None)

    if can("market_regime"):
        regime_status_payload, regime_status_err = fetch_json("/regime/status")
    if can("ai_posture"):
        meta_payload, meta_err = fetch_json("/meta/status")
    if can("risk_level"):
        global_risk_payload, global_risk_err = fetch_json("/risk/status")
    if can("signals_summary") or can("live_control") or can("active_bots_summary") or can("control_reasoning"):
        control_payload, control_err = fetch_json("/control/signals")

    plan_theme = {
        "Explorer": {"badge": "Explorer", "tone": "Essential visibility"},
        "Trader": {"badge": "Trader", "tone": "Execution-aware intelligence"},
        "Pro": {"badge": "Pro", "tone": "Performance and strategy depth"},
        "Institutional": {"badge": "Institutional", "tone": "Portfolio and fleet intelligence"},
    }
    theme = plan_theme.get(plan, plan_theme["Explorer"])

    st.title("AlgoSphere Client Dashboard")
    with st.container(border=True):
        h1, h2, h3 = st.columns([2, 1, 1])
        h1.markdown(f"### Plan {theme['badge']}")
        h1.caption(theme["tone"])
        h2.metric("Current Plan", plan)
        h3.metric("Role", role)
        st.caption("Read-only client mode. No execution controls. No internal admin actions.")
    st.markdown("---")

    def show_locked(section_title: str, required_plan: str) -> None:
        # UI-only: do not reveal any internal data when a section is not entitled.
        with st.container(border=True):
            st.markdown(f"#### [LOCKED] {section_title}")
            st.caption(f"Available on `{required_plan}` and above.")
            st.write("- Premium insight preview")
            st.write("- Upgrade path available in Pricing & Plans")
            st.write("- Client-safe placeholder (no hidden data exposure)")

    if can("market_regime") and regime_status_payload is not None:
        st.markdown("### Core Market Context")
        rs = regime_status_payload
        c1, c2 = st.columns(2)
        c1.metric("current_regime", str(rs.get("current_regime", "—")))
        c2.metric("confidence_score", f"{float(rs.get('confidence_score', 0.0)):.3f}")
    elif can("market_regime") and regime_status_err:
        st.warning(f"Market regime unavailable: {regime_status_err}")

    if can("ai_posture") and meta_payload is not None:
        mp1, mp2 = st.columns(2)
        mp1.metric("system_posture", str(meta_payload.get("system_posture", "—")))
        mp2.metric("confidence", f"{float(meta_payload.get('confidence', 0.0)):.3f}")
    elif can("ai_posture") and meta_err:
        st.warning(f"Meta posture unavailable: {meta_err}")

    if can("risk_level") and global_risk_payload is not None:
        rr1, rr2 = st.columns(2)
        rr1.metric("risk_level", str(global_risk_payload.get("risk_level", "—")))
        rr2.metric("global_risk_score", f"{float(global_risk_payload.get('global_risk_score', 0.0)):.3f}")
    elif can("risk_level") and global_risk_err:
        st.warning(f"Risk status unavailable: {global_risk_err}")

    if can("signals_summary") and control_payload is not None:
        st.markdown("### Signal Overview")
        st.subheader("Simplified Signals Summary")
        signals = control_payload.get("signals") or []
        by_state: dict[str, int] = {}
        for s in signals:
            cs = str(s.get("control_state") or "")
            if cs:
                by_state[cs] = by_state.get(cs, 0) + 1
        c1, c2 = st.columns(2)
        c1.metric("total_signals", int(control_payload.get("count", len(signals)) or 0))
        c2.metric("active_signals", int(len([s for s in signals if bool(s.get("control_active"))])))
        if by_state:
            for k, v in sorted(by_state.items(), key=lambda kv: -kv[1]):
                st.write(f"- {k}: {v}")
    elif can("signals_summary") and control_err:
        st.warning(f"Control signals unavailable: {control_err}")

    if can("live_control") and control_payload is not None:
        st.markdown("### Control Intelligence")
        st.subheader("Live Control State")
        signals = control_payload.get("signals") or []
        active = [s for s in signals if bool(s.get("control_active"))]
        show = active if active else signals
        rows: list[dict[str, object]] = []
        for s in show:
            reasoning = s.get("reasoning") or []
            rows.append(
                {
                    "bot": s.get("name"),
                    "control_state": s.get("control_state"),
                    "control_level": s.get("control_level") or s.get("control_state"),
                    "boost_level": s.get("boost_level"),
                    "target_volume": s.get("target_volume"),
                    "recommended_action": s.get("recommended_action"),
                    "updated_at": s.get("updated_at"),
                    "reasoning": " | ".join([str(x) for x in reasoning[:3]]),
                }
            )
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    elif not can("live_control"):
        show_locked("Live Control State", "Trader")

    if can("active_bots_summary") and control_payload is not None:
        st.subheader("Active Bots Summary")
        signals = control_payload.get("signals") or []
        active = [s for s in signals if bool(s.get("control_active"))]
        st.write(f"active_bots={len(active)}")
    elif not can("active_bots_summary"):
        show_locked("Active Bots Summary", "Trader")

    if can("control_reasoning") and control_payload is not None:
        st.subheader("Control Reasoning (sample)")
        shown = 0
        for s in (control_payload.get("signals") or []):
            if shown >= 4:
                break
            rs = s.get("reasoning") or []
            if rs:
                st.caption(f"{s.get('name')} | {s.get('control_state')}")
                for line in rs[:2]:
                    st.write(f"- {line}")
                shown += 1
    elif not can("control_reasoning"):
        show_locked("Control Reasoning (sample)", "Trader")

    # Tier extensions (Pro / Institutional) — still read-only.
    if can("strategy_registry") or can("strategy_scoring") or can("performance_summary"):
        st.markdown("### Pro Analytics")
        factory_top_payload, _ = fetch_json("/factory/top?limit=10")
        perf_system_payload, _ = fetch_json("/performance/system")
        perf_top_payload, _ = fetch_json("/performance/top")
        perf_strategies_payload, _ = fetch_json("/performance/strategies")

        if can("strategy_registry") and factory_top_payload is not None:
            st.subheader("Strategy Registry (Top)")
            top_items = factory_top_payload.get("top", []) or []
            if top_items:
                st.dataframe(pd.DataFrame(top_items), use_container_width=True, hide_index=True)
        if can("performance_summary") and perf_system_payload is not None:
            st.subheader("Performance Summary")
            ps = perf_system_payload
            p1, p2, p3 = st.columns(3)
            p1.metric("runner_success_rate", f"{float(ps.get('runner_success_rate', 0.0)):.2%}")
            p2.metric("runner_fail_rate", f"{float(ps.get('runner_fail_rate', 0.0)):.2%}")
            p3.metric("pipeline_throughput", f"{float(ps.get('pipeline_throughput', 0.0)):.2f}")
        if can("strategy_scoring") and perf_top_payload is not None:
            st.subheader("Strategy Scoring (Top)")
            top_items = perf_top_payload.get("strategies") or []
            if top_items:
                st.dataframe(pd.DataFrame(top_items[:25]), use_container_width=True, hide_index=True)
        if can("strategy_scoring") and perf_strategies_payload is not None:
            st.caption("Additional strategy samples available.")
    else:
        show_locked("Strategy Registry (Top)", "Pro")
        show_locked("Strategy Scoring (Top)", "Pro")
        show_locked("Performance Summary", "Pro")

    if (
        can("portfolio_allocation_intelligence")
        or can("portfolio_brain")
        or can("cluster_fleet_summary")
        or can("advanced_strategy_insights")
    ):
        st.markdown("### Institutional Intelligence")
        portfolio_payload, _ = fetch_json("/portfolio/allocation")
        cluster_status_payload, _ = fetch_json("/cluster/status")
        multi_runner_status_payload, _ = fetch_json("/multi_runner/status")

        if can("portfolio_allocation_intelligence") and portfolio_payload is not None:
            st.subheader("Portfolio Allocation Intelligence")
            brain = portfolio_payload.get("brain") or {}
            c1, c2 = st.columns(2)
            c1.metric("allocation_count", int(brain.get("allocation_count", portfolio_payload.get("allocation_count", 0) or 0) or 0))
            c2.metric("total_allocated_percent", f"{float(brain.get('total_allocated_percent', 0.0) or 0.0):.2f}%")

        if can("portfolio_brain") and portfolio_payload is not None:
            st.subheader("Portfolio Brain")
            brain = portfolio_payload.get("brain") or {}
            top_priorities = brain.get("top_priorities") or []
            if top_priorities:
                st.dataframe(pd.DataFrame(top_priorities[:25]), use_container_width=True, hide_index=True)

        if can("cluster_fleet_summary") and (multi_runner_status_payload is not None or cluster_status_payload is not None):
            st.subheader("Cluster / Fleet Summary")
            if cluster_status_payload is not None:
                st.caption(f"cluster_health={cluster_status_payload.get('cluster_health', '—')}")
            if multi_runner_status_payload is not None:
                fs = multi_runner_status_payload.get("fleet_summary") or {}
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("runners", int(fs.get("runner_count", 0) or 0))
                c2.metric("healthy", int(fs.get("healthy_runner_count", 0) or 0))
                c3.metric("degraded", int(fs.get("degraded_count", 0) or 0))
                c4.metric("offline", int(fs.get("offline_count", 0) or 0))

        if can("advanced_strategy_insights"):
            st.subheader("Advanced Strategy Insights")
            perf_strategies_payload, _ = fetch_json("/performance/strategies")
            if perf_strategies_payload is not None:
                rows = perf_strategies_payload.get("strategies") or []
                if rows:
                    st.dataframe(pd.DataFrame(rows[:60]), use_container_width=True, hide_index=True)
    else:
        show_locked("Portfolio Allocation Intelligence", "Institutional")
        show_locked("Portfolio Brain", "Institutional")
        show_locked("Cluster / Fleet Summary", "Institutional")
        show_locked("Advanced Strategy Insights", "Institutional")

    st.caption(f"Entitlements applied. Plan={effective_plan}. Role={role}.")
    st.stop()


bots_payload, bots_err = fetch_json("/bots")
control_payload, _ = fetch_json("/control/signals")
fund_payload, _ = fetch_json("/fund/status")
factory_top_payload, _ = fetch_json("/factory/top?limit=5")
paper_payload, _ = fetch_json("/paper/status")
feedback_payload, _ = fetch_json("/paper/feedback")
auto_payload, _ = fetch_json("/auto/status")
autonomous_payload, autonomous_err = fetch_json("/autonomous/status")
system_health_payload, system_health_err = fetch_json("/system/health")
system_errors_payload, system_errors_err = fetch_json("/system/errors")
cluster_status_payload, cluster_status_err = fetch_json("/cluster/status")
cluster_runners_payload, cluster_runners_err = fetch_json("/cluster/runners")
live_safe_payload, _ = fetch_json("/live_safe/status")
portfolio_payload, _ = fetch_json("/portfolio/allocation")
meta_payload, meta_err = fetch_json("/meta/status")
meta_recommendations_payload, meta_recs_err = fetch_json("/meta/recommendations")
meta_learning_status_payload, meta_learning_status_err = fetch_json("/meta/learning/status")
meta_learning_insights_payload, meta_learning_insights_err = fetch_json("/meta/learning/insights")
report_payload, _ = fetch_json("/report/summary")
capital_payload, _ = fetch_json("/capital/status")
review_status_payload, _ = fetch_json("/review/status")
review_candidates_payload, _ = fetch_json("/review/candidates?limit=10")
demo_status_payload, _ = fetch_json("/demo/status")
demo_candidates_payload, _ = fetch_json("/demo/candidates?limit=10")
demo_playbook_payload, demo_playbook_err = fetch_json("/demo/playbook/status")
demo_playbook_checks_payload, demo_playbook_checks_err = fetch_json("/demo/playbook/checks")
executor_status_payload, _ = fetch_json("/executor/status")
executor_candidates_payload, _ = fetch_json("/executor/candidates?limit=10")
runner_status_payload, _ = fetch_json("/runner/status")
runner_jobs_payload, _ = fetch_json("/runner/jobs?limit=10")
multi_runner_status_payload, multi_runner_err = fetch_json("/multi_runner/status")
multi_runner_jobs_payload, multi_runner_jobs_err = fetch_json("/multi_runner/jobs?limit=200")
operator_payload, operator_err = fetch_json("/operator/status")
brain_payload, brain_err = fetch_json("/brain")
alerts_payload, alerts_err = fetch_json("/alerts")
alerts_summary_payload, _ = fetch_json("/alerts/summary")
global_risk_payload, global_risk_err = fetch_json("/risk/status")
global_risk_alerts_payload, global_risk_alerts_err = fetch_json("/risk/alerts")
regime_status_payload, regime_status_err = fetch_json("/regime/status")
regime_recommendations_payload, regime_recommendations_err = fetch_json("/regime/recommendations")
live_data_status_payload, live_data_status_err = fetch_json("/live/status")
live_data_market_payload, live_data_market_err = fetch_json("/live/market")
memory_status_payload, memory_status_err = fetch_json("/memory/status")
memory_strategy_payload, memory_strategy_err = fetch_json("/memory/strategy")
memory_family_payload, memory_family_err = fetch_json("/memory/family")
memory_regime_payload, memory_regime_err = fetch_json("/memory/regime")
recovery_payload, recovery_err = fetch_json("/recovery/status")
perf_system_payload, perf_sys_err = fetch_json("/performance/system")
perf_top_payload, _ = fetch_json("/performance/top")
perf_strategies_payload, perf_str_err = fetch_json("/performance/strategies")
evolution_candidates_payload, evolution_err = fetch_json("/evolution/candidates")
evolution_lineage_payload, evolution_lineage_err = fetch_json("/evolution/lineage")
evolution_loop_payload, evolution_loop_err = fetch_json("/evolution_loop/status")
promotion_payload, promotion_err = fetch_json("/promotion/candidates")
fund_alloc_payload, fund_alloc_err = fetch_json("/fund/allocation/status")
fund_portfolio_payload, fund_pf_err = fetch_json("/fund/portfolio")

if operator_payload is None:
    st.error(
        "Could not load `/operator/status`. Cockpit requires backend health. "
        f"Error: {operator_err or 'unknown'}"
    )
    st.stop()

if brain_payload is not None:
    st.info(f"Brain Regime: {brain_payload.get('regime', 'NEUTRAL')} | {brain_payload.get('message', '')}")
elif brain_err:
    st.warning(f"Brain endpoint unavailable: {brain_err}")

pipeline = operator_payload.get("pipeline", {})
capital = operator_payload.get("capital", {})
risk_flags = operator_payload.get("risk_flags", [])
report_warnings = (report_payload or {}).get("warnings", [])
combined_warnings = [str(x) for x in (risk_flags + report_warnings)]

st.subheader("System Overview")
top = st.columns(10)
top[0].metric("system_health", str(operator_payload.get("system_health", "WARNING")))
top[1].metric("portfolio_state", str((report_payload or {}).get("portfolio_state", "NORMAL")))
top[2].metric(
    "recommended_portfolio_action",
    str((report_payload or {}).get("recommended_portfolio_action", "KEEP_RUNNING")),
)
top[3].metric("total_candidates", int(pipeline.get("total_candidates", 0)))
top[4].metric("paper_success", int(pipeline.get("paper_success", 0)))
top[5].metric("live_safe_ready", int(pipeline.get("live_safe_ready", 0)))
top[6].metric("demo_queued", int(pipeline.get("demo_queued", 0)))
top[7].metric("runner_active", int((runner_status_payload or {}).get("counts", {}).get("RUNNER_ACTIVE", 0)))
top[8].metric("total_capital", f"{float(capital.get('total_capital', 0.0)):.2f}")
top[9].metric("risk_usage", f"{float(capital.get('risk_usage', 0.0)):.4f}")

if combined_warnings:
    st.error("Critical Warnings:\n- " + "\n- ".join(dict.fromkeys(combined_warnings)))
else:
    st.success("No active cockpit warnings.")

st.divider()
st.subheader("Meta AI Control Engine")
st.caption(
    "Orchestrates Global Risk, Market Regime, Portfolio Brain, Long-Term Memory, Performance, "
    "Multi-Runner, Fund, and evolution signals. Decision layer only — no trading or deployment."
)
if meta_payload is not None:
    mctrl1, mctrl2, mctrl3 = st.columns(3)
    mctrl1.metric("system_posture", str(meta_payload.get("system_posture", "—")))
    mctrl2.metric("confidence", f"{float(meta_payload.get('confidence', 0.0)):.3f}")
    mctrl3.caption(
        f"decision_layer_only={meta_payload.get('decision_layer_only')} | "
        f"demo_simulation_only={meta_payload.get('demo_simulation_only')}"
    )
    reasons = meta_payload.get("reasoning") or []
    if reasons:
        st.markdown("**Reasoning**")
        for line in reasons:
            st.info(str(line))
    recs = meta_payload.get("recommendations") or []
    if recs:
        st.markdown("**Recommendations**")
        for r in recs:
            st.write(f"- {r}")
    diag = meta_payload.get("diagnostics") or {}
    if diag:
        st.markdown("**Diagnostics**")
        with st.expander("risk / regime / memory / performance / runner / capital / portfolio_brain"):
            st.json(diag)
elif meta_err:
    st.warning(f"Meta AI Control status unavailable: {meta_err}")

st.divider()
st.subheader("Live Data Engine")
st.caption(
    "Read-only market ingestion (public APIs): snapshots, volatility/trend signals, and health. "
    "No trading or broker execution."
)
if live_data_status_payload is not None:
    ld1, ld2, ld3, ld4 = st.columns(4)
    ld1.metric("data_health", str(live_data_status_payload.get("data_health", "—")))
    lu = live_data_status_payload.get("last_update") or "—"
    ld2.metric("last_update", str(lu)[:19] if lu != "—" else "—")
    src = live_data_status_payload.get("sources") or []
    ld3.metric("sources_active", len(src))
    ld4.metric("symbols", len(live_data_status_payload.get("symbols_tracked") or []))
    if src:
        st.caption("Sources: " + ", ".join(str(s) for s in src))
    rows = (live_data_market_payload or {}).get("symbols") or []
    if rows:
        st.markdown("**Symbols**")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No symbol rows yet (waiting for first successful refresh or offline).")
elif live_data_status_err:
    st.warning(f"Live Data Engine unavailable: {live_data_status_err}")

if meta_recommendations_payload is not None:
    with st.expander("GET /meta/recommendations (duplicate of recommendations list)"):
        st.json(meta_recommendations_payload)
elif meta_recs_err:
    st.caption(f"/meta/recommendations unavailable: {meta_recs_err}")

st.divider()
st.subheader("Self Improving Meta AI")
st.caption(
    "Meta orchestration learning from posture, performance, risk, regime, memory, and portfolio outcomes. "
    "Self-learning advisory only."
)
if meta_learning_status_payload is not None:
    mls = meta_learning_status_payload
    l1, l2, l3 = st.columns(3)
    l1.metric("learning_entries", int(mls.get("learning_entries", 0)))
    l2.metric("learning_health", str(mls.get("learning_health", "—")))
    l3.metric("last_update", str(mls.get("last_update") or "—")[:19])
    bp = mls.get("best_postures") or []
    if bp:
        st.markdown("**Best postures (avg outcome)**")
        st.dataframe(pd.DataFrame(bp), use_container_width=True, hide_index=True)
elif meta_learning_status_err:
    st.warning(f"Meta learning status unavailable: {meta_learning_status_err}")

if meta_learning_insights_payload is not None:
    mli = meta_learning_insights_payload
    tp = mli.get("top_patterns") or []
    if tp:
        st.markdown("**Learning insights**")
        for line in tp:
            st.success(str(line))
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Best posture by regime**")
        st.json(mli.get("best_posture_by_regime") or {})
    with c2:
        st.markdown("**Risk posture patterns**")
        st.json(mli.get("risk_posture_patterns") or {})
    st.markdown("**Confidence adjustments**")
    st.json(mli.get("confidence_adjustments") or {})
elif meta_learning_insights_err:
    st.caption(f"Meta learning insights unavailable: {meta_learning_insights_err}")

if st.button("Run meta learning update (POST /meta/learning/update)"):
    try:
        r = requests.post(f"{api_url}/meta/learning/update", timeout=60)
        r.raise_for_status()
        st.success("meta learning updated")
        st.json(r.json())
    except requests.RequestException as exc:
        st.error(str(exc))

st.divider()
st.subheader("Production Hardening")
st.caption(
    "Stability layer: health monitoring, error tracking, snapshots, startup restore, and safe restart checks."
)
if system_health_payload is not None:
    sh = system_health_payload
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("system_health", str(sh.get("system_health", "—")))
    h2.metric("uptime_s", f"{float(sh.get('uptime', 0.0)):.0f}")
    h3.metric("memory_usage_mb", str(sh.get("memory_usage")))
    h4.metric("cpu_usage_pct", str(sh.get("cpu_usage")))
    eng = sh.get("engines") or {}
    if eng:
        st.markdown("**Engine health**")
        st.dataframe(
            pd.DataFrame([{"engine": k, "health": v} for k, v in eng.items()]),
            use_container_width=True,
            hide_index=True,
        )
    le = sh.get("last_errors") or []
    if le:
        st.markdown("**Last errors**")
        st.dataframe(pd.DataFrame(le), use_container_width=True, hide_index=True)
elif system_health_err:
    st.warning(f"System health unavailable: {system_health_err}")

if system_errors_payload is not None:
    se = system_errors_payload
    st.caption(
        f"errors={int(se.get('error_count', 0))} | last_error={str((se.get('last_error') or {}).get('message', '—'))[:120]}"
    )
    rec = se.get("recent_errors") or []
    if rec:
        with st.expander("Recent errors"):
            st.dataframe(pd.DataFrame(rec), use_container_width=True, hide_index=True)
elif system_errors_err:
    st.caption(f"System errors unavailable: {system_errors_err}")

if st.button("Create system snapshot (POST /system/snapshot)"):
    try:
        r = requests.post(f"{api_url}/system/snapshot", timeout=60)
        r.raise_for_status()
        st.success("snapshot created")
        st.json(r.json())
    except requests.RequestException as exc:
        st.error(str(exc))

st.divider()
st.subheader("Alerting Engine")
if alerts_summary_payload is not None:
    ac1, ac2, ac3, ac4 = st.columns(4)
    ac1.metric("Active alerts", int(alerts_summary_payload.get("total_alerts", 0)))
    ac2.metric(
        "CRITICAL",
        int(alerts_summary_payload.get("critical_count", 0)),
        help="Requires immediate operator attention.",
    )
    ac3.metric("WARNING", int(alerts_summary_payload.get("warning_count", 0)))
    ac4.metric("INFO", int(alerts_summary_payload.get("info_count", 0)))
    crit_n = int(alerts_summary_payload.get("critical_count", 0))
    if crit_n > 0:
        st.error(
            f"**{crit_n} CRITICAL alert(s)** — review titles and recommended actions below before proceeding."
        )
    top_alerts = alerts_summary_payload.get("top_active_alerts") or []
    if top_alerts:
        st.markdown("**Most important operator actions**")
        for item in top_alerts[:8]:
            sev = str(item.get("severity", ""))
            title = str(item.get("title", ""))
            action = str(item.get("recommended_operator_action", ""))
            line = f"**[{sev}]** {title} — {action}"
            if sev == "CRITICAL":
                st.error(line)
            elif sev == "WARNING":
                st.warning(line)
            else:
                st.info(line)
elif alerts_err:
    st.caption(f"Alerting summary unavailable: {alerts_err}")

if alerts_payload is not None:
    active_only = [a for a in alerts_payload.get("alerts", []) if a.get("active", True)]
    st.caption(
        f"API active count: {int(alerts_payload.get('count', 0))} | "
        f"Showing {len(active_only)} active row(s) in table."
    )
    if active_only:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "severity": x.get("severity"),
                        "category": x.get("category"),
                        "title": x.get("title"),
                        "source": x.get("source"),
                        "recommended_operator_action": x.get("recommended_operator_action"),
                        "message": x.get("message"),
                    }
                    for x in active_only
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No active alerts.")
elif alerts_err:
    st.warning(f"Alerts list unavailable: {alerts_err}")

st.divider()
st.subheader("Global Risk Engine")
st.caption(
    "Portfolio-level risk view from portfolio brain, fund sim, performance, multi-runner fleet, "
    "recovery, capital, and review backlog. Decision layer only — no trading or deployment."
)
if global_risk_payload is not None:
    gr = global_risk_payload
    g1, g2, g3 = st.columns(3)
    g1.metric("global_risk_score", f"{float(gr.get('global_risk_score', 0.0)):.3f}")
    g2.metric("risk_level", str(gr.get("risk_level", "—")))
    comps = gr.get("components") or {}
    g3.caption(
        f"conc {float(comps.get('concentration_risk', 0)):.2f} | "
        f"corr {float(comps.get('correlation_risk', 0)):.2f} | "
        f"dd {float(comps.get('drawdown_risk', 0)):.2f}"
    )
    if comps:
        st.markdown("**Risk components**")
        st.dataframe(
            pd.DataFrame([{"component": k, "value": v} for k, v in comps.items()]),
            use_container_width=True,
            hide_index=True,
        )
    recs = gr.get("recommendations") or []
    if recs:
        st.markdown("**Recommendations**")
        for line in recs:
            st.info(str(line))
elif global_risk_err:
    st.warning(f"Global risk status unavailable: {global_risk_err}")

if global_risk_alerts_payload is not None:
    ral = global_risk_alerts_payload.get("alerts") or []
    st.markdown("**Global risk alerts**")
    if ral:
        st.dataframe(pd.DataFrame(ral), use_container_width=True, hide_index=True)
    else:
        st.caption("No component-level risk alerts.")
elif global_risk_alerts_err:
    st.caption(f"Risk alerts unavailable: {global_risk_alerts_err}")

st.divider()
st.subheader("Market Regime Engine")
st.caption(
    "Read-only regime view from performance, portfolio brain, fund sim, paper outcomes, global risk, "
    "and meta diagnostics. Decision layer only — no execution or allocation changes."
)
if regime_status_payload is not None:
    rs = regime_status_payload
    m1, m2, m3 = st.columns(3)
    m1.metric("current_regime", str(rs.get("current_regime", "—")))
    m2.metric("confidence_score", f"{float(rs.get('confidence_score', 0.0)):.3f}")
    m3.caption(
        f"decision_layer_only={rs.get('decision_layer_only')} | "
        f"demo_simulation_only={rs.get('demo_simulation_only')}"
    )
    st.markdown("**Favored families**")
    st.write(", ".join(rs.get("favored_strategy_families") or []) or "—")
    st.markdown("**Reduced families**")
    st.write(", ".join(rs.get("reduced_strategy_families") or []) or "—")
    st.markdown("**Paused families**")
    st.write(", ".join(rs.get("paused_strategy_families") or []) or "—")
    reasons = rs.get("regime_reasoning") or []
    if reasons:
        st.markdown("**Regime reasoning**")
        for line in reasons:
            st.info(str(line))
elif regime_status_err:
    st.warning(f"Market regime status unavailable: {regime_status_err}")

if regime_recommendations_payload is not None:
    rrows = regime_recommendations_payload.get("recommendations") or []
    st.markdown("**Regime recommendations**")
    if rrows:
        st.dataframe(
            pd.DataFrame(
                [{"family": x.get("family"), "action": x.get("action"), "reason": x.get("reason")} for x in rrows]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("No family-level actions.")
elif regime_recommendations_err:
    st.caption(f"Regime recommendations unavailable: {regime_recommendations_err}")

st.divider()
st.subheader("Long Term Memory Engine")
st.caption(
    "Persistent learning memory from performance, regime, risk, evolution, and portfolio signals. "
    "Learning and recall only — no trading, deployment, or execution changes."
)
if memory_status_payload is not None:
    ms = memory_status_payload
    mm1, mm2, mm3 = st.columns(3)
    mm1.metric("memory_entries", int(ms.get("memory_entries", 0)))
    mm2.metric("memory_health", str(ms.get("memory_health", "—")))
    mm3.metric("update_count", int(ms.get("update_count", 0)))
    st.caption(f"last_update: {ms.get('last_update') or '—'}")
    insights = ms.get("learning_insights") or []
    if insights:
        st.markdown("**Learning insights**")
        for line in insights:
            st.success(str(line))
    evo = ms.get("evolution_memory") or {}
    risk = ms.get("risk_memory_summary") or {}
    st.caption(
        f"Evolution memory: snapshots={evo.get('total_snapshots', 0)} | "
        f"Risk memory: samples={risk.get('samples', 0)} "
        f"avg_global_risk={risk.get('avg_global_risk_score', '—')}"
    )
elif memory_status_err:
    st.warning(f"Memory status unavailable: {memory_status_err}")

if memory_strategy_payload is not None:
    st.markdown("**Strategy memory**")
    rows = memory_strategy_payload.get("strategies") or []
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No strategy memory yet — POST /memory/update to record snapshots.")
elif memory_strategy_err:
    st.caption(f"Strategy memory unavailable: {memory_strategy_err}")

if memory_family_payload is not None:
    st.markdown("**Family memory**")
    frows = memory_family_payload.get("families") or []
    if frows:
        st.dataframe(pd.DataFrame(frows), use_container_width=True, hide_index=True)
elif memory_family_err:
    st.caption(f"Family memory unavailable: {memory_family_err}")

if memory_regime_payload is not None:
    st.markdown("**Regime memory**")
    rrows = memory_regime_payload.get("regimes") or []
    if rrows:
        st.dataframe(pd.DataFrame(rrows), use_container_width=True, hide_index=True)
elif memory_regime_err:
    st.caption(f"Regime memory unavailable: {memory_regime_err}")

if st.button("Run memory update (POST /memory/update)"):
    try:
        post_m = requests.post(f"{api_url}/memory/update", timeout=120)
        post_m.raise_for_status()
        mud = post_m.json()
        st.success(
            f"ok={mud.get('ok')} memory_entries={mud.get('memory_entries')} "
            f"health={mud.get('memory_health')}"
        )
        with st.expander("Last memory update detail"):
            st.json(mud)
    except requests.RequestException as exc:
        st.error(str(exc))

st.divider()
st.subheader("Auto Recovery Engine")
st.caption("Safe orchestration only: demo runner, paper, factory — no broker or capital deployment.")
if recovery_payload is not None:
    r1, r2, r3 = st.columns(3)
    r1.metric("recovery_state", str(recovery_payload.get("recovery_state", "RECOVERY_IDLE")))
    r2.metric("active_flag", str(recovery_payload.get("active", False)))
    r3.metric(
        "history_entries",
        len(recovery_payload.get("recovery_history") or []),
    )
    st.write("**last_action:**", recovery_payload.get("last_action") or "—")
    st.write("**last_result:**", recovery_payload.get("last_result") or "—")
    st.caption(f"last_recovery: {recovery_payload.get('last_recovery') or '—'}")
    hist = recovery_payload.get("recovery_history") or []
    if hist:
        st.markdown("**recovery_history**")
        st.dataframe(pd.DataFrame(hist), use_container_width=True, hide_index=True)
    if st.button("Run safe recovery (POST /recovery/run)"):
        try:
            post_r = requests.post(f"{api_url}/recovery/run", timeout=120)
            post_r.raise_for_status()
            data = post_r.json()
            st.success(f"run_ok={data.get('run_ok')}")
            with st.expander("Last recovery run detail"):
                st.json(data)
        except requests.RequestException as exc:
            st.error(str(exc))
elif recovery_err:
    st.warning(f"Recovery status unavailable: {recovery_err}")

st.divider()
st.subheader("Performance Engine")
st.caption("Analytics only — derived from runner/paper logs, executor snapshot, operator pipeline, recovery history.")
if perf_system_payload is not None:
    ps = perf_system_payload
    pm1, pm2, pm3, pm4, pm5 = st.columns(5)
    pm1.metric("runner_success_rate", f"{float(ps.get('runner_success_rate', 0.0)):.2%}")
    pm2.metric("runner_fail_rate", f"{float(ps.get('runner_fail_rate', 0.0)):.2%}")
    pm3.metric("avg_runner_duration_s", f"{float(ps.get('avg_runner_duration', 0.0)):.2f}")
    pm4.metric("pipeline_throughput", f"{float(ps.get('pipeline_throughput', 0.0)):.2f}")
    pm5.metric("recovery_rate", f"{float(ps.get('recovery_rate', 0.0)):.2%}")
    st.caption(
        f"total_jobs (runner log): {int(ps.get('total_jobs', 0))} | "
        f"executor_ready: {int(ps.get('executor_ready_count', 0))} | "
        f"executor_running: {int(ps.get('executor_running_count', 0))}"
    )
    trends = ps.get("performance_trends") or []
    if trends:
        st.markdown("**Performance trends** (daily runs / successes from log)")
        tdf = pd.DataFrame(trends)
        if "date" in tdf.columns and not tdf.empty:
            st.line_chart(tdf.set_index("date")[["runs", "successes"]])
else:
    st.caption(f"System performance unavailable: {perf_sys_err or 'unknown'}")

if perf_top_payload is not None:
    top_perf = perf_top_payload.get("strategies") or []
    st.markdown("**Top strategies (by performance_score)**")
    if top_perf:
        st.dataframe(pd.DataFrame(top_perf), use_container_width=True, hide_index=True)
    else:
        st.caption("No scored strategies yet — complete runner jobs or apply paper feedback to build history.")

if perf_strategies_payload is not None:
    with st.expander("All strategies performance (sample)"):
        all_rows = perf_strategies_payload.get("strategies") or []
        st.caption(f"Total strategies in response: {int(perf_strategies_payload.get('count', 0))}")
        if all_rows:
            st.dataframe(pd.DataFrame(all_rows[:40]), use_container_width=True, hide_index=True)
elif perf_str_err:
    st.caption(f"Strategies performance unavailable: {perf_str_err}")

st.divider()
st.subheader("AI Strategy Evolution")
st.caption(
    "Performance-classified weak/strong strategies; POST /evolution/run creates new factory variants only "
    "(parents unchanged). No execution or capital deployment."
)
if evolution_candidates_payload is not None:
    weak = evolution_candidates_payload.get("weak_strategies") or []
    strong = evolution_candidates_payload.get("strong_strategies") or []
    e1, e2, e3 = st.columns(3)
    e1.metric("weak candidates", len(weak))
    e2.metric("strong candidates", len(strong))
    th = evolution_candidates_payload.get("thresholds") or {}
    e3.caption(
        f"weak: score<{th.get('weak_max_performance_score', '?')} & "
        f"runs≥{th.get('weak_min_total_runs', '?')} | "
        f"strong: score>{th.get('strong_min_performance_score', '?')} & "
        f"success>{th.get('strong_min_success_rate', '?')}"
    )
    if weak:
        st.markdown("**Weak strategies (evolution targets)**")
        st.dataframe(pd.DataFrame(weak), use_container_width=True, hide_index=True)
    if strong:
        st.markdown("**Strong strategies (clone / fine-tune targets)**")
        st.dataframe(pd.DataFrame(strong), use_container_width=True, hide_index=True)
    if st.button("Run AI evolution (POST /evolution/run)"):
        try:
            post_e = requests.post(
                f"{api_url}/evolution/run",
                params={"max_weak": 5, "max_strong": 5},
                timeout=120,
            )
            post_e.raise_for_status()
            er = post_e.json()
            st.success(
                f"created={len(er.get('created_variants', []))} "
                f"skipped={len(er.get('skipped', []))}"
            )
            with st.expander("Last evolution run detail"):
                st.json(er)
        except requests.RequestException as exc:
            st.error(str(exc))
elif evolution_err:
    st.warning(f"Evolution candidates unavailable: {evolution_err}")

if evolution_lineage_payload is not None:
    lineage = evolution_lineage_payload.get("lineage") or []
    recent = sorted(
        lineage,
        key=lambda x: str(x.get("created_at") or ""),
        reverse=True,
    )
    st.markdown("**Evolution lineage** (parent → child)")
    if lineage:
        st.dataframe(pd.DataFrame(lineage), use_container_width=True, hide_index=True)
    else:
        st.caption("No parent/child edges yet — run evolution or use factory evolve.")
    st.markdown("**Recent evolved variants** (by created_at)")
    if recent:
        st.dataframe(pd.DataFrame(recent[:20]), use_container_width=True, hide_index=True)
    else:
        st.caption("No variants recorded.")
elif evolution_lineage_err:
    st.caption(f"Evolution lineage unavailable: {evolution_lineage_err}")

st.divider()
st.subheader("Continuous Evolution Engine")
st.caption(
    "Orchestrates AI evolution → factory inserts → paper simulation → feedback → performance snapshot on a timer. "
    "No broker, trading, or capital deployment."
)
if evolution_loop_payload is not None:
    el = evolution_loop_payload
    l1, l2, l3, l4, l5 = st.columns(5)
    l1.metric("state", str(el.get("state", "—")))
    l2.metric("loops_completed", int(el.get("loops_completed", 0)))
    l3.metric("interval_s", int(el.get("interval_sec", 0)))
    l4.metric("max/hour", int(el.get("max_loops_per_hour", 0)))
    l5.metric("last_cycle", str(el.get("last_cycle_at") or "—")[:19])
    if el.get("last_error"):
        st.warning(f"Last error: {el.get('last_error')}")
    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("Start loop (POST /evolution_loop/start)"):
            try:
                r = requests.post(
                    f"{api_url}/evolution_loop/start",
                    params={
                        "interval_sec": 120,
                        "max_loops_per_hour": 12,
                        "max_weak": 5,
                        "max_strong": 5,
                    },
                    timeout=30,
                )
                r.raise_for_status()
                st.success(str(r.json().get("state", "")))
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    with b2:
        if st.button("Pause loop (POST /evolution_loop/pause)"):
            try:
                r = requests.post(f"{api_url}/evolution_loop/pause", timeout=30)
                r.raise_for_status()
                st.success("paused")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    with b3:
        if st.button("Run once (POST /evolution_loop/run_once)"):
            try:
                r = requests.post(f"{api_url}/evolution_loop/run_once", timeout=120)
                r.raise_for_status()
                st.success("cycle completed")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    last = el.get("last_cycle_result")
    if last:
        with st.expander("Last cycle result (summary)"):
            st.json(last)
elif evolution_loop_err:
    st.warning(f"Evolution loop status unavailable: {evolution_loop_err}")

st.divider()
st.subheader("Full Autonomous Fund Mode")
st.caption(
    "Meta AI controlled research loop: regime -> risk -> memory -> evolution -> paper -> performance -> "
    "portfolio/fund/multi-runner -> meta decision -> repeat. Orchestration only."
)
if autonomous_payload is not None:
    au = autonomous_payload
    a1, a2, a3, a4, a5, a6 = st.columns(6)
    a1.metric("state", str(au.get("state", "—")))
    a2.metric("loops_completed", int(au.get("loops_completed", 0)))
    a3.metric("posture", str(au.get("posture") or "—"))
    a4.metric("confidence", f"{float(au.get('confidence', 0.0)):.3f}")
    a5.metric("interval_s", int(au.get("interval_sec", 0)))
    a6.metric("max/hour", int(au.get("max_loops_per_hour", 0)))
    st.write("**last_loop_at:**", au.get("last_loop_at") or "—")
    st.write("**last_decision:**", au.get("last_decision") or "—")
    errs = au.get("errors") or []
    if errs:
        st.warning("Recent errors:\n- " + "\n- ".join(str(x) for x in errs[-5:]))
    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("Start autonomous (POST /autonomous/start)"):
            try:
                r = requests.post(
                    f"{api_url}/autonomous/start",
                    params={"interval_sec": 120, "max_loops_per_hour": 20},
                    timeout=30,
                )
                r.raise_for_status()
                st.success("autonomous started")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    with b2:
        if st.button("Pause autonomous (POST /autonomous/pause)"):
            try:
                r = requests.post(f"{api_url}/autonomous/pause", timeout=30)
                r.raise_for_status()
                st.success("autonomous paused")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    with b3:
        if st.button("Run once autonomous (POST /autonomous/run_once)"):
            try:
                r = requests.post(f"{api_url}/autonomous/run_once", timeout=180)
                r.raise_for_status()
                st.success("autonomous cycle completed")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
elif autonomous_err:
    st.warning(f"Autonomous mode status unavailable: {autonomous_err}")

st.divider()
st.subheader("Distributed Runner Cluster")
st.caption(
    "Cluster coordination for distributed autonomous runners: registration, heartbeat, load balancing, "
    "offline detection, and failover reassignment (simulation only)."
)
if cluster_status_payload is not None:
    cs = cluster_status_payload
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("cluster_health", str(cs.get("cluster_health", "—")))
    c2.metric("runner_count", int(cs.get("runner_count", 0)))
    c3.metric("healthy_runners", int(cs.get("healthy_runners", 0)))
    c4.metric("offline_runners", int(cs.get("offline_runners", 0)))
    c5.metric("total_capacity", int(cs.get("total_capacity", 0)))
    if int(cs.get("offline_runners", 0)) > 0:
        st.warning("Offline runners detected — failover reassignment is recommended.")
elif cluster_status_err:
    st.warning(f"Cluster status unavailable: {cluster_status_err}")

if cluster_runners_payload is not None:
    rows = cluster_runners_payload.get("runners") or []
    st.markdown("**Runner list / load distribution**")
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No distributed runners registered yet.")
elif cluster_runners_err:
    st.caption(f"Cluster runners unavailable: {cluster_runners_err}")

st.divider()
st.subheader("FP Markets Demo Playbook")
st.caption(
    "Safety-first phased rollout on FP Markets cTrader demo accounts only. "
    "No live trading, no real capital."
)
if demo_playbook_payload is not None:
    dp = demo_playbook_payload
    p1, p2, p3 = st.columns(3)
    p1.metric("phase", str(dp.get("phase", "—")))
    p2.metric("state", str(dp.get("state", "—")))
    p3.metric("readiness", str(dp.get("readiness", "—")))
    st.markdown("**Checks**")
    st.json(dp.get("checks") or {})
    blk = dp.get("blocking_conditions") or []
    if blk:
        st.warning("Blocking conditions:\n- " + "\n- ".join(str(x) for x in blk))
    recs = dp.get("recommendations") or []
    if recs:
        st.markdown("**Recommendations**")
        for r in recs:
            st.write(f"- {r}")
    pb1, pb2, pb3 = st.columns(3)
    with pb1:
        if st.button("Start playbook (POST /demo/playbook/start)"):
            try:
                r = requests.post(f"{api_url}/demo/playbook/start", timeout=30)
                r.raise_for_status()
                st.success("playbook started")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
    with pb2:
        if st.button("Advance phase (POST /demo/playbook/next)"):
            try:
                r = requests.post(f"{api_url}/demo/playbook/next", timeout=30)
                r.raise_for_status()
                data = r.json()
                if data.get("advanced"):
                    st.success("playbook advanced")
                else:
                    st.warning(f"playbook blocked: {data.get('reason')}")
                st.json(data)
            except requests.RequestException as exc:
                st.error(str(exc))
    with pb3:
        if st.button("Reset playbook (POST /demo/playbook/reset)"):
            try:
                r = requests.post(f"{api_url}/demo/playbook/reset", timeout=30)
                r.raise_for_status()
                st.success("playbook reset")
                st.json(r.json())
            except requests.RequestException as exc:
                st.error(str(exc))
elif demo_playbook_err:
    st.warning(f"Demo playbook unavailable: {demo_playbook_err}")

if demo_playbook_checks_payload is not None:
    st.markdown("**Phase Gate Checklist**")
    dpc = demo_playbook_checks_payload
    c1, c2 = st.columns(2)
    c1.metric("readiness", str(dpc.get("readiness", "—")))
    c2.metric("ready_to_advance", str(bool(dpc.get("ready_to_advance", False))))
    st.json(dpc.get("checks") or {})
    b = dpc.get("blocking_conditions") or []
    if b:
        st.warning("Checklist blocking:\n- " + "\n- ".join(str(x) for x in b))
elif demo_playbook_checks_err:
    st.caption(f"Phase gate checklist unavailable: {demo_playbook_checks_err}")

st.divider()
st.subheader("Smart Promotion Engine")
st.caption(
    "Decision layer: review priority / approve-for-demo / executor prepare / runner recommendation only. "
    "No demo queue auto-deploy, no runner ack/start, no capital."
)
if promotion_payload is not None:
    th = promotion_payload.get("thresholds") or {}
    st.markdown("**Promotion thresholds**")
    st.json(th)
    rn = promotion_payload.get("runner_candidates") or []
    ex = promotion_payload.get("executor_candidates") or []
    st.markdown("**Top promotion candidates (runner tier, then executor)**")
    show_rows = (rn or ex)[:12]
    if show_rows:
        st.dataframe(pd.DataFrame(show_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No strategies meet promotion thresholds yet.")
    st.markdown("**Promotion actions**")
    if st.button("Run smart promotion (POST /promotion/run)"):
        try:
            post_p = requests.post(f"{api_url}/promotion/run", timeout=120)
            post_p.raise_for_status()
            pr = post_p.json()
            st.success(
                f"promoted={len(pr.get('promoted', []))} skipped={len(pr.get('skipped', []))}"
            )
            with st.expander("Last promotion run detail"):
                st.json(pr)
        except requests.RequestException as exc:
            st.error(str(exc))
    histp = promotion_payload.get("recent_promotion_history") or []
    if histp:
        st.markdown("**Promotion history**")
        st.dataframe(pd.DataFrame(histp), use_container_width=True, hide_index=True)
elif promotion_err:
    st.warning(f"Promotion candidates unavailable: {promotion_err}")

st.divider()
st.subheader("Fund Engine (simulation)")
st.caption(
    "Simulated allocation from factory strategy performance only. "
    "GET /fund/status remains bot-level fund mode; allocation uses GET /fund/allocation/status."
)
if fund_alloc_payload is not None:
    fa = fund_alloc_payload
    f1, f2, f3, f4, f5, f6 = st.columns(6)
    f1.metric("total_capital", f"{float(fa.get('total_capital', 0)):,.0f}")
    f2.metric("allocated", f"{float(fa.get('allocated_capital', 0)):,.0f}")
    f3.metric("free", f"{float(fa.get('free_capital', 0)):,.0f}")
    f4.metric("portfolio_return (sim)", f"{float(fa.get('portfolio_return', 0)):.4f}")
    f5.metric("risk_score", f"{float(fa.get('risk_score', 0)):.4f}")
    f6.metric("drawdown (sim)", f"{float(fa.get('drawdown', 0)):.4f}")
    if fund_portfolio_payload is not None:
        st.markdown("**Portfolio distribution (top allocated)**")
        strat_rows = fund_portfolio_payload.get("strategies") or []
        if strat_rows:
            st.dataframe(pd.DataFrame(strat_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("No strategies above performance_score 0.70 — nothing allocated.")
        rh = fund_portfolio_payload.get("rebalance_history") or []
        if rh:
            st.markdown("**Rebalance history**")
            st.dataframe(pd.DataFrame(rh), use_container_width=True, hide_index=True)
    if st.button("Simulate rebalance (POST /fund/rebalance)"):
        try:
            post_f = requests.post(f"{api_url}/fund/rebalance", timeout=60)
            post_f.raise_for_status()
            fr = post_f.json()
            st.success(f"rebalanced_at={fr.get('rebalanced_at')}")
            with st.expander("Rebalance detail"):
                st.json(fr)
        except requests.RequestException as exc:
            st.error(str(exc))
elif fund_alloc_err:
    st.warning(f"Fund allocation status unavailable: {fund_alloc_err}")

st.divider()

left, right = st.columns([1, 1])
with left:
    st.subheader("Pipeline Overview")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Paper Running", int(pipeline.get("paper_running", 0)))
    p2.metric("Review Pending", int(pipeline.get("review_pending", 0)))
    p3.metric("Demo Running", int(pipeline.get("demo_running", 0)))
    p4.metric("Loops Completed", int(operator_payload.get("loops_completed", 0)))
    st.caption(f"Last cycle: {operator_payload.get('last_cycle_at')}")

    if factory_top_payload is not None:
        st.markdown("**Top Strategies**")
        top_items = factory_top_payload.get("top", [])
        if top_items:
            st.dataframe(pd.DataFrame(top_items), use_container_width=True, hide_index=True)
        else:
            st.caption("No top strategies available.")

with right:
    st.subheader("Risk & Capital")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", f"{float(capital.get('total_capital', 0.0)):.2f}")
    c2.metric("Allocated", f"{float(capital.get('allocated', 0.0)):.2f}")
    c3.metric("Free", f"{float(capital.get('free', 0.0)):.2f}")
    c4.metric("Risk Usage", f"{float(capital.get('risk_usage', 0.0)):.4f}")
    c5.metric("Growth Rate", f"{float(capital.get('growth_rate', 0.0)):.4f}")
    if meta_payload is not None:
        st.caption(
            f"Meta AI: posture={meta_payload.get('system_posture', '—')} | "
            f"confidence={float(meta_payload.get('confidence', 0.0)):.3f}"
        )

st.divider()

st.subheader("Portfolio & Allocation (AI Portfolio Brain)")
portfolio = operator_payload.get("portfolio", {})
a1, a2, a3, a4 = st.columns(4)
a1.metric("allocation_count", int(portfolio.get("allocation_count", 0)))
a2.metric("total_allocated_percent", f"{float(portfolio.get('total_allocated_percent', 0.0)):.2f}%")
a3.metric("brain rotate in", len(portfolio.get("brain_rotate_in") or []))
a4.metric("brain rotate out", len(portfolio.get("brain_rotate_out") or []))
brain_pri = portfolio.get("brain_top_priorities") or []
if brain_pri:
    st.markdown("**Top priorities (ranked)**")
    st.dataframe(pd.DataFrame(brain_pri), use_container_width=True, hide_index=True)
brain_fam = portfolio.get("brain_family_concentration") or []
if brain_fam:
    st.markdown("**Family concentration**")
    st.dataframe(pd.DataFrame(brain_fam), use_container_width=True, hide_index=True)
bri = portfolio.get("brain_rotate_in") or []
bro = portfolio.get("brain_rotate_out") or []
if bri or bro:
    rc1, rc2 = st.columns(2)
    with rc1:
        st.markdown("**Rotate in suggestions**")
        st.dataframe(pd.DataFrame(bri) if bri else pd.DataFrame(), use_container_width=True, hide_index=True)
    with rc2:
        st.markdown("**Rotate out suggestions**")
        st.dataframe(pd.DataFrame(bro) if bro else pd.DataFrame(), use_container_width=True, hide_index=True)
shifts = portfolio.get("brain_capital_shifts") or []
if shifts:
    st.markdown("**Dynamic allocation shifts (simulated)**")
    st.dataframe(pd.DataFrame(shifts), use_container_width=True, hide_index=True)
top_alloc = portfolio.get("top_allocations", [])
if top_alloc:
    st.markdown("**Top allocations (by capital %)**")
    st.dataframe(pd.DataFrame(top_alloc), use_container_width=True, hide_index=True)
elif portfolio_payload is not None:
    st.dataframe(
        pd.DataFrame(portfolio_payload.get("allocations", [])),
        use_container_width=True,
        hide_index=True,
    )
    brain = (portfolio_payload.get("brain") or {}) if portfolio_payload else {}
    if brain.get("top_priorities"):
        st.markdown("**Brain priorities (from /portfolio/allocation)**")
        st.dataframe(pd.DataFrame(brain.get("top_priorities", [])), use_container_width=True, hide_index=True)

st.divider()

st.subheader("Candidate Review")
review_counts = (review_status_payload or {}).get("counts", {})
r1, r2, r3, r4, r5 = st.columns(5)
r1.metric("Pending", int(review_counts.get("PENDING_REVIEW", 0)))
r2.metric("Under Review", int(review_counts.get("UNDER_REVIEW", 0)))
r3.metric("Approved Demo", int(review_counts.get("APPROVED_FOR_DEMO", 0)))
r4.metric("Rejected", int(review_counts.get("REJECTED_BY_REVIEW", 0)))
r5.metric("Needs Testing", int(review_counts.get("NEEDS_MORE_TESTING", 0)))
if review_candidates_payload is not None:
    review_rows = review_candidates_payload.get("candidates", [])
    if review_rows:
        st.dataframe(pd.DataFrame(review_rows), use_container_width=True, hide_index=True)

st.divider()

st.subheader("Demo Queue / Executor / Runner")
dq, ex, rn = st.columns(3)

with dq:
    st.markdown("**Demo Queue**")
    demo_counts = (demo_status_payload or {}).get("counts", {})
    st.write(
        f"Queue: {int(demo_counts.get('DEMO_QUEUE', 0))} | "
        f"Assigned: {int(demo_counts.get('DEMO_ASSIGNED', 0))} | "
        f"Running: {int(demo_counts.get('DEMO_RUNNING', 0))}"
    )
    demo_rows = (demo_candidates_payload or {}).get("candidates", [])
    if demo_rows:
        st.dataframe(pd.DataFrame(demo_rows), use_container_width=True, hide_index=True)

with ex:
    st.markdown("**Executor Adapter**")
    ex_counts = (executor_status_payload or {}).get("counts", {})
    st.write(
        f"Ready: {int(ex_counts.get('EXECUTOR_READY', 0))} | "
        f"Running: {int(ex_counts.get('EXECUTOR_RUNNING', 0))} | "
        f"Paused: {int(ex_counts.get('EXECUTOR_PAUSED', 0))}"
    )
    ex_rows = (executor_status_payload or {}).get("prepared_or_running", [])
    if ex_rows:
        st.dataframe(pd.DataFrame(ex_rows), use_container_width=True, hide_index=True)
    elif executor_candidates_payload is not None:
        st.dataframe(
            pd.DataFrame(executor_candidates_payload.get("candidates", [])),
            use_container_width=True,
            hide_index=True,
        )

with rn:
    st.markdown("**Runner Bridge**")
    runner_counts = (runner_status_payload or {}).get("counts", {})
    st.write(
        f"Ack: {int(runner_counts.get('RUNNER_ACKNOWLEDGED', 0))} | "
        f"Active: {int(runner_counts.get('RUNNER_ACTIVE', 0))} | "
        f"Completed: {int(runner_counts.get('RUNNER_COMPLETED', 0))} | "
        f"Failed: {int(runner_counts.get('RUNNER_FAILED', 0))}"
    )
    runner_rows = (runner_status_payload or {}).get("current_jobs", [])
    if runner_rows:
        st.dataframe(pd.DataFrame(runner_rows), use_container_width=True, hide_index=True)
    elif runner_jobs_payload is not None:
        st.dataframe(
            pd.DataFrame(runner_jobs_payload.get("jobs", [])),
            use_container_width=True,
            hide_index=True,
        )

st.divider()

st.subheader("Multi-Runner Engine")
st.caption(
    "Demo/simulation fleet coordination only: runner registration, load, and strategy→runner "
    "assignment metadata. No broker execution or live trading."
)
if multi_runner_status_payload is not None:
    fs = multi_runner_status_payload.get("fleet_summary") or {}
    js = multi_runner_status_payload.get("jobs_summary") or {}
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("runners", int(fs.get("runner_count", 0)))
    m2.metric("healthy", int(fs.get("healthy_runner_count", 0)))
    m3.metric("degraded", int(fs.get("degraded_count", 0)))
    m4.metric("offline", int(fs.get("offline_count", 0)))
    m5.metric("unassigned jobs", int(js.get("unassigned_queue", 0)))
    warn_lines: list[str] = []
    if int(fs.get("offline_count", 0)) > 0:
        warn_lines.append("One or more runners are marked offline.")
    if int(fs.get("degraded_count", 0)) > 0:
        warn_lines.append("Stale heartbeat — runners shown as degraded until they heartbeat again.")
    if warn_lines:
        st.warning("\n".join(warn_lines))
    runners_tbl = multi_runner_status_payload.get("runners") or []
    if runners_tbl:
        st.markdown("**Fleet: capacity vs load**")
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "runner_id": x.get("runner_id"),
                        "runner_status": x.get("runner_status"),
                        "status_explain": x.get("status_explain"),
                        "runner_health": x.get("runner_health"),
                        "capacity": x.get("runner_capacity"),
                        "current_load": x.get("current_load"),
                        "assigned_jobs": x.get("assigned_jobs_count"),
                        "spare": x.get("spare_capacity"),
                        "last_seen_at": x.get("last_seen_at"),
                    }
                    for x in runners_tbl
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
elif multi_runner_err:
    st.caption(f"Multi-runner status unavailable: {multi_runner_err}")

if multi_runner_jobs_payload is not None:
    uq = multi_runner_jobs_payload.get("unassigned_queue") or []
    br = multi_runner_jobs_payload.get("by_runner") or {}
    st.markdown(
        f"**Jobs view:** total {int(multi_runner_jobs_payload.get('total_jobs', 0))} | "
        f"unassigned queue: {len(uq)} | runners with jobs: {len(br)}"
    )
    if uq:
        with st.expander("Unassigned queue (sample)"):
            st.dataframe(pd.DataFrame(uq[:25]), use_container_width=True, hide_index=True)
    for rid, rows in sorted(br.items(), key=lambda kv: kv[0]):
        with st.expander(f"Runner `{rid}` ({len(rows)} job(s))"):
            st.dataframe(pd.DataFrame(rows[:40]), use_container_width=True, hide_index=True)
elif multi_runner_jobs_err:
    st.caption(f"Multi-runner jobs unavailable: {multi_runner_jobs_err}")

st.divider()

st.subheader("Reporting Snapshot")
if report_payload is not None:
    st.caption(
        f"Generated at: {report_payload.get('report_generated_at', '')} | "
        f"paper_success: {report_payload.get('paper_success', 0)} | "
        f"live_safe_candidates: {report_payload.get('live_safe_candidates', 0)}"
    )
    recs = report_payload.get("recommendations", [])
    if recs:
        st.info("Recommendations:\n- " + "\n- ".join([str(r) for r in recs]))
    top5 = report_payload.get("top_5_strategies", [])
    if top5:
        st.dataframe(pd.DataFrame(top5), use_container_width=True, hide_index=True)

if bots_payload is None and bots_err:
    st.warning(f"Bots overview unavailable: {bots_err}")
elif bots_payload is not None:
    st.markdown("**Live Control Engine**")
    if control_payload is not None:
        st.caption(
            f"live_engine={control_payload.get('live_engine')} | "
            f"updated_at={str(control_payload.get('updated_at') or '—')[:19]}"
        )
        live_rows = (control_payload.get("signals") or []) if isinstance(control_payload, dict) else []
        if live_rows:
            show = []
            for row in live_rows:
                if not isinstance(row, dict):
                    continue
                show.append(
                    {
                        "name": row.get("name"),
                        "control_state": row.get("control_state"),
                        "recommended_action": row.get("recommended_action"),
                        "control_active": row.get("control_active"),
                        "entriesEnabled": row.get("entriesEnabled"),
                        "target_volume": row.get("target_volume"),
                        "effective_capital": row.get("effective_capital"),
                        "updated_at": row.get("updated_at"),
                        "reasoning": " | ".join(str(x) for x in (row.get("reasoning") or [])[:3]),
                    }
                )
            st.dataframe(pd.DataFrame(show), use_container_width=True, hide_index=True)
    st.divider()
    st.markdown("**Bots & Control Signals (Detailed)**")
    bots = bots_payload.get("bots", [])
    signals_by_name = {x.get("name"): x for x in (control_payload or {}).get("signals", []) if isinstance(x, dict)}
    for row in bots:
        sig = signals_by_name.get(row.get("name"))
        row["recommended_action"] = sig.get("recommended_action") if sig else "NO_CHANGE"
    if bots:
        st.dataframe(pd.DataFrame(bots), use_container_width=True, hide_index=True)
