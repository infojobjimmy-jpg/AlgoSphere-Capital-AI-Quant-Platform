import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if not os.environ.get("ALGO_SPHERE_DB_PATH"):
    os.environ["ALGO_SPHERE_DB_PATH"] = str(
        Path(tempfile.mkdtemp()) / "fake_bot_test.db"
    )

from backend.main import (
    apply_paper_feedback,
    deploy_paper_bots,
    evolve_factory_strategies,
    generate_factory_strategies,
    get_bots,
    get_brain,
    get_control_signals,
    get_factory_lineage,
    get_factory_strategies,
    get_factory_top,
    get_fund_signals,
    get_fund_status,
    get_fund_allocation_status,
    get_fund_portfolio,
    post_fund_rebalance,
    get_paper_feedback_preview,
    get_paper_status,
    get_auto_loop_status,
    get_live_safe_candidates,
    get_live_safe_status,
    get_capital_status,
    get_legacy_meta_status,
    get_portfolio_allocation,
    get_report_daily,
    get_report_summary,
    get_review_candidates,
    get_review_status,
    approve_review_candidate,
    assign_demo_candidate,
    get_demo_candidates,
    get_demo_status,
    get_executor_candidates,
    get_executor_status,
    get_runner_jobs,
    get_runner_status,
    get_operator_console_status,
    get_alerts,
    get_alerts_summary,
    acknowledge_alert,
    get_recovery_engine_status,
    run_recovery_engine,
    get_performance_system,
    get_performance_strategies,
    get_performance_top,
    get_promotion_candidates,
    run_smart_promotion_engine,
    ack_runner_job,
    complete_runner_job,
    fail_runner_job,
    pause_executor_item,
    pause_runner_job,
    prepare_executor_item,
    pause_demo_candidate,
    queue_demo_candidate,
    reject_demo_candidate,
    start_executor_item,
    start_runner_job,
    stop_executor_item,
    reject_review_candidate,
    flag_review_candidate,
    promote_live_safe,
    run_auto_cycle_once,
    on_startup,
    update_account,
    upsert_bot,
)
from backend.schemas import AccountUpdate, BotUpdate


class FakeBotFlowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        db_path = Path(os.environ["ALGO_SPHERE_DB_PATH"])
        if db_path.exists():
            db_path.unlink()
        on_startup()

    def test_fake_bot_end_to_end_flow(self) -> None:
        account_result = update_account(AccountUpdate(balance=20000, risk_limit=0.03))
        self.assertEqual(account_result["balance"], 20000.0)
        self.assertEqual(account_result["risk_limit"], 0.03)

        bot_a = upsert_bot(
            BotUpdate(
                name="fake_alpha",
                profit=450.0,
                drawdown=110.0,
                win_rate=0.62,
                trades=120,
            )
        )
        bot_b = upsert_bot(
            BotUpdate(
                name="fake_beta",
                profit=-90.0,
                drawdown=320.0,
                win_rate=0.46,
                trades=40,
            )
        )

        self.assertIn(bot_a["decision"], {"EXECUTE", "MONITOR", "REDUCE", "PAUSE"})
        self.assertIn(bot_b["risk_level"], {"LOW", "MEDIUM", "HIGH"})

        bots_payload = get_bots()
        self.assertEqual(bots_payload["count"], 2)
        self.assertIsInstance(bots_payload["total_profit"], float)
        self.assertEqual(len(bots_payload["bots"]), 2)

        first_bot = bots_payload["bots"][0]
        self.assertIn("score", first_bot)
        self.assertIn("capital_alloc", first_bot)
        self.assertIn("decision", first_bot)
        self.assertIn("control_state", first_bot)
        self.assertIn(first_bot["control_state"], {"MONITOR", "REDUCE", "BOOST", "KILL"})
        self.assertIn("effective_capital", first_bot)
        self.assertIn("control_active", first_bot)

        sig_payload = get_control_signals()
        self.assertEqual(sig_payload["count"], 2)
        self.assertEqual(len(sig_payload["signals"]), 2)
        action_map = {
            "KILL": "STOP",
            "REDUCE": "LOWER_VOLUME",
            "BOOST": "INCREASE_VOLUME",
            "MONITOR": "NO_CHANGE",
        }
        for s in sig_payload["signals"]:
            self.assertIn("name", s)
            self.assertIn("control_state", s)
            self.assertIn("recommended_action", s)
            self.assertEqual(
                s["recommended_action"],
                action_map.get(s["control_state"], "NO_CHANGE"),
            )

        fund_payload = get_fund_status()
        self.assertIn("fund_engine", fund_payload)
        fe = fund_payload["fund_engine"]
        self.assertIn("total_capital", fe)
        self.assertIn("allocated_capital", fe)
        self.assertIn("free_capital", fe)
        self.assertIn(
            fund_payload["portfolio_state"],
            {"NORMAL", "CAUTION", "DEFENSIVE", "LOCKDOWN"},
        )
        self.assertIn(
            fund_payload["recommended_portfolio_action"],
            {"KEEP_RUNNING", "TIGHTEN_RISK", "REDUCE_ALL", "STOP_NEW_ENTRIES"},
        )
        self.assertEqual(fund_payload["summary"]["bot_count"], 2)
        self.assertEqual(
            fund_payload["summary"]["active_bot_count"],
            2,
        )
        self.assertIn("top_bots", fund_payload)
        self.assertIn("worst_bots", fund_payload)
        self.assertIsInstance(fund_payload["reasoning"], str)

        fund_signal = get_fund_signals()
        self.assertIn(
            fund_signal["portfolio_state"],
            {"NORMAL", "CAUTION", "DEFENSIVE", "LOCKDOWN"},
        )
        self.assertIn(
            fund_signal["recommended_action"],
            {"KEEP_RUNNING", "TIGHTEN_RISK", "REDUCE_ALL", "STOP_NEW_ENTRIES"},
        )
        self.assertEqual(fund_signal["active_bot_count"], 2)
        self.assertIn("kill_bot_count", fund_signal)
        self.assertIn("reduce_bot_count", fund_signal)
        self.assertIsInstance(fund_signal["reasoning"], str)

        generated = generate_factory_strategies(count=8, seed=7)
        self.assertEqual(generated["generated_count"], 8)
        self.assertEqual(len(generated["strategies"]), 8)

        strategies = get_factory_strategies()
        self.assertGreaterEqual(strategies["count"], 8)
        sample = strategies["strategies"][0]
        self.assertIn(sample["family"], {"EMA_CROSS", "MOMENTUM", "MEAN_REVERSION", "SESSION_BREAKOUT"})
        self.assertIn(
            sample["status"],
            {"GENERATED", "TESTING", "CANDIDATE", "REJECTED", "APPROVED_FOR_REVIEW"},
        )
        self.assertIn("parameters", sample)
        self.assertIn("fitness_score", sample)

        top = get_factory_top(limit=3)
        self.assertEqual(top["count"], 3)
        self.assertEqual(len(top["top"]), 3)
        self.assertIn("origin_type", top["top"][0])
        self.assertIn("generation", top["top"][0])

        evolved = evolve_factory_strategies(
            top_n=2,
            children_per_parent=2,
            crossover_rate=0.5,
            seed=13,
        )
        self.assertGreaterEqual(evolved["evolved_count"], 4)
        self.assertGreaterEqual(evolved["total_count"], 12)
        self.assertTrue(evolved["strategies"])
        child = evolved["strategies"][0]
        self.assertIn(child["origin_type"], {"MUTATED", "CROSSED"})
        self.assertGreaterEqual(int(child["generation"]), 1)
        self.assertIn("parent_strategy_id", child)
        self.assertIsNotNone(child["parent_strategy_id"])
        self.assertIn("mutation_note", child)

        lineage = get_factory_lineage(child["strategy_id"])
        self.assertTrue(lineage["found"])
        self.assertGreaterEqual(len(lineage["lineage"]), 1)
        self.assertEqual(lineage["lineage"][0]["strategy_id"], child["strategy_id"])

        paper_deploy_result = deploy_paper_bots(max_bots=5)
        self.assertLessEqual(paper_deploy_result["deployed_count"], 5)
        self.assertEqual(paper_deploy_result["max_paper_bots"], 5)
        self.assertIn("paper_bots", paper_deploy_result)

        paper_status = get_paper_status()
        self.assertLessEqual(paper_status["count"], 5)
        self.assertIn("summary", paper_status)
        self.assertIn("running_paper_bots", paper_status)
        if paper_status["running_paper_bots"]:
            paper_one = paper_status["running_paper_bots"][0]
            self.assertIn(
                paper_one["status"],
                {"PAPER_RUNNING", "PAPER_REJECTED", "PAPER_SUCCESS"},
            )
            self.assertIn("paper_profit", paper_one)
            self.assertIn("paper_drawdown", paper_one)
            self.assertIn("paper_win_rate", paper_one)
            self.assertIn("paper_trades", paper_one)

        preview_feedback = get_paper_feedback_preview()
        self.assertEqual(preview_feedback["mode"], "preview")
        self.assertIn("results", preview_feedback)
        applied_feedback = apply_paper_feedback()
        self.assertEqual(applied_feedback["mode"], "applied")
        self.assertIn("results", applied_feedback)
        if applied_feedback["results"]:
            one = applied_feedback["results"][0]
            self.assertIn(
                one["target_status"],
                {"PAPER_SUCCESS", "PAPER_REJECTED", "EVOLVE_AGAIN"},
            )
            self.assertIn(one["action"], {"PROMOTE", "REJECT", "EVOLVE_AGAIN"})
            self.assertIn("feedback_score", one)
            self.assertIn("promotion_score", one)

        cycle_once = run_auto_cycle_once()
        self.assertTrue(cycle_once["ok"])
        auto_status = get_auto_loop_status()
        self.assertGreaterEqual(auto_status["loops_completed"], 1)

        live_candidates = get_live_safe_candidates()
        self.assertIn("candidates", live_candidates)
        live_promote = promote_live_safe()
        self.assertIn("results", live_promote)
        if live_promote["results"]:
            l = live_promote["results"][0]
            self.assertIn(
                l["target_status"],
                {
                    "APPROVED_FOR_LIVE_REVIEW",
                    "LIVE_SAFE_CANDIDATE",
                    "LIVE_SAFE_REJECTED",
                    "LIVE_SAFE_READY",
                },
            )
        live_status = get_live_safe_status()
        self.assertTrue(live_status["review_only"])
        self.assertTrue(live_status["manual_approval_required"])
        self.assertIn("state_counts", live_status)

        allocation = get_portfolio_allocation()
        self.assertIn("allocations", allocation)
        self.assertIn("total_allocated_percent", allocation)
        for a in allocation["allocations"]:
            self.assertIn("strategy_id", a)
            self.assertIn("weight", a)
            self.assertIn("capital_percent", a)
            self.assertIn("risk_score", a)
            self.assertIn("allocation_reason", a)
            self.assertLessEqual(float(a["capital_percent"]), 20.0)

        meta = get_legacy_meta_status()
        self.assertIn(meta["system_health"], {"GOOD", "WARNING", "CRITICAL"})
        self.assertIn(meta["risk_mode"], {"NORMAL", "DEFENSIVE"})
        self.assertIn(meta["generation_speed"], {"SLOW", "NORMAL", "FAST"})
        self.assertIn(meta["portfolio_quality"], {"LOW", "MEDIUM", "HIGH"})
        self.assertIn("evolution_rate", meta)
        self.assertIn("recommendations", meta)

        capital = get_capital_status()
        self.assertIn("total_capital", capital)
        self.assertIn("allocated", capital)
        self.assertIn("free", capital)
        self.assertIn("risk_usage", capital)
        self.assertIn("growth_rate", capital)
        self.assertGreaterEqual(float(capital["total_capital"]), 0.0)
        self.assertGreaterEqual(float(capital["allocated"]), 0.0)
        self.assertGreaterEqual(float(capital["free"]), 0.0)

        report = get_report_summary()
        self.assertIn("system_health", report)
        self.assertIn("risk_mode", report)
        self.assertIn("portfolio_state", report)
        self.assertIn("recommended_portfolio_action", report)
        self.assertIn("total_capital", report)
        self.assertIn("allocated", report)
        self.assertIn("free", report)
        self.assertIn("growth_rate", report)
        self.assertIn("total_strategies", report)
        self.assertIn("live_safe_candidates", report)
        self.assertIn("paper_running", report)
        self.assertIn("paper_success", report)
        self.assertIn("top_5_strategies", report)
        self.assertIn("warnings", report)
        self.assertIn("recommendations", report)
        self.assertLessEqual(len(report["top_5_strategies"]), 5)

        daily = get_report_daily()
        self.assertIn("date", daily)
        self.assertIn("system_health", daily)
        self.assertIn("capital", daily)
        self.assertIn("strategy_counts", daily)
        self.assertIn("top_3", daily)
        self.assertLessEqual(len(daily["top_3"]), 3)

        review_candidates = get_review_candidates(limit=10)
        self.assertIn("count", review_candidates)
        self.assertIn("candidates", review_candidates)
        if review_candidates["candidates"]:
            rc = review_candidates["candidates"][0]
            self.assertIn("strategy_id", rc)
            self.assertIn("family", rc)
            self.assertIn("status", rc)
            self.assertIn("fitness_score", rc)
            self.assertIn("promotion_score", rc)
            self.assertIn("review_status", rc)
            self.assertIn("review_priority", rc)

            sid = rc["strategy_id"]
            approved = approve_review_candidate(sid, reviewer="qa_operator")
            self.assertTrue(approved["ok"])
            self.assertEqual(approved["review_status"], "APPROVED_FOR_DEMO")

            flagged = flag_review_candidate(
                sid,
                note="Need additional stress-test runs.",
                reviewer="qa_operator",
            )
            self.assertTrue(flagged["ok"])
            self.assertEqual(flagged["review_status"], "NEEDS_MORE_TESTING")

            rejected = reject_review_candidate(
                sid,
                note="Rejecting for now due to review findings.",
                reviewer="qa_operator",
            )
            self.assertTrue(rejected["ok"])
            self.assertEqual(rejected["review_status"], "REJECTED_BY_REVIEW")

        review_status = get_review_status()
        self.assertIn("counts", review_status)
        self.assertIn("top_priority_candidates", review_status)
        self.assertIn("review_only", review_status)
        self.assertTrue(review_status["review_only"])

        demo_candidates = get_demo_candidates(limit=20)
        self.assertIn("count", demo_candidates)
        self.assertIn("candidates", demo_candidates)
        if demo_candidates["candidates"]:
            eligible = next(
                (x for x in demo_candidates["candidates"] if x.get("eligible") is True),
                demo_candidates["candidates"][0],
            )
            sid_demo = eligible["strategy_id"]
            queued = queue_demo_candidate(sid_demo, note="Queueing via test")
            self.assertTrue(queued["ok"])
            self.assertEqual(queued["demo_status"], "DEMO_QUEUE")

            assigned = assign_demo_candidate(
                sid_demo,
                assignee="demo_slot_a",
                note="Assigned via test",
            )
            self.assertTrue(assigned["ok"])
            self.assertEqual(assigned["demo_status"], "DEMO_ASSIGNED")
            self.assertEqual(assigned["demo_assignee"], "demo_slot_a")

            paused = pause_demo_candidate(sid_demo, note="Paused via test")
            self.assertTrue(paused["ok"])
            self.assertEqual(paused["demo_status"], "DEMO_PAUSED")

            demo_rejected = reject_demo_candidate(sid_demo, note="Rejected via test")
            self.assertTrue(demo_rejected["ok"])
            self.assertEqual(demo_rejected["demo_status"], "DEMO_REJECTED")

        demo_status = get_demo_status()
        self.assertIn("counts", demo_status)
        self.assertIn("top_priority", demo_status)
        self.assertIn("queue_only", demo_status)
        self.assertTrue(demo_status["queue_only"])

        executor_candidates = get_executor_candidates(limit=20)
        self.assertIn("count", executor_candidates)
        self.assertIn("candidates", executor_candidates)
        if executor_candidates["candidates"]:
            ex_eligible = next(
                (x for x in executor_candidates["candidates"] if x.get("eligible") is True),
                None,
            )
            if ex_eligible is not None:
                sid_ex = ex_eligible["strategy_id"]
                prepared = prepare_executor_item(
                    sid_ex,
                    target="demo_runner_a",
                    note="Prepared via test",
                )
                self.assertTrue(prepared["ok"])
                self.assertEqual(prepared["executor_status"], "EXECUTOR_READY")

                started = start_executor_item(sid_ex, note="Started via test")
                self.assertTrue(started["ok"])
                self.assertEqual(started["executor_status"], "EXECUTOR_RUNNING")

                ex_paused = pause_executor_item(sid_ex, note="Paused via test")
                self.assertTrue(ex_paused["ok"])
                self.assertEqual(ex_paused["executor_status"], "EXECUTOR_PAUSED")

                ex_stopped = stop_executor_item(sid_ex, note="Stopped via test")
                self.assertTrue(ex_stopped["ok"])
                self.assertEqual(ex_stopped["executor_status"], "EXECUTOR_STOPPED")

        executor_status = get_executor_status()
        self.assertIn("counts", executor_status)
        self.assertIn("prepared_or_running", executor_status)
        self.assertIn("adapter_only", executor_status)
        self.assertTrue(executor_status["adapter_only"])

        runner_jobs = get_runner_jobs(limit=20)
        self.assertIn("count", runner_jobs)
        self.assertIn("jobs", runner_jobs)
        runner_eligible = next((j for j in runner_jobs["jobs"] if j.get("eligible") is True), None)
        if runner_eligible is not None:
            sid_run = runner_eligible["strategy_id"]
            acked = ack_runner_job(
                sid_run,
                runner_id="runner_alpha",
                note="Ack via test",
            )
            self.assertTrue(acked["ok"])
            self.assertEqual(acked["runner_status"], "RUNNER_ACKNOWLEDGED")

            started_run = start_runner_job(sid_run, note="Start via test")
            self.assertTrue(started_run["ok"])
            self.assertEqual(started_run["runner_status"], "RUNNER_ACTIVE")

            paused_run = pause_runner_job(sid_run, note="Pause via test")
            self.assertTrue(paused_run["ok"])
            self.assertEqual(paused_run["runner_status"], "RUNNER_PAUSED")

            completed_run = complete_runner_job(sid_run, note="Complete via test")
            self.assertTrue(completed_run["ok"])
            self.assertEqual(completed_run["runner_status"], "RUNNER_COMPLETED")

        # Fail path: prepare a second item if available, then fail it.
        runner_jobs_after = get_runner_jobs(limit=20).get("jobs", [])
        fail_target = next((j for j in runner_jobs_after if j.get("strategy_id") != (runner_eligible or {}).get("strategy_id") and j.get("eligible") is True), None)
        if fail_target is not None:
            sid_fail = fail_target["strategy_id"]
            ack_runner_job(sid_fail, runner_id="runner_beta", note="Ack for fail path")
            failed_run = fail_runner_job(sid_fail, note="Failure via test")
            self.assertTrue(failed_run["ok"])
            self.assertEqual(failed_run["runner_status"], "RUNNER_FAILED")

        runner_status = get_runner_status()
        self.assertIn("counts", runner_status)
        self.assertIn("current_jobs", runner_status)
        self.assertIn("summary", runner_status)
        self.assertIn("bridge_only", runner_status)
        self.assertTrue(runner_status["bridge_only"])

        operator = get_operator_console_status()
        self.assertIn("system_health", operator)
        self.assertIn("risk_mode", operator)
        self.assertIn("generation_speed", operator)
        self.assertIn("loops_completed", operator)
        self.assertIn("last_cycle_at", operator)
        self.assertIn("pipeline", operator)
        self.assertIn("capital", operator)
        self.assertIn("portfolio", operator)
        self.assertIn("risk_flags", operator)
        self.assertIn("timestamp", operator)
        self.assertIn("total_candidates", operator["pipeline"])
        self.assertIn("paper_running", operator["pipeline"])
        self.assertIn("paper_success", operator["pipeline"])
        self.assertIn("review_pending", operator["pipeline"])
        self.assertIn("demo_queued", operator["pipeline"])
        self.assertIn("allocation_count", operator["portfolio"])
        self.assertIn("total_allocated_percent", operator["portfolio"])
        self.assertIn("top_allocations", operator["portfolio"])
        self.assertIsInstance(operator["risk_flags"], list)

        brain = get_brain().model_dump()
        self.assertIn(brain["regime"], {"RISK_ON", "NEUTRAL", "RISK_OFF"})
        self.assertIsInstance(brain["message"], str)

        conn = sqlite3.connect(os.environ["ALGO_SPHERE_DB_PATH"])
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM bots")
            row_count = cursor.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(row_count, 2)

    def test_zz_alerting_engine_endpoints(self) -> None:
        alerts = get_alerts()
        self.assertIn("count", alerts)
        self.assertIn("alerts", alerts)
        self.assertIsInstance(alerts["alerts"], list)
        self.assertEqual(
            alerts["count"],
            sum(1 for a in alerts["alerts"] if a.get("active", True)),
        )
        for a in alerts["alerts"]:
            self.assertIn("alert_id", a)
            self.assertIn("rule_code", a)
            self.assertIn(
                a["category"],
                {"RISK", "CAPITAL", "PIPELINE", "REVIEW", "DEMO", "RUNNER", "SYSTEM"},
            )
            self.assertIn(a["severity"], {"INFO", "WARNING", "CRITICAL"})
            self.assertIn("title", a)
            self.assertIn("message", a)
            self.assertIn("source", a)
            self.assertIn("created_at", a)
            self.assertIn("active", a)
            self.assertIn("recommended_operator_action", a)

        summary = get_alerts_summary()
        self.assertIn("total_alerts", summary)
        self.assertIn("critical_count", summary)
        self.assertIn("warning_count", summary)
        self.assertIn("info_count", summary)
        self.assertIn("top_active_alerts", summary)
        self.assertEqual(
            summary["total_alerts"],
            summary["critical_count"] + summary["warning_count"] + summary["info_count"],
        )

        if alerts["alerts"]:
            first_id = str(alerts["alerts"][0]["alert_id"])
            ack = acknowledge_alert(first_id)
            self.assertTrue(ack.get("ok"))
            after = get_alerts()
            match = next(x for x in after["alerts"] if str(x["alert_id"]) == first_id)
            self.assertFalse(match.get("active", True))

    def test_zz_auto_recovery_engine(self) -> None:
        st = get_recovery_engine_status()
        self.assertIn("recovery_state", st)
        self.assertIn("last_action", st)
        self.assertIn("last_result", st)
        self.assertIn("active", st)
        self.assertIn("recovery_history", st)
        run = run_recovery_engine()
        self.assertIn("run_ok", run)
        self.assertIn("triggers_fired", run)
        self.assertIn("actions_log", run)
        self.assertIn(run.get("recovery_state", ""), {"RECOVERY_SUCCESS", "RECOVERY_FAILED"})

    def test_zz_performance_engine(self) -> None:
        sys_body = get_performance_system()
        self.assertIn("runner_success_rate", sys_body)
        self.assertIn("runner_fail_rate", sys_body)
        self.assertIn("avg_runner_duration", sys_body)
        self.assertIn("pipeline_throughput", sys_body)
        self.assertIn("recovery_rate", sys_body)
        self.assertIn("total_jobs", sys_body)
        self.assertIn("performance_trends", sys_body)

        strat_body = get_performance_strategies()
        self.assertIn("count", strat_body)
        self.assertIn("strategies", strat_body)
        self.assertEqual(strat_body["count"], len(strat_body["strategies"]))
        for row in strat_body["strategies"][:3]:
            self.assertIn("strategy_id", row)
            self.assertIn("total_runs", row)
            self.assertIn("success_rate", row)
            self.assertIn("avg_duration", row)
            self.assertIn("performance_score", row)
            self.assertIn("last_run", row)

        top_body = get_performance_top()
        self.assertIn("strategies", top_body)
        self.assertLessEqual(len(top_body["strategies"]), 10)

    def test_zz_smart_promotion_engine(self) -> None:
        cand = get_promotion_candidates()
        self.assertIn("review_candidates", cand)
        self.assertIn("demo_candidates", cand)
        self.assertIn("executor_candidates", cand)
        self.assertIn("runner_candidates", cand)
        self.assertIn("thresholds", cand)
        self.assertIn("recent_promotion_history", cand)
        run = run_smart_promotion_engine()
        self.assertIn("promoted", run)
        self.assertIn("skipped", run)
        self.assertIsInstance(run["promoted"], list)
        self.assertIsInstance(run["skipped"], list)

    def test_zz_fund_engine_endpoints(self) -> None:
        st = get_fund_allocation_status()
        self.assertEqual(st["total_capital"], 100_000.0)
        self.assertIn("allocated_capital", st)
        self.assertIn("free_capital", st)
        self.assertIn("portfolio_return", st)
        self.assertIn("risk_score", st)
        self.assertIn("drawdown", st)
        port = get_fund_portfolio()
        self.assertIn("strategies", port)
        self.assertIsInstance(port["strategies"], list)
        rb = post_fund_rebalance()
        self.assertTrue(rb.get("ok"))
        self.assertIn("rebalanced_at", rb)
        self.assertIn("strategies", rb)


if __name__ == "__main__":
    unittest.main(verbosity=2)
