from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.self_code.benchmark_engine import benchmark_candidate
from app.self_code.deployment_gate import evaluate_deployment_gate


async def run_self_code_pipeline(
    *,
    session: AsyncSession,
    proposal_summary: str,
    proposal_diff: str,
    baseline_command: str,
    candidate_command: str,
) -> dict[str, Any]:
    report = await benchmark_candidate(
        proposal_diff=proposal_diff,
        baseline_command=baseline_command,
        candidate_command=candidate_command,
    )
    decision = evaluate_deployment_gate(report, proposal_diff)
    meta = {
        "baseline": {
            "ok": report.baseline.ok,
            "returncode": report.baseline.returncode,
            "duration_sec": report.baseline.duration_sec,
            "stdout": report.baseline.stdout,
            "stderr": report.baseline.stderr,
            "command": report.baseline.command,
        },
        "candidate": {
            "ok": report.candidate.ok,
            "returncode": report.candidate.returncode,
            "duration_sec": report.candidate.duration_sec,
            "stdout": report.candidate.stdout,
            "stderr": report.candidate.stderr,
            "command": report.candidate.command,
        },
        "decision": {"approved": decision.approved, "deployed": decision.deployed, "reason": decision.reason},
    }
    await session.execute(
        text(
            """
            INSERT INTO self_code_runs (proposal_summary, benchmark_delta, approved, dry_run, meta)
            VALUES (:s, :bd, :ok, :dr, CAST(:m AS jsonb))
            """
        ),
        {
            "s": proposal_summary[:2000],
            "bd": float(report.delta),
            "ok": bool(decision.approved),
            "dr": bool(settings.self_code_dry_run),
            "m": json.dumps(meta),
        },
    )
    await session.commit()
    return {
        "delta": float(report.delta),
        "approved": bool(decision.approved),
        "deployed": bool(decision.deployed),
        "reason": decision.reason,
        "baseline_ok": bool(report.baseline.ok),
        "candidate_ok": bool(report.candidate.ok),
    }
