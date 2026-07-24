"""Safe self-improvement audit trail (diffs + benchmark gate; deploy only when enabled and approved)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_session
from app.self_code.pipeline import run_self_code_pipeline

router = APIRouter()


class SelfCodeProposal(BaseModel):
    proposal_summary: str
    proposal_diff: str
    baseline_command: str | None = None
    candidate_command: str | None = None


@router.get("/status")
async def status() -> dict[str, object]:
    return {
        "enabled": settings.self_code_enabled,
        "dry_run": settings.self_code_dry_run,
        "workspace_root": settings.self_code_workspace_root,
        "baseline_command": settings.self_code_baseline_command,
        "candidate_command": settings.self_code_candidate_command,
        "note": "Proposals run in isolation; production deploy requires explicit approval + benchmark win.",
    }


@router.post("/run")
async def run_proposal(proposal: SelfCodeProposal, session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    if not settings.self_code_enabled:
        return {"executed": False, "reason": "self_code_disabled"}
    if not proposal.proposal_diff.strip():
        return {"executed": False, "reason": "empty_diff"}
    result = await run_self_code_pipeline(
        session=session,
        proposal_summary=proposal.proposal_summary,
        proposal_diff=proposal.proposal_diff,
        baseline_command=proposal.baseline_command or settings.self_code_baseline_command,
        candidate_command=proposal.candidate_command or settings.self_code_candidate_command,
    )
    return {"executed": True, "dry_run": settings.self_code_dry_run, **result}
