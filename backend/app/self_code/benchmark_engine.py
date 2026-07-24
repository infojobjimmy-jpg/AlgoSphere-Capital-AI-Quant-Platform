from __future__ import annotations

from dataclasses import dataclass

from app.self_code.sandbox_runner import SandboxResult, run_in_sandbox


@dataclass(slots=True)
class BenchmarkReport:
    baseline: SandboxResult
    candidate: SandboxResult
    delta: float
    passed: bool


def _score(res: SandboxResult) -> float:
    # Prefer passing runs and faster completion.
    base = 1.0 if res.ok else 0.0
    speed_bonus = max(0.0, 0.20 - min(0.20, res.duration_sec / 600.0))
    return base + speed_bonus


async def benchmark_candidate(
    *,
    proposal_diff: str,
    baseline_command: str,
    candidate_command: str,
) -> BenchmarkReport:
    baseline = await run_in_sandbox(command=baseline_command, proposal_diff=None)
    candidate = await run_in_sandbox(command=candidate_command, proposal_diff=proposal_diff)
    delta = _score(candidate) - _score(baseline)
    return BenchmarkReport(
        baseline=baseline,
        candidate=candidate,
        delta=delta,
        passed=baseline.ok and candidate.ok,
    )
