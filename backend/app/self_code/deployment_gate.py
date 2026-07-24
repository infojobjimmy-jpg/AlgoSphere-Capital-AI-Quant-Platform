from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from app.config import settings
from app.self_code.benchmark_engine import BenchmarkReport


@dataclass(slots=True)
class DeploymentDecision:
    approved: bool
    deployed: bool
    reason: str


def _apply_patch_to_workspace(diff_text: str) -> bool:
    root = Path(settings.self_code_workspace_root).resolve()
    patch_file = root / ".self_code_apply.patch"
    patch_file.write_text(diff_text, encoding="utf-8")
    cp = subprocess.run(  # noqa: S603
        f'git apply "{patch_file}"',
        cwd=str(root),
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
    )
    if cp.returncode == 0:
        return True
    cp2 = subprocess.run(  # noqa: S603
        f'patch -p1 -i "{patch_file}"',
        cwd=str(root),
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
    )
    return cp2.returncode == 0


def evaluate_deployment_gate(report: BenchmarkReport, proposal_diff: str) -> DeploymentDecision:
    if not report.passed:
        return DeploymentDecision(approved=False, deployed=False, reason="benchmark_failed")
    if report.delta < float(settings.self_code_min_improvement):
        return DeploymentDecision(approved=False, deployed=False, reason="insufficient_improvement")
    if settings.self_code_dry_run:
        return DeploymentDecision(approved=True, deployed=False, reason="approved_dry_run")
    deployed = _apply_patch_to_workspace(proposal_diff)
    if not deployed:
        return DeploymentDecision(approved=False, deployed=False, reason="deploy_apply_failed")
    return DeploymentDecision(approved=True, deployed=True, reason="deployed")
