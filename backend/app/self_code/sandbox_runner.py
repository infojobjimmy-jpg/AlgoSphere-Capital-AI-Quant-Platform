from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from app.config import settings


@dataclass(slots=True)
class SandboxResult:
    ok: bool
    command: str
    returncode: int
    duration_sec: float
    stdout: str
    stderr: str


def _clip(s: str, n: int = 4000) -> str:
    return s if len(s) <= n else s[: n - 24] + "\n...[truncated]..."


def _run_cmd(cwd: Path, cmd: str, timeout_sec: int) -> SandboxResult:
    import time

    started = time.monotonic()
    cp = subprocess.run(  # noqa: S603
        cmd,
        cwd=str(cwd),
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    elapsed = time.monotonic() - started
    return SandboxResult(
        ok=cp.returncode == 0,
        command=cmd,
        returncode=cp.returncode,
        duration_sec=elapsed,
        stdout=_clip(cp.stdout or ""),
        stderr=_clip(cp.stderr or ""),
    )


def _apply_diff(workdir: Path, diff_text: str) -> bool:
    patch_file = workdir / ".candidate.patch"
    patch_file.write_text(diff_text, encoding="utf-8")
    # Try git apply first, then patch fallback.
    cp = subprocess.run(  # noqa: S603
        f'git apply "{patch_file}"',
        cwd=str(workdir),
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
    )
    if cp.returncode == 0:
        return True
    cp2 = subprocess.run(  # noqa: S603
        f'patch -p1 -i "{patch_file}"',
        cwd=str(workdir),
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
    )
    return cp2.returncode == 0


async def run_in_sandbox(*, command: str, proposal_diff: str | None = None) -> SandboxResult:
    root = Path(settings.self_code_workspace_root).resolve()
    if not root.exists():
        raise RuntimeError(f"self_code_workspace_missing:{root}")
    timeout_sec = max(10, int(settings.self_code_command_timeout_sec))
    with tempfile.TemporaryDirectory(prefix="acap-selfcode-") as td:
        box = Path(td)
        shutil.copytree(root, box / "repo", dirs_exist_ok=True)
        repo = box / "repo"
        if proposal_diff and proposal_diff.strip():
            if not _apply_diff(repo, proposal_diff):
                return SandboxResult(
                    ok=False,
                    command=command,
                    returncode=2,
                    duration_sec=0.0,
                    stdout="",
                    stderr="failed to apply proposal diff in sandbox",
                )
        return await asyncio.to_thread(_run_cmd, repo, command, timeout_sec)
