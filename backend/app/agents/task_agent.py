"""
Safe task agent: maps a fixed set of plain-text commands to predefined docker compose
invocations. Unknown input is rejected. No shell=True; no user-controlled argv.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import sys
import time
from subprocess import CompletedProcess, TimeoutExpired, run
from collections.abc import AsyncIterator
from typing import Any

logger = logging.getLogger("acap.task_agent")

_CMD_TIMEOUT_SEC = 120.0


def _compose_cwd() -> str:
    return os.getenv("TASK_AGENT_COMPOSE_DIR") or os.getcwd()


def _normalize(line: str) -> str:
    return " ".join(line.strip().lower().split())


def _allowed_cmd(line: str) -> list[str] | None:
    """Return argv list for docker compose, or None if not allowed."""
    key = _normalize(line)
    if not key:
        return None
    mapping: dict[str, list[str]] = {
        "restart trading": ["docker", "compose", "restart", "trading"],
        "restart ingestion": ["docker", "compose", "restart", "ingestion"],
        "show trading logs": ["docker", "compose", "logs", "--tail", "100", "trading"],
        "show ingestion logs": ["docker", "compose", "logs", "--tail", "100", "ingestion"],
        "status": ["docker", "compose", "ps"],
    }
    return mapping.get(key)


def _run_safe(argv: list[str]) -> tuple[str, str, int]:
    cwd = _compose_cwd()
    try:
        proc: CompletedProcess[str] = run(
            argv,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=_CMD_TIMEOUT_SEC,
            shell=False,
            check=False,
        )
        out = (proc.stdout or "") + (("\n" + proc.stderr) if proc.stderr else "")
        status = "success" if proc.returncode == 0 else "error"
        return status, out.strip() or "(no output)", proc.returncode
    except TimeoutExpired:
        return "error", f"command timed out after {_CMD_TIMEOUT_SEC}s", -1
    except OSError as e:
        return "error", str(e), -1


def _log_task(command_display: str, status: str, output: str, returncode: int) -> None:
    payload: dict[str, Any] = {
        "command": command_display,
        "status": status,
        "output": output[:32_000],
        "returncode": returncode,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    logger.info("TASK EXECUTED:\n%s", json.dumps(payload, indent=2))


async def _stdin_lines() -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if line == "":
            await asyncio.sleep(0.2)
            continue
        yield line.rstrip("\n\r")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "task_agent ready; allowed: %s",
        "; ".join(
            [
                "restart trading",
                "restart ingestion",
                "show trading logs",
                "show ingestion logs",
                "status",
            ]
        ),
    )
    async for line in _stdin_lines():
        argv = _allowed_cmd(line)
        display = line.strip()
        if argv is None:
            _log_task(
                display or "(empty)",
                "rejected",
                "Unknown or disallowed command. No action taken.",
                -1,
            )
            continue
        status, output, rc = await asyncio.to_thread(_run_safe, argv)
        _log_task(shlex.join(argv), status, output, rc)


if __name__ == "__main__":
    asyncio.run(main())
