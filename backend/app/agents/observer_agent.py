"""
Read-only diagnostic observer: ingests log lines (stdin and/or files), detects recurring
failure patterns, and emits structured suggestions. Does not execute commands, modify
files, or restart services.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

logger = logging.getLogger("acap.observer_agent")

_INSIGHT_COOLDOWN_SEC = 120.0
_NO_TRADE_WARN_SEC = 600.0
_NO_TRADE_SEVERE_SEC = 1_800.0


@dataclass
class _ObserverState:
    """Process-local state for dedupe and trading-activity heuristics."""

    last_trade_mono: float = field(default_factory=time.monotonic)
    last_insight_mono: dict[str, float] = field(default_factory=dict)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit_insight(state: _ObserverState, payload: dict[str, Any]) -> None:
    key = str(payload.get("issue", "unknown"))
    now = time.monotonic()
    prev = state.last_insight_mono.get(key, 0.0)
    if now - prev < _INSIGHT_COOLDOWN_SEC:
        return
    state.last_insight_mono[key] = now
    body = {**payload, "observed_at": _now_iso()}
    logger.info("AI OBSERVER:\n%s", json.dumps(body, indent=2))


def _analyze_line(line: str, state: _ObserverState) -> None:
    s = line.strip()
    if not s:
        return

    low = s.lower()

    if "trade decision" in low or ('"decision"' in low and "trading_agent" in low):
        state.last_trade_mono = time.monotonic()
    if low.startswith("trade:") or "trading_agent signal" in low:
        state.last_trade_mono = time.monotonic()

    if re.search(r"kafka.*not ready|not ready.*kafka", low):
        _emit_insight(
            state,
            {
                "issue": "Kafka broker not ready (log pattern)",
                "severity": "medium",
                "suggestion": "Ensure wait_for_kafka() runs before consumers/producers start; verify "
                "KAFKA_BOOTSTRAP and that the broker container is healthy.",
                "source_line_hint": s[:240],
            },
        )
    if "connectionrefusederror" in low.replace(" ", "") and ("9092" in s or "kafka" in low):
        _emit_insight(
            state,
            {
                "issue": "Kafka connection refused",
                "severity": "high",
                "suggestion": "Check broker is listening on the advertised listener and that clients use "
                "the correct host:port for the network (e.g. kafka:9092 inside Compose).",
                "source_line_hint": s[:240],
            },
        )

    if "connectionrefusederror" in low.replace(" ", "") and ("5432" in s or "@db:" in low or " db " in low):
        _emit_insight(
            state,
            {
                "issue": "PostgreSQL connection refused",
                "severity": "high",
                "suggestion": "Confirm Postgres is up before workers open DB sessions; use startup wait "
                "or depends_on with application-side retry.",
                "source_line_hint": s[:240],
            },
        )

    if "opensky" in low and ("429" in s or "too many requests" in low):
        _emit_insight(
            state,
            {
                "issue": "OpenSky rate limiting (HTTP 429)",
                "severity": "low",
                "suggestion": "Reduce poll frequency and add backoff on 429; confirm simulated "
                "fallback is active so aircraft layer is not empty.",
                "source_line_hint": s[:240],
            },
        )
    if "no usable" in low and ("aircraft" in low or "states" in low):
        _emit_insight(
            state,
            {
                "issue": "Empty or unusable aircraft telemetry",
                "severity": "medium",
                "suggestion": "Verify upstream API health, rate limits, and fallback ingestion paths.",
                "source_line_hint": s[:240],
            },
        )
    if "ingest" in low and ("returned no rows" in low or "no rows" in low):
        _emit_insight(
            state,
            {
                "issue": "Ingestion produced empty dataset",
                "severity": "low",
                "suggestion": "Check external API keys, quotas, and network egress from the worker container.",
                "source_line_hint": s[:240],
            },
        )

    if "paper trade skipped" in low:
        if "drawdown" in low:
            _emit_insight(
                state,
                {
                    "issue": "Paper trades skipped due to drawdown gate",
                    "severity": "low",
                    "suggestion": "Expected protective behavior; review risk thresholds only if policy "
                    "should change.",
                    "source_line_hint": s[:240],
                },
            )
        elif "kill_switch" in low or "kill switch" in low:
            _emit_insight(
                state,
                {
                    "issue": "Trading kill switch engaged",
                    "severity": "medium",
                    "suggestion": "Confirm TRADING_KILL_SWITCH / Redis kill key is intentional before "
                    "expecting new paper orders.",
                    "source_line_hint": s[:240],
                },
            )


def _periodic_checks(state: _ObserverState) -> None:
    now = time.monotonic()
    idle = now - state.last_trade_mono
    if idle > _NO_TRADE_SEVERE_SEC:
        _emit_insight(
            state,
            {
                "issue": "No trading activity for an extended period",
                "severity": "medium",
                "suggestion": "Verify market_crypto telemetry is flowing, trading_agent is running, and "
                "signal rules still see sufficient ticks.",
            },
        )
    elif idle > _NO_TRADE_WARN_SEC:
        _emit_insight(
            state,
            {
                "issue": "Low trading log activity",
                "severity": "low",
                "suggestion": "If intentional (quiet market), no action. Otherwise confirm Kafka topic "
                "acap.telemetry and trading_agent consumer group.",
            },
        )


def _log_paths_from_env() -> list[Path]:
    raw = (os.getenv("OBSERVER_LOG_FILES") or "").strip()
    if not raw:
        return []
    return [Path(p.strip()) for p in raw.split(",") if p.strip()]


async def _stdin_lines() -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if line == "":
            await asyncio.sleep(0.25)
            continue
        yield line.rstrip("\n\r")


async def _tail_file(path: Path) -> AsyncIterator[str]:
    pos = 0
    if path.is_file():
        pos = path.stat().st_size
    while True:
        if not path.is_file():
            await asyncio.sleep(1.0)
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(pos)
                chunk = fh.read()
                pos = fh.tell()
        except OSError:
            await asyncio.sleep(1.0)
            continue
        if chunk:
            for part in chunk.splitlines():
                yield f"[{path.name}] {part}"
        await asyncio.sleep(1.0)


async def _merge_sources(paths: list[Path]) -> AsyncIterator[str]:
    q: asyncio.Queue[str | None] = asyncio.Queue()

    async def push_stdin() -> None:
        async for line in _stdin_lines():
            await q.put(line)

    async def push_file(p: Path) -> None:
        async for line in _tail_file(p):
            await q.put(line)

    tasks: list[asyncio.Task[Any]] = []
    if not sys.stdin.isatty():
        tasks.append(asyncio.create_task(push_stdin()))
    for p in paths:
        tasks.append(asyncio.create_task(push_file(p)))
    try:
        while True:
            item = await q.get()
            if item is None:
                break
            yield item
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _periodic_loop(state: _ObserverState) -> None:
    while True:
        await asyncio.sleep(30.0)
        _periodic_checks(state)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    paths = _log_paths_from_env()
    if sys.stdin.isatty() and not paths:
        logger.info(
            "AI OBSERVER:\n%s",
            json.dumps(
                {
                    "issue": "observer_no_input_configured",
                    "severity": "low",
                    "suggestion": "Pipe container logs on stdin, e.g. "
                    "`docker compose logs -f kafka ingestion trading 2>&1 | python -m app.agents.observer_agent`, "
                    "or set OBSERVER_LOG_FILES to comma-separated file paths readable from this process.",
                    "observed_at": _now_iso(),
                },
                indent=2,
            ),
        )
        return

    state = _ObserverState()
    ticker = asyncio.create_task(_periodic_loop(state))
    try:
        async for line in _merge_sources(paths):
            _analyze_line(line, state)
    finally:
        ticker.cancel()
        try:
            await ticker
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
