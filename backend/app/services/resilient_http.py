"""Shared HTTP GET with browser-like headers and exponential backoff (ingestion resilience)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

# Initial attempt + 3 retries; sleep 1s, 2s, 4s after failures 1..3
_BACKOFF_SEC = (1.0, 2.0, 4.0)


async def get_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 40.0,
) -> Any:
    last: BaseException | None = None
    async with httpx.AsyncClient(
        timeout=timeout,
        headers=_DEFAULT_HEADERS,
        follow_redirects=True,
    ) as client:
        for attempt in range(4):
            try:
                r = await client.get(url, params=params)
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                last = exc
                if attempt < 3:
                    delay = _BACKOFF_SEC[attempt]
                    logger.warning(
                        "HTTP GET %s failed (attempt %d/4): %s; retry in %.1fs",
                        url,
                        attempt + 1,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning("HTTP GET %s exhausted retries", url)
    assert last is not None
    raise last


async def get_text(url: str, *, timeout: float = 45.0) -> str:
    last: BaseException | None = None
    async with httpx.AsyncClient(
        timeout=timeout,
        headers=_DEFAULT_HEADERS,
        follow_redirects=True,
    ) as client:
        for attempt in range(4):
            try:
                r = await client.get(url)
                r.raise_for_status()
                return r.text
            except Exception as exc:
                last = exc
                if attempt < 3:
                    delay = _BACKOFF_SEC[attempt]
                    logger.warning(
                        "HTTP GET %s failed (attempt %d/4): %s; retry in %.1fs",
                        url,
                        attempt + 1,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning("HTTP GET %s exhausted retries", url)
    assert last is not None
    raise last
