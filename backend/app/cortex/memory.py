"""Episodic memory hook — Chroma upsert after the full cognitive tick (includes plan state)."""

from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.memory import chroma_memory

logger = logging.getLogger(__name__)


async def remember_episode(snap: dict[str, Any]) -> None:
    if not settings.autonomy_chroma_memory:
        return
    try:
        await chroma_memory.upsert_episode(snap)
    except Exception:
        logger.exception("cortex memory upsert failed")
