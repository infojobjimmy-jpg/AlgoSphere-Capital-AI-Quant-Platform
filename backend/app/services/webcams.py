from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)


def load_webcam_registry() -> list[dict[str, Any]]:
    """
    Load user-curated public camera metadata (no automated harvesting).
    """
    path = Path(settings.webcam_registry_path)
    if not path.is_file():
        path = Path(__file__).resolve().parents[2] / "config" / "webcams.example.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return list(data.get("cameras", []))
    except Exception:
        logger.exception("failed to load webcam registry")
        return []
