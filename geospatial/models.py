from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class LiveObject:
    id: str
    kind: str
    latitude: float
    longitude: float
    altitude_m: float = 0.0
    heading_deg: float | None = None
    speed: float | None = None
    label: str = ""
    updated_at: str = field(default_factory=utc_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FeedSnapshot:
    feed: str
    status: str
    objects: list[LiveObject]
    generated_at: str = field(default_factory=utc_now_iso)
    source: str = ""
    message: str = ""
    stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "feed": self.feed,
            "status": self.status,
            "count": len(self.objects),
            "generated_at": self.generated_at,
            "source": self.source,
            "message": self.message,
            "stale": self.stale,
            "objects": [obj.to_dict() for obj in self.objects],
        }
