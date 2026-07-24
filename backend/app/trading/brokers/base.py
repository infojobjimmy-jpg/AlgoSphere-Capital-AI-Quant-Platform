from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class BrokerError(RuntimeError):
    """Raised when a live broker adapter cannot execute safely."""


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: str
    qty: float
    stop_price: float
    notional_usd: float
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionResult:
    order_id: str
    status: str
    filled_price: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


class BrokerAdapter(Protocol):
    async def execute_order(self, req: OrderRequest) -> ExecutionResult:
        ...

    async def fetch_positions(self) -> list[dict[str, Any]]:
        ...

    async def fetch_account(self) -> dict[str, float]:
        ...
