"""
Read-only exploitation agent: consumes acap.telemetry from Kafka and emits heuristic signals.

Uses shared Kafka helpers from app.kafka_bus (same module as KafkaBus); does not publish or mutate pipelines.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.kafka_bus import make_consumer, wait_for_kafka
from app.memory.chroma_memory import get_chroma_client

logger = logging.getLogger("acap.cortex_agent")

TOPIC = settings.kafka_telemetry_topic
GROUP_ID = f"{settings.app_slug}-cortex-agent"

_AIRCRAFT_CLUSTER_MIN = 400
_SHIPS_CLUSTER_MIN = 12
_SATELLITES_BURST_MIN = 40
_CAMERAS_FIELD_MIN = 8
_CRYPTO_MOVE_PCT = 2.5

_CHROMA_SIGNALS = "acap_cortex_signals"
_last_crypto_price: dict[str, float] = {}
_last_emit_mono: dict[str, float] = defaultdict(float)
_EMIT_COOLDOWN_SEC = 20.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _signal(sig_type: str, confidence: float, source: str) -> dict[str, Any]:
    return {
        "type": sig_type,
        "confidence": round(float(confidence), 4),
        "timestamp": _now_iso(),
        "source": source,
    }


def _emit_ok(signal_key: str) -> bool:
    now = time.monotonic()
    if now - _last_emit_mono[signal_key] < _EMIT_COOLDOWN_SEC:
        return False
    _last_emit_mono[signal_key] = now
    return True


def _signals_for_layer(layer: str, items: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    n = len(items) if isinstance(items, list) else 0

    if layer == "aircraft" and n >= _AIRCRAFT_CLUSTER_MIN:
        key = "AIR_TRAFFIC_CLUSTER"
        if _emit_ok(key):
            conf = min(0.45 + (n / 2500.0) * 0.5, 0.99)
            out.append(_signal(key, conf, "aircraft"))

    elif layer == "ships" and n >= _SHIPS_CLUSTER_MIN:
        key = "MARITIME_ACTIVITY"
        if _emit_ok(key):
            conf = min(0.4 + (n / 80.0) * 0.45, 0.95)
            out.append(_signal(key, conf, "ships"))

    elif layer == "satellites" and n >= _SATELLITES_BURST_MIN:
        key = "SATELLITE_DENSITY"
        if _emit_ok(key):
            conf = min(0.35 + (n / 200.0) * 0.55, 0.92)
            out.append(_signal(key, conf, "satellites"))

    elif layer == "cameras" and n >= _CAMERAS_FIELD_MIN:
        key = "CAMERA_NETWORK_LOAD"
        if _emit_ok(key):
            conf = min(0.3 + (n / 40.0) * 0.5, 0.9)
            out.append(_signal(key, conf, "cameras"))

    elif layer == "market_crypto":
        global _last_crypto_price
        max_move = 0.0
        for row in items if isinstance(items, list) else []:
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol") or "")
            try:
                pf = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            if not sym:
                continue
            prev = _last_crypto_price.get(sym)
            if prev and prev > 0:
                max_move = max(max_move, abs(pf - prev) / prev * 100.0)
            _last_crypto_price[sym] = pf
        if max_move >= _CRYPTO_MOVE_PCT:
            key = "MARKET_VOLATILITY"
            if _emit_ok(key):
                conf = min(0.5 + min(max_move / 15.0, 1.0) * 0.45, 0.95)
                out.append(_signal(key, conf, "market_crypto"))

    return out


def _persist_signal_chroma(sig: dict[str, Any]) -> None:
    if not settings.autonomy_chroma_memory:
        return
    client = get_chroma_client()
    coll = client.get_or_create_collection(
        name=_CHROMA_SIGNALS,
        metadata={"hnsw:space": "cosine"},
    )
    doc = json.dumps(sig, separators=(",", ":"))
    sid = hashlib.sha256(
        f"{sig['timestamp']}|{sig['type']}|{sig['source']}".encode("utf-8"),
    ).hexdigest()[:32]
    coll.upsert(
        ids=[sid],
        documents=[doc],
        metadatas=[
            {
                "type": str(sig["type"]),
                "source": str(sig["source"]),
                "confidence": float(sig["confidence"]),
                "timestamp": str(sig["timestamp"]),
            }
        ],
    )


async def main() -> None:
    await wait_for_kafka()
    consumer = await make_consumer(TOPIC, group_id=GROUP_ID)
    try:
        while True:
            try:
                msg = await consumer.getmany(timeout_ms=1000)
                for _, batch in msg.items():
                    for record in batch:
                        try:
                            payload = json.loads(record.value.decode("utf-8"))
                        except Exception:
                            continue
                        layer = payload.get("layer")
                        if layer not in (
                            "aircraft",
                            "satellites",
                            "ships",
                            "market_crypto",
                            "cameras",
                        ):
                            continue
                        items = payload.get("items") or []
                        if not isinstance(items, list):
                            items = []
                        for sig in _signals_for_layer(str(layer), items):
                            logger.info("cortex_agent signal %s", json.dumps(sig))
                            if settings.autonomy_chroma_memory:
                                try:
                                    await asyncio.to_thread(_persist_signal_chroma, sig)
                                except Exception:
                                    logger.debug("cortex_agent chroma upsert skipped", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("cortex_agent consume tick failed")
                await asyncio.sleep(1.0)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
