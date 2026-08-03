import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings

logger = logging.getLogger(__name__)

_WAIT_INTERVAL_SEC = 2.0


def _bootstrap_hosts() -> list[tuple[str, int]]:
    raw = (settings.kafka_bootstrap or "").strip()
    if not raw:
        return [("localhost", 9092)]
    hosts: list[tuple[str, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            host, port_s = part.rsplit(":", 1)
            try:
                hosts.append((host.strip(), int(port_s.strip())))
            except ValueError:
                hosts.append((host.strip(), 9092))
        else:
            hosts.append((part, 9092))
    return hosts or [("localhost", 9092)]


async def probe_kafka_tcp(*, timeout: float = 3.0) -> bool:
    """True if at least one bootstrap broker accepts TCP (read-only health check)."""
    for host, port in _bootstrap_hosts():
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            continue
    return False


def _sleep_before_attempt(attempt: int) -> float:
    """Backoff: first probe has no sleep; then 2s, 2.5s, 3s, … capped at 60s."""
    if attempt <= 1:
        return 0.0
    return min(60.0, 2.0 + 0.5 * (attempt - 2))


async def wait_for_kafka(*, max_attempts: int | None = None) -> None:
    """
    Startup synchronization: block until at least one Kafka broker accepts TCP connections.

    Uses a lightweight TCP probe (not consumer/producer startup).

    - ``max_attempts=None`` (default): retry indefinitely with backoff (shared bus / producers).
    - ``max_attempts=N``: at most N attempts; raises ``RuntimeError`` if the broker never accepts TCP.
    """
    hosts = _bootstrap_hosts()
    attempt = 0
    while True:
        if max_attempts is not None and attempt >= max_attempts:
            msg = (
                f"Kafka unreachable after {max_attempts} attempts "
                f"(bootstrap={settings.kafka_bootstrap!r})"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        attempt += 1
        if attempt > 1:
            await asyncio.sleep(_sleep_before_attempt(attempt))

        if max_attempts is None:
            if attempt <= 5 or attempt % 30 == 0:
                logger.info("Waiting for Kafka... attempt %d", attempt)
        else:
            logger.info("Waiting for Kafka... attempt %d", attempt)

        for host, port in hosts:
            try:
                _reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=5.0,
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                if max_attempts is not None:
                    logger.info("Kafka is ready")
                logger.info(
                    "Kafka ready (TCP %s:%s); bootstrap=%s after %d wait attempt(s)",
                    host,
                    port,
                    settings.kafka_bootstrap,
                    attempt,
                )
                return
            except Exception:
                continue


def _json_dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class KafkaBus:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def producer(self) -> AIOKafkaProducer:
        while self._producer is None:
            await wait_for_kafka()
            p = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap,
                compression_type="gzip",
            )
            try:
                await p.start()
                self._producer = p
            except Exception:
                logger.warning(
                    "Kafka producer start failed (bootstrap=%s); will re-wait and retry",
                    settings.kafka_bootstrap,
                    exc_info=True,
                )
                try:
                    await p.stop()
                except Exception:
                    pass
                await asyncio.sleep(_WAIT_INTERVAL_SEC)
        return self._producer

    async def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        p = await self.producer()
        await p.send_and_wait(
            topic,
            key=key.encode("utf-8") if key else None,
            value=_json_dumps(value),
        )

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None


async def make_consumer(
    *topics: str,
    group_id: str = "gaios-bridge",
) -> AIOKafkaConsumer:
    await wait_for_kafka()
    while True:
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        try:
            await consumer.start()
            return consumer
        except Exception:
            logger.warning(
                "Kafka consumer start failed (bootstrap=%s); re-waiting and retrying",
                settings.kafka_bootstrap,
                exc_info=True,
            )
            try:
                await consumer.stop()
            except Exception:
                pass
            await wait_for_kafka()
            await asyncio.sleep(_WAIT_INTERVAL_SEC)
