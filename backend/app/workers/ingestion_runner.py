import asyncio
import logging
from datetime import datetime, timezone

from app.config import settings
from app.kafka_bus import KafkaBus, wait_for_kafka
from app.services.celestrak import fetch_satellites
from app.services.opensky import OPENSKY_MIN_POLL_INTERVAL_SEC, fetch_aircraft_states
from app.services.aisstream import run_ais_stream
from app.services.weather import fetch_global_weather_grid
from app.db.session import SessionLocal, ensure_database
from app.market.binance import fetch_crypto_top_binance as fetch_crypto_top
from app.market.binance_ws import run_binance_ws_stream
from app.market.finnhub import fetch_equity_sample
from app.market.twelve_data import fetch_forex_sample
from app.services.camera_pipeline import fetch_cameras_for_telemetry
from app.services.nhc_storms import fetch_active_storms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("acap.ingestion")

TOPIC = settings.kafka_telemetry_topic


async def emit(bus: KafkaBus, layer: str, items: list) -> None:
    payload = {
        "layer": layer,
        "items": items,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }

    await bus.publish(TOPIC, key=layer, value=payload)

    logger.info(f"✅ Sent {layer} → {len(items)} items to Kafka")


async def loop_aircraft(bus: KafkaBus) -> None:
    while True:
        try:
            states = await fetch_aircraft_states()
            await emit(bus, "aircraft", states[:2500])
        except Exception:
            logger.exception("aircraft ingest failed")
            await emit(bus, "aircraft", [])
        await asyncio.sleep(OPENSKY_MIN_POLL_INTERVAL_SEC)


async def loop_satellites(bus: KafkaBus) -> None:
    while True:
        try:
            sats = await fetch_satellites()
            await emit(bus, "satellites", sats)
        except Exception:
            logger.exception("satellite ingest failed")
            await emit(bus, "satellites", [])
        await asyncio.sleep(60.0)


async def loop_weather(bus: KafkaBus) -> None:
    while True:
        try:
            wx = await fetch_global_weather_grid()
            await emit(bus, "weather", wx)
        except Exception:
            logger.exception("weather ingest failed")
            await emit(bus, "weather", [])
        await asyncio.sleep(120.0)


async def loop_storms(bus: KafkaBus) -> None:
    while True:
        try:
            storms = await fetch_active_storms()
            await emit(bus, "storms", storms)
        except Exception:
            logger.exception("active storm ingest failed")
            await emit(bus, "storms", [])
        await asyncio.sleep(300.0)


async def loop_ships(bus: KafkaBus) -> None:
    async def publish(ships: list) -> None:
        await emit(bus, "ships", ships)

    await run_ais_stream(publish)


async def loop_binance_ws(bus: KafkaBus) -> None:
    async def push(items: list) -> None:
        await emit(bus, "market_crypto", items)

    await run_binance_ws_stream(push)


async def loop_market_crypto(bus: KafkaBus) -> None:
    while True:
        try:
            rows = await fetch_crypto_top()
            await emit(bus, "market_crypto", rows)
        except Exception:
            logger.exception("market_crypto ingest failed")

        # 🔥 FAST MODE FOR TRADING
        await asyncio.sleep(5.0)


async def loop_market_forex(bus: KafkaBus) -> None:
    while True:
        try:
            rows = await fetch_forex_sample()
            if rows:
                await emit(bus, "market_forex", rows)
        except Exception:
            logger.exception("market_forex ingest failed")
        await asyncio.sleep(60.0)


async def loop_market_equities(bus: KafkaBus) -> None:
    while True:
        try:
            rows = await fetch_equity_sample()
            if rows:
                await emit(bus, "market_equities", rows)
        except Exception:
            logger.exception("market_equities ingest failed")
        await asyncio.sleep(90.0)


async def loop_cameras(bus: KafkaBus) -> None:
    while True:
        try:
            async with SessionLocal() as session:
                cams = await fetch_cameras_for_telemetry(session)
            await emit(bus, "cameras", cams)
        except Exception:
            logger.exception("camera pipeline failed")
            await emit(bus, "cameras", [])
        await asyncio.sleep(45.0)


async def main() -> None:
    await ensure_database()
    await wait_for_kafka()
    bus = KafkaBus()

    try:
        geospatial_tasks = [
            loop_aircraft(bus),
            loop_satellites(bus),
            loop_weather(bus),
            loop_storms(bus),
            loop_ships(bus),
            loop_cameras(bus),
        ]
        market_tasks = [] if settings.public_geospatial_mode else [
            loop_market_crypto(bus),
            loop_binance_ws(bus),
            loop_market_forex(bus),
            loop_market_equities(bus),
        ]
        await asyncio.gather(*(geospatial_tasks + market_tasks))
    finally:
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
