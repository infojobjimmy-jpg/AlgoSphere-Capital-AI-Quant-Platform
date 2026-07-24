import asyncio
import logging

from app.config import settings
from app.db.session import SessionLocal, ensure_database
from app.services.camera_store import bootstrap_from_json, list_enabled_cameras, update_camera_probe
from app.services.camera_validation import validate_camera_row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gaios.camera_worker")


async def main() -> None:
    await ensure_database()
    async with SessionLocal() as session:
        await bootstrap_from_json(session)

    while True:
        try:
            async with SessionLocal() as session:
                rows = await list_enabled_cameras(session)
                for r in rows:
                    row = {
                        "slug": r.slug,
                        "stream_url": r.stream_url,
                        "info_url": r.info_url,
                    }
                    res = await validate_camera_row(row)
                    async with SessionLocal() as s2:
                        await update_camera_probe(
                            s2,
                            r.slug,
                            health_state=str(res["health_state"]),
                            latency_ms=res.get("latency_ms"),
                            http_status=res.get("http_status"),
                            stream_category=str(res.get("category")),
                            confidence=float(res.get("conf") or 0.0),
                        )
        except Exception:
            logger.exception("camera probe cycle failed")

        await asyncio.sleep(float(settings.camera_probe_interval_sec))


if __name__ == "__main__":
    asyncio.run(main())
