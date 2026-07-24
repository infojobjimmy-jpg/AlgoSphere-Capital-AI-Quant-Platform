import asyncio
import logging
from collections.abc import AsyncIterator
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

logger = logging.getLogger(__name__)

_WAIT_INTERVAL_SEC = 2.0

_engine = None
_session_factory = None
_init_lock = asyncio.Lock()


def _db_host_port() -> tuple[str, int]:
    u = urlparse(settings.database_url)
    host = u.hostname or "localhost"
    port = u.port or 5432
    return host, port


async def wait_for_db() -> None:
    """
    Startup synchronization: block until PostgreSQL accepts TCP on the host/port from
    settings.database_url. Retries every 2s, logs each cycle, does not raise.
    """
    host, port = _db_host_port()
    attempt = 0
    while True:
        attempt += 1
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
            logger.info(
                "PostgreSQL ready (TCP %s:%s) after %d wait attempt(s)",
                host,
                port,
                attempt,
            )
            return
        except Exception:
            logger.warning(
                "PostgreSQL not ready (%s:%s); retrying in %.1fs (wait attempt %d)",
                host,
                port,
                _WAIT_INTERVAL_SEC,
                attempt,
            )
            await asyncio.sleep(_WAIT_INTERVAL_SEC)


async def ensure_database() -> None:
    """Wait for Postgres, then create the SQLAlchemy async engine and session factory once."""
    global _engine, _session_factory
    async with _init_lock:
        if _session_factory is not None:
            return
        await wait_for_db()
        _engine = create_async_engine(settings.database_url, echo=False)
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)


class _SessionLocalFactory:
    """async_sessionmaker-compatible callable for `async with SessionLocal()`."""

    def __call__(self, *args, **kwargs):
        if _session_factory is None:
            raise RuntimeError(
                "Database engine not initialized; await ensure_database() before using SessionLocal"
            )
        return _session_factory(*args, **kwargs)


SessionLocal = _SessionLocalFactory()


class Base(DeclarativeBase):
    pass


async def get_session() -> AsyncIterator[AsyncSession]:
    await ensure_database()
    async with SessionLocal() as session:
        yield session
