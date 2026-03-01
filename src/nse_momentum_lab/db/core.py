from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.models import Base

logger = logging.getLogger(__name__)


def create_engine() -> AsyncEngine:
    settings = get_settings()
    url = settings.database_url
    if url is None:
        raise ValueError("database_url is not configured")
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    # Log masked URL to avoid exposing credentials
    logger.info(f"Creating database engine for: {settings.get_masked_database_url()}")
    return create_async_engine(url, pool_pre_ping=True)


_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None
_init_lock: threading.Lock = threading.Lock()


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        with _init_lock:
            if _engine is None:
                _engine = create_engine()
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        engine = get_engine()
        with _init_lock:
            if _sessionmaker is None:
                _sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    return _sessionmaker


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async_session = get_sessionmaker()
    async with async_session() as session:
        yield session


async def init_db() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized")


async def close_db() -> None:
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _sessionmaker = None
    logger.info("Database connection closed")
