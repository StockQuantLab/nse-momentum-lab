"""Shared integration-test fixtures.

Integration tests require:
- Docker Compose running (postgres, minio)
- Doppler for secrets injection
"""

from __future__ import annotations

import asyncio
import os
import sys
import warnings
from collections.abc import AsyncGenerator, Generator
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from httpx import AsyncClient
from minio import Minio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nse_momentum_lab.api.app import create_app
from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    JobRun,
    MdOhlcvAdj,
    RefSymbol,
    ScanResult,
    ScanRun,
)
from nse_momentum_lab.services.ingest.minio import MinioArtifactStore


def pytest_configure(config):
    if sys.platform == "win32":
        # Required for psycopg async tests on Windows; suppress deprecation noise on 3.14+.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="'asyncio.WindowsSelectorEventLoopPolicy' is deprecated and slated for removal in Python 3.16",
                category=DeprecationWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="'asyncio.set_event_loop_policy' is deprecated and slated for removal in Python 3.16",
                category=DeprecationWarning,
            )
            policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
            if policy_cls is not None:
                asyncio.set_event_loop_policy(policy_cls())
    config.addinivalue_line("markers", "integration: marks tests as integration tests")


def _set_integration_env_defaults() -> None:
    os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
    os.environ.setdefault("POSTGRES_PORT", "5434")
    os.environ.setdefault("POSTGRES_USER", "postgres")
    os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
    os.environ.setdefault("POSTGRES_DB", "postgres")

    os.environ.setdefault("MINIO_HOST", "127.0.0.1")
    os.environ.setdefault("MINIO_PORT", "9003")
    os.environ.setdefault("MINIO_ROOT_USER", "minioadmin")
    os.environ.setdefault("MINIO_ROOT_PASSWORD", "minioadmin")
    os.environ.setdefault("MINIO_SECURE", "false")


def _apply_integration_schema_scripts(conn: psycopg.Connection) -> None:
    init_dir = Path(__file__).resolve().parents[2] / "db" / "init"
    for sql_file in sorted(init_dir.glob("*.sql")):
        conn.execute(sql_file.read_text(encoding="utf-8"))
    conn.commit()


@pytest.fixture(scope="session", autouse=True)
def ensure_integration_services_available() -> None:
    """Skip integration suite when required infra/secrets are unavailable.

    This keeps `pytest` (full-suite) deterministic on local machines where
    Docker services or Doppler-injected secrets are not present.

    Validates that core tables from 001_init.sql exist (not ORM-only tables).
    """
    _set_integration_env_defaults()
    settings = get_settings()

    # Required tables from db/init/001_init.sql
    required_tables = {
        "ref_symbol",
        "ref_exchange_calendar",
        "exp_run",
        "job_run",
        "scan_run",
        "scan_result",
        "signal",
        "bt_trade",
    }

    try:
        with psycopg.connect(str(settings.database_url), connect_timeout=3) as conn:
            _apply_integration_schema_scripts(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                # Check for specific required tables
                cur.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'nseml'"
                )
                existing_tables = {row[0] for row in cur.fetchall()}
                missing = required_tables - existing_tables
                if missing:
                    pytest.skip(
                        f"Integration DB schema incomplete. Missing tables: {missing}. "
                        f"Ensure db/init/001_init.sql has been applied."
                    )
    except Exception as exc:
        pytest.skip(f"Integration DB unavailable: {exc}")

    try:
        if settings.minio_endpoint is None:
            pytest.skip("Integration MinIO unavailable: MINIO endpoint missing")
        client = Minio(
            endpoint=str(settings.minio_endpoint).replace("http://", "").replace("https://", ""),
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        client.list_buckets()
    except Exception as exc:
        pytest.skip(f"Integration MinIO unavailable: {exc}")


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def db_sessionmaker() -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    """Create database sessionmaker for integration tests."""
    sessionmaker = get_sessionmaker()
    yield sessionmaker


@pytest_asyncio.fixture
async def db_session(
    db_sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession]:
    """Create a fresh database session for each test."""
    async with db_sessionmaker() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def clean_db(db_sessionmaker: async_sessionmaker[AsyncSession]) -> AsyncGenerator[None]:
    """Clean all mutable tables before each test."""
    async with db_sessionmaker() as session:
        await session.execute(text("SET search_path TO nseml"))
        await session.execute(text("TRUNCATE TABLE scan_result CASCADE"))
        await session.execute(text("TRUNCATE TABLE scan_run CASCADE"))
        await session.execute(text("TRUNCATE TABLE md_ohlcv_adj CASCADE"))
        await session.execute(text("TRUNCATE TABLE md_ohlcv_raw CASCADE"))
        await session.execute(text("TRUNCATE TABLE ref_symbol CASCADE"))
        await session.execute(text("TRUNCATE TABLE job_run CASCADE"))
        await session.execute(text("TRUNCATE TABLE exp_run CASCADE"))
        await session.commit()

    yield


@pytest_asyncio.fixture
async def sample_symbols(db_session: AsyncSession) -> list[RefSymbol]:
    """Create sample symbols for testing."""
    symbols = [
        RefSymbol(symbol="RELIANCE", series="EQ", status="ACTIVE", name="Reliance Industries Ltd"),
        RefSymbol(symbol="TCS", series="EQ", status="ACTIVE", name="Tata Consultancy Services Ltd"),
        RefSymbol(symbol="INFY", series="EQ", status="ACTIVE", name="Infosys Ltd"),
        RefSymbol(symbol="HDFC", series="EQ", status="ACTIVE", name="HDFC Bank Ltd"),
        RefSymbol(symbol="ICICIBANK", series="EQ", status="ACTIVE", name="ICICI Bank Ltd"),
    ]
    for sym in symbols:
        db_session.add(sym)
    await db_session.commit()
    for sym in symbols:
        await db_session.refresh(sym)
    return symbols


@pytest_asyncio.fixture
async def sample_ohlcv(
    db_session: AsyncSession, sample_symbols: list[RefSymbol]
) -> list[MdOhlcvAdj]:
    """Create sample OHLCV data for testing."""
    ohlcv_data = []
    base_prices = {
        "RELIANCE": 2500.0,
        "TCS": 3500.0,
        "INFY": 1500.0,
        "HDFC": 1600.0,
        "ICICIBANK": 900.0,
    }

    for day_offset in range(5):
        trading_date = date(2024, 1, 15 + day_offset)
        for sym in sample_symbols:
            base_price = base_prices.get(sym.symbol, 1000.0)
            variation = 0.02 * (day_offset + 1)

            ohlcv = MdOhlcvAdj(
                symbol_id=sym.symbol_id,
                trading_date=trading_date,
                open_adj=base_price * (1 + variation),
                high_adj=base_price * (1 + variation + 0.01),
                low_adj=base_price * (1 + variation - 0.01),
                close_adj=base_price * (1 + variation + 0.005),
                volume=1000000 + (day_offset * 100000),
                value_traded=base_price * 1000000,
                adj_factor=1.0,
            )
            db_session.add(ohlcv)
            ohlcv_data.append(ohlcv)

    await db_session.commit()
    return ohlcv_data


@pytest_asyncio.fixture
async def sample_scan_run(
    db_session: AsyncSession,
    sample_symbols: list[RefSymbol],
) -> ScanRun:
    """Create a sample scan run with results."""
    from nse_momentum_lab.utils import compute_short_hash

    scan_run = ScanRun(
        scan_def_id=1,
        asof_date=date(2024, 1, 19),
        dataset_hash=compute_short_hash(b"test_dataset", length=16),
        status="COMPLETED",
        started_at=datetime(2024, 1, 19, 18, 0),
        finished_at=datetime(2024, 1, 19, 18, 5),
    )
    db_session.add(scan_run)
    await db_session.commit()
    await db_session.refresh(scan_run)

    for i, sym in enumerate(sample_symbols[:3]):
        result = ScanResult(
            scan_run_id=scan_run.scan_run_id,
            symbol_id=sym.symbol_id,
            asof_date=date(2024, 1, 19),
            passed=True,
            score=0.8 - (i * 0.1),
            reason_json={"reason": "Momentum breakout"},
        )
        db_session.add(result)

    await db_session.commit()
    return scan_run


@pytest_asyncio.fixture
async def sample_job_run(db_session: AsyncSession) -> JobRun:
    """Create a sample job run for testing.

    Note: Only uses columns defined in db/init/001_init.sql.
    New ORM-only columns (job_kind, inputs_json, outputs_json, partition_scope, code_hash)
    are skipped as they don't exist in the current DB schema.
    """
    # Use raw SQL to insert only columns that exist in the current schema
    # This avoids issues with ORM models having columns not yet in the DB
    from sqlalchemy import bindparam, text
    from sqlalchemy.dialects.postgresql import JSONB

    job_name = "daily_pipeline"
    asof_date = date(2024, 1, 19)
    idempotency_key = f"test_idempotency_key_{uuid4().hex}"
    status = "COMPLETED"
    started_at = datetime(2024, 1, 19, 18, 0)
    finished_at = datetime(2024, 1, 19, 18, 10)
    duration_ms = 600000
    metrics_json = {"rows_processed": 1000}

    result = await db_session.execute(
        text(
            "INSERT INTO nseml.job_run "
            "(job_name, asof_date, idempotency_key, status, started_at, finished_at, "
            "duration_ms, metrics_json) "
            "VALUES (:job_name, :asof_date, :idempotency_key, :status, :started_at, "
            ":finished_at, :duration_ms, :metrics_json) "
            "RETURNING job_run_id"
        ).bindparams(bindparam("metrics_json", type_=JSONB)),
        {
            "job_name": job_name,
            "asof_date": asof_date,
            "idempotency_key": idempotency_key,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "metrics_json": metrics_json,
        },
    )
    job_run_id = result.scalar_one()
    await db_session.commit()

    # Create a JobRun object with just the ID for test use
    job = JobRun(
        job_run_id=job_run_id,
        job_name=job_name,
        asof_date=asof_date,
        idempotency_key=idempotency_key,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        metrics_json=metrics_json,
    )
    return job


@pytest_asyncio.fixture
async def api_client() -> AsyncGenerator[AsyncClient]:
    """Create async HTTP client for API testing."""

    app = create_app()

    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def minio_store() -> Generator[MinioArtifactStore]:
    """Create MinIO store for testing."""
    store = MinioArtifactStore()
    yield store
