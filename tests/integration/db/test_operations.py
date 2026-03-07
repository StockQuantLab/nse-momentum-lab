"""Integration tests for database operations.

Run with: doppler run -- uv run pytest tests/integration/db -v

Tests use the schema defined in db/init/001_init.sql. ORM models may have
additional columns not yet in the DB schema - tests use raw SQL where needed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text

from nse_momentum_lab.db.models import (
    JobRun,
    MdOhlcvAdj,
    RefSymbol,
    ScanResult,
    ScanRun,
)

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def setup_test_data(db_session, clean_db):
    """Setup test data for database tests."""
    symbols = [
        RefSymbol(symbol="TEST1", series="EQ", status="ACTIVE"),
        RefSymbol(symbol="TEST2", series="EQ", status="ACTIVE"),
    ]
    for sym in symbols:
        db_session.add(sym)
    await db_session.commit()
    return symbols


async def insert_job_run_raw(
    db_session,
    job_name: str,
    asof_date: date,
    idempotency_key: str,
    status: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    duration_ms: int | None = None,
) -> int:
    """Insert a job_run using raw SQL (avoids ORM-only columns)."""
    result = await db_session.execute(
        text(
            "INSERT INTO nseml.job_run "
            "(job_name, asof_date, idempotency_key, status, started_at, finished_at, duration_ms) "
            "VALUES (:job_name, :asof_date, :idempotency_key, :status, "
            ":started_at, :finished_at, :duration_ms) "
            "RETURNING job_run_id"
        ),
        {
            "job_name": job_name,
            "asof_date": asof_date,
            "idempotency_key": idempotency_key,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
        },
    )
    return result.scalar_one()


class TestDatabaseConnection:
    async def test_database_connection(self, db_session):
        """Test that database connection works."""
        result = await db_session.execute(select(func.now()))
        assert result.scalar() is not None

    async def test_schema_exists(self, db_session):
        """Test that nseml schema exists."""
        result = await db_session.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'nseml'")
        )
        table_count = int(result.scalar() or 0)
        assert table_count > 0


class TestSymbolOperations:
    async def test_insert_symbol(self, db_session, clean_db):
        """Test inserting a symbol."""
        symbol = RefSymbol(symbol="NEWTEST", series="EQ", status="ACTIVE")
        db_session.add(symbol)
        await db_session.commit()

        result = await db_session.execute(select(RefSymbol).where(RefSymbol.symbol == "NEWTEST"))
        found = result.scalar_one()
        assert found.symbol == "NEWTEST"
        assert found.series == "EQ"

    async def test_symbol_unique_constraint(self, db_session, setup_test_data):
        """Test that duplicate symbols raise error."""
        from sqlalchemy.exc import IntegrityError

        duplicate = RefSymbol(symbol="TEST1", series="EQ", status="ACTIVE")
        db_session.add(duplicate)

        with pytest.raises(IntegrityError):
            await db_session.commit()

    async def test_query_active_symbols(self, db_session, setup_test_data):
        """Test querying active symbols."""
        result = await db_session.execute(select(RefSymbol).where(RefSymbol.status == "ACTIVE"))
        symbols = result.scalars().all()
        assert len(symbols) >= 2


class TestOHLCVOperations:
    async def test_insert_ohlcv(self, db_session, setup_test_data):
        """Test inserting OHLCV data."""
        symbols = setup_test_data

        ohlcv = MdOhlcvAdj(
            symbol_id=symbols[0].symbol_id,
            trading_date=date(2024, 1, 15),
            open_adj=100.0,
            high_adj=105.0,
            low_adj=98.0,
            close_adj=102.0,
            volume=1000000,
            adj_factor=1.0,
        )
        db_session.add(ohlcv)
        await db_session.commit()

        result = await db_session.execute(
            select(MdOhlcvAdj).where(
                MdOhlcvAdj.symbol_id == symbols[0].symbol_id,
                MdOhlcvAdj.trading_date == date(2024, 1, 15),
            )
        )
        found = result.scalar_one()
        assert found.close_adj == 102.0

    async def test_ohlcv_range_query(self, db_session, setup_test_data):
        """Test querying OHLCV data by date range."""
        symbols = setup_test_data

        for i in range(5):
            ohlcv = MdOhlcvAdj(
                symbol_id=symbols[0].symbol_id,
                trading_date=date(2024, 1, 15 + i),
                open_adj=100.0 + i,
                high_adj=105.0 + i,
                low_adj=98.0 + i,
                close_adj=102.0 + i,
                volume=1000000,
                adj_factor=1.0,
            )
            db_session.add(ohlcv)
        await db_session.commit()

        result = await db_session.execute(
            select(MdOhlcvAdj)
            .where(MdOhlcvAdj.symbol_id == symbols[0].symbol_id)
            .where(MdOhlcvAdj.trading_date >= date(2024, 1, 16))
            .where(MdOhlcvAdj.trading_date <= date(2024, 1, 18))
            .order_by(MdOhlcvAdj.trading_date)
        )
        records = result.scalars().all()
        assert len(records) == 3


class TestJobRunOperations:
    async def test_insert_job(self, db_session, clean_db):
        """Test inserting a job run."""
        job_run_id = await insert_job_run_raw(
            db_session,
            job_name="test_job",
            asof_date=date(2024, 1, 15),
            idempotency_key="test_key_123",
            status="RUNNING",
            started_at=datetime.now(UTC),
        )
        await db_session.commit()

        assert job_run_id is not None

    async def test_idempotency_key_unique(self, db_session, clean_db):
        """Test that idempotency keys are unique."""
        from sqlalchemy.exc import IntegrityError

        await insert_job_run_raw(
            db_session,
            job_name="test_job",
            asof_date=date(2024, 1, 15),
            idempotency_key="unique_key_123",
            status="COMPLETED",
            started_at=datetime.now(UTC),
        )
        await db_session.commit()

        # Try to insert duplicate idempotency key
        await insert_job_run_raw(
            db_session,
            job_name="test_job",
            asof_date=date(2024, 1, 15),
            idempotency_key="unique_key_123",  # Duplicate key
            status="RUNNING",
            started_at=datetime.now(UTC),
        )

        with pytest.raises(IntegrityError):
            await db_session.commit()

    async def test_update_job_status(self, db_session, clean_db):
        """Test updating job status."""
        job_run_id = await insert_job_run_raw(
            db_session,
            job_name="test_job",
            asof_date=date(2024, 1, 15),
            idempotency_key="update_test_key",
            status="RUNNING",
            started_at=datetime.now(UTC),
        )
        await db_session.commit()

        # Update using raw SQL
        await db_session.execute(
            text(
                "UPDATE nseml.job_run "
                "SET status = :status, finished_at = :finished_at, duration_ms = :duration_ms "
                "WHERE job_run_id = :job_run_id"
            ),
            {
                "status": "COMPLETED",
                "finished_at": datetime.now(UTC),
                "duration_ms": 1000,
                "job_run_id": job_run_id,
            },
        )
        await db_session.commit()

        result = await db_session.execute(
            text("SELECT status, finished_at FROM nseml.job_run WHERE job_run_id = :job_run_id"),
            {"job_run_id": job_run_id},
        )
        row = result.fetchone()
        assert row[0] == "COMPLETED"
        assert row[1] is not None


class TestScanOperations:
    async def test_insert_scan_run(self, db_session, setup_test_data):
        """Test inserting scan run with results."""
        symbols = setup_test_data

        scan_run = ScanRun(
            scan_def_id=1,
            asof_date=date(2024, 1, 15),
            dataset_hash="abcd1234",
            status="COMPLETED",
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
        )
        db_session.add(scan_run)
        await db_session.commit()

        result = ScanResult(
            scan_run_id=scan_run.scan_run_id,
            symbol_id=symbols[0].symbol_id,
            asof_date=date(2024, 1, 15),
            passed=True,
            score=0.85,
            reason_json={"reason": "Test"},
        )
        db_session.add(result)
        await db_session.commit()

        query = await db_session.execute(
            select(ScanResult).where(ScanResult.scan_run_id == scan_run.scan_run_id)
        )
        found = query.scalar_one()
        assert found.passed is True
        assert found.score == 0.85


class TestTransactionHandling:
    async def test_rollback_on_error(self, db_session, setup_test_data):
        """Test that transactions rollback on error."""
        _symbols = setup_test_data  # Fixture ensures data exists

        symbol_count_before = await db_session.execute(select(func.count()).select_from(RefSymbol))
        count_before = symbol_count_before.scalar()

        try:
            new_symbol = RefSymbol(symbol="TEST1", series="EQ", status="ACTIVE")
            db_session.add(new_symbol)
            await db_session.flush()
        except Exception:
            await db_session.rollback()

        symbol_count_after = await db_session.execute(select(func.count()).select_from(RefSymbol))
        count_after = symbol_count_after.scalar()

        assert count_after == count_before

    async def test_commit_with_session_begin(self, db_sessionmaker):
        """Test transaction commit with session.begin()."""
        async with db_sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO nseml.ref_symbol (symbol, series, status) "
                        "VALUES (:symbol, :series, :status)"
                    ),
                    {"symbol": "COMMITTEST", "series": "EQ", "status": "ACTIVE"},
                )

        async with db_sessionmaker() as session:
            result = await session.execute(
                text("SELECT * FROM nseml.ref_symbol WHERE symbol = :symbol"),
                {"symbol": "COMMITTEST"},
            )
            found = result.fetchone()
            assert found is not None
