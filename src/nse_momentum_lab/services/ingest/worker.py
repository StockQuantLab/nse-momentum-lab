"""
Ingestion worker - placeholder for CLI compatibility.

This project uses Zerodha Parquet data accessed via DuckDB.
See nse_momentum_lab.db.market_db for the actual data layer.
No active ingestion is performed.
"""

from dataclasses import dataclass
from datetime import date


@dataclass
class IngestResult:
    """Result of an ingestion operation (always SUCCESS for Parquet mode)."""

    status: str
    rows_processed: int = 0
    rows_quarantined: int = 0
    issues: list[str] | None = None


class IngestionWorker:
    """No-op worker. All market data comes from pre-built Parquet files."""

    def __init__(self):
        """Initialize the no-op worker."""

    async def run(self, trading_date: date) -> IngestResult:
        """Return success - data is already available via Parquet."""
        return IngestResult(
            status="SUCCESS",
            rows_processed=0,
            rows_quarantined=0,
        )

    async def close(self) -> None:
        """No-op - no resources to clean up."""
