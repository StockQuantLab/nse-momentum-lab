"""
Ingestion worker - now delegates to real incremental pipeline.

The old no-op interface is preserved for backward compatibility.
Real ingestion is now handled by IngestionPipeline in pipeline.py.
"""

from dataclasses import dataclass
from datetime import date

from nse_momentum_lab.services.ingest.pipeline import (
    IngestionWorker as PipelineWorker,
)


@dataclass
class IngestResult:
    """Result of an ingestion operation."""

    status: str
    rows_processed: int = 0
    rows_quarantined: int = 0
    issues: list[str] | None = None
    job_id: int | None = None


class IngestionWorker:
    """
    Ingestion worker - now backed by real incremental pipeline.

    For backward compatibility, maintains the original no-op interface
    but delegates to IngestionPipeline when configured with incremental mode.
    """

    def __init__(self, session=None, use_incremental: bool = False):
        """
        Initialize the ingestion worker.

        Args:
            session: Postgres session for incremental mode
            use_incremental: If False, maintains no-op behavior for compatibility
        """
        self._use_incremental = use_incremental
        self._session = session
        self._pipeline: PipelineWorker | None = None

        if use_incremental and session:
            self._pipeline = PipelineWorker(session)

    async def run(self, trading_date: date) -> IngestResult:
        """
        Run ingestion for a trading date.

        If use_incremental=False (default), returns success for compatibility.
        If use_incremental=True, delegates to real pipeline.
        """
        if self._pipeline:
            result = await self._pipeline.run(trading_date)
            return IngestResult(
                status=result.status.value,
                rows_processed=result.rows_processed,
                rows_quarantined=result.rows_quarantined,
                issues=result.issues if result.issues else None,
                job_id=result.job_id,
            )

        # No-op mode for backward compatibility
        return IngestResult(
            status="SUCCESS",
            rows_processed=0,
            rows_quarantined=0,
        )

    async def close(self) -> None:
        """Clean up resources."""
        if self._pipeline:
            await self._pipeline.close()
