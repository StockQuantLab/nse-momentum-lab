"""
Feature build progress reporting.

This module provides a durable, file-backed progress stream for feature
materialization jobs. It complements logging by writing structured NDJSON
events that can be tailed from any shell or process wrapper.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT")
DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE = os.getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
DEFAULT_DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", str(min(os.cpu_count() or 2, 4))))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_progress_file() -> Path:
    """Return the default NDJSON progress file path for feature builds."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    return (
        _repo_root()
        / "data"
        / "progress"
        / "feature_builds"
        / (f"feature_build_{timestamp}_{pid}.ndjson")
    )


@dataclass(slots=True)
class FeatureBuildProgressEvent:
    timestamp: str
    run_id: str
    status: str
    stage: str
    message: str
    progress_pct: float | None = None
    step: int | None = None
    step_total: int | None = None
    pending_features: int | None = None
    feature_name: str | None = None
    row_count: int | None = None
    duration_seconds: float | None = None
    error_message: str | None = None


class FeatureBuildProgressReporter:
    """Writes structured progress events to an NDJSON file and logger."""

    def __init__(
        self,
        progress_file: Path | None = None,
        *,
        run_id: str | None = None,
    ) -> None:
        self.progress_file = progress_file or default_progress_file()
        self.run_id = run_id or self.progress_file.stem
        self._lock = Lock()
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Feature build progress file: %s", self.progress_file)

    def emit(
        self,
        *,
        stage: str,
        message: str,
        status: str = "running",
        progress_pct: float | None = None,
        step: int | None = None,
        step_total: int | None = None,
        pending_features: int | None = None,
        feature_name: str | None = None,
        row_count: int | None = None,
        duration_seconds: float | None = None,
        error_message: str | None = None,
    ) -> None:
        event = FeatureBuildProgressEvent(
            timestamp=datetime.now(UTC).isoformat(),
            run_id=self.run_id,
            status=status,
            stage=stage,
            message=message,
            progress_pct=progress_pct,
            step=step,
            step_total=step_total,
            pending_features=pending_features,
            feature_name=feature_name,
            row_count=row_count,
            duration_seconds=duration_seconds,
            error_message=error_message,
        )
        logger.info("[PROGRESS] %s [%s] %s", self.run_id, stage, message)

        payload: dict[str, Any] = asdict(event)
        line = json.dumps(payload, sort_keys=True)
        with self._lock:
            with self.progress_file.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")


def configure_duckdb_for_feature_build(
    con,
    *,
    memory_limit: str | None = None,
    max_temp_directory_size: str | None = None,
    threads: int | None = None,
    preserve_insertion_order: bool = False,
) -> None:
    """Apply safe DuckDB tuning for feature builds.

    The defaults keep memory bounded while preserving deterministic output.
    """

    memory_limit = memory_limit or DEFAULT_DUCKDB_MEMORY_LIMIT
    max_temp_directory_size = max_temp_directory_size or DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE
    threads = threads or DEFAULT_DUCKDB_THREADS

    logger.info(
        (
            "Configuring DuckDB for feature build "
            "(memory_limit=%s, max_temp_directory_size=%s, threads=%d, preserve_insertion_order=%s)"
        ),
        memory_limit or "default",
        max_temp_directory_size or "default",
        threads,
        preserve_insertion_order,
    )
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
    if max_temp_directory_size:
        con.execute(f"SET max_temp_directory_size='{max_temp_directory_size}'")
    con.execute(f"SET preserve_insertion_order={'true' if preserve_insertion_order else 'false'}")
    con.execute(f"SET threads={threads}")
