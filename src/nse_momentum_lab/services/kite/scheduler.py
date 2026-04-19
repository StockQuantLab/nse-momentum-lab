from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.kite.auth import KiteAuth, get_kite_auth
from nse_momentum_lab.services.kite.writer import KiteWriter, get_kite_writer
from nse_momentum_lab.utils.constants import IngestionDataset, IngestionUniverse

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "raw" / "kite" / "checkpoints"
PARQUET_DAILY_DIR = PROJECT_ROOT / "data" / "parquet" / "daily"
CHECKPOINT_FLUSH_EVERY = 25
PROGRESS_LOG_EVERY = 10
BACKFILL_START_DATE = date(2025, 4, 1)
MAX_FETCH_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # seconds; doubles each attempt (2s, 4s, ...)


def _is_transient_error(exc: Exception) -> bool:
    """Return True for API errors worth retrying (rate limits, server errors, timeouts)."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError)):
        return True
    return False


LOCK_FILE_PATH = PROJECT_ROOT / "data" / "raw" / "kite" / "checkpoints" / ".ingestion.lock"

PLATFORM_HAS_LOCK = False

if os.name == "nt":
    try:
        import msvcrt

        PLATFORM_HAS_LOCK = True
    except ImportError:
        msvcrt = None


def _try_acquire_lock() -> tuple[int, bool]:
    """Attempt to acquire file lock. Returns (fd, acquired)."""
    if not PLATFORM_HAS_LOCK or msvcrt is None:
        return -1, True
    try:
        LOCK_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(LOCK_FILE_PATH), os.O_CREAT | os.O_RDWR)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        return fd, True
    except OSError:
        return -1, False


def _release_lock(fd: int) -> None:
    """Release file lock and close fd."""
    if fd < 0:
        return
    if PLATFORM_HAS_LOCK and msvcrt is not None:
        try:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    try:
        os.close(fd)
    except OSError:
        pass


@dataclass(slots=True)
class CheckpointState:
    path: Path
    completed_symbols: set[str]


class KiteScheduler:
    def __init__(self, auth: KiteAuth | None = None, writer: KiteWriter | None = None) -> None:
        self.auth = auth or get_kite_auth()
        self.writer = writer or get_kite_writer()
        self._local_parquet_symbols_cache: list[str] | None = None

    def get_ingestion_status(self) -> dict[str, Any]:
        market_db = get_market_db(read_only=True)
        daily_range = None
        five_min_range = None
        if getattr(market_db, "_has_daily", False):
            row = market_db.con.execute(
                "SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM v_daily"
            ).fetchone()
            if row:
                daily_range = {
                    "min_date": row[0].isoformat() if row[0] else None,
                    "max_date": row[1].isoformat() if row[1] else None,
                    "symbols": int(row[2] or 0),
                }
        if getattr(market_db, "_has_5min", False):
            row = market_db.con.execute(
                "SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM v_5min"
            ).fetchone()
            if row:
                five_min_range = {
                    "min_date": row[0].isoformat() if row[0] else None,
                    "max_date": row[1].isoformat() if row[1] else None,
                    "symbols": int(row[2] or 0),
                }

        instrument_cache = self.auth.get_instrument_master_path("NSE")
        return {
            "authenticated": self.auth.is_authenticated(),
            "instrument_cache_path": str(instrument_cache),
            "instrument_cache_exists": instrument_cache.exists(),
            "daily": daily_range,
            "5min": five_min_range,
        }

    def get_symbols_from_local_parquet(self) -> list[str]:
        if self._local_parquet_symbols_cache is not None:
            return list(self._local_parquet_symbols_cache)
        if not PARQUET_DAILY_DIR.exists():
            return []
        symbols: list[str] = []
        for child in sorted(PARQUET_DAILY_DIR.iterdir()):
            if not child.is_dir():
                continue
            if (child / "all.parquet").exists() or (child / "kite.parquet").exists():
                symbols.append(child.name.strip().upper())
        self._local_parquet_symbols_cache = symbols
        return list(symbols)

    def get_symbols_from_kite(
        self,
        *,
        exchange: str = "NSE",
        segment: str = "NSE",
        refresh: bool = False,
    ) -> list[str]:
        if refresh:
            self.auth.refresh_instruments(exchange)
        rows = self.auth.get_instruments(exchange)
        symbols: list[str] = []
        for row in rows:
            tradingsymbol = str(row.get("tradingsymbol") or "").strip().upper()
            row_segment = str(row.get("segment") or "").strip().upper()
            if not tradingsymbol:
                continue
            if segment and row_segment and row_segment != segment.strip().upper():
                continue
            symbols.append(tradingsymbol)
        return list(dict.fromkeys(symbols))

    def run_daily_ingestion(
        self,
        symbols: list[str] | None = None,
        trading_date: date | None = None,
        update_features: bool = False,
        save_raw: bool = False,
        resume: bool = True,
        universe: IngestionUniverse = IngestionUniverse.LOCAL_FIRST,
    ) -> dict[str, Any]:
        target_date = trading_date or datetime.now(UTC).date()
        return self.run_daily_range_ingestion(
            symbols=symbols,
            start_date=target_date,
            end_date=target_date,
            update_features=update_features,
            save_raw=save_raw,
            resume=resume,
            universe=universe,
        )

    def run_daily_range_ingestion(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        update_features: bool = False,
        mode: str = "append",
        save_raw: bool = False,
        resume: bool = True,
        universe: IngestionUniverse = IngestionUniverse.LOCAL_FIRST,
    ) -> dict[str, Any]:
        start = start_date or datetime.now(UTC).date()
        end = end_date or start
        return self._run_ingestion(
            dataset=IngestionDataset.DAILY,
            symbols=symbols,
            start_date=start,
            end_date=end,
            save_raw=save_raw,
            resume=resume,
            mode=mode,
            update_features=update_features,
            universe=universe,
        )

    def run_5min_ingestion(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        save_raw: bool = False,
        resume: bool = True,
        universe: IngestionUniverse = IngestionUniverse.LOCAL_FIRST,
    ) -> dict[str, Any]:
        start = start_date or BACKFILL_START_DATE
        end = end_date or datetime.now(UTC).date()
        return self._run_ingestion(
            dataset=IngestionDataset.FIVE_MIN,
            symbols=symbols,
            start_date=start,
            end_date=end,
            save_raw=save_raw,
            resume=resume,
            mode="append",
            update_features=False,
            universe=universe,
        )

    def _run_ingestion(
        self,
        *,
        dataset: IngestionDataset,
        symbols: list[str] | None,
        start_date: date,
        end_date: date,
        save_raw: bool,
        resume: bool,
        mode: str,
        update_features: bool,
        universe: IngestionUniverse,
    ) -> dict[str, Any]:
        lock_fd, acquired = _try_acquire_lock()
        if not acquired:
            logger.warning("Kite ingestion already running; skipping")
            return {"error": "concurrent_ingestion_blocked"}
        try:
            return self._run_ingestion_inner(
                dataset=dataset,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                save_raw=save_raw,
                resume=resume,
                mode=mode,
                update_features=update_features,
                universe=universe,
            )
        finally:
            _release_lock(lock_fd)

    def _run_ingestion_inner(
        self,
        *,
        dataset: IngestionDataset,
        symbols: list[str] | None,
        start_date: date,
        end_date: date,
        save_raw: bool,
        resume: bool,
        mode: str,
        update_features: bool,
        universe: IngestionUniverse,
    ) -> dict[str, Any]:
        resolved_symbols = self._resolve_symbols(
            symbols=symbols,
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            universe=universe,
        )
        checkpoint = self._load_checkpoint(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            resume=resume,
            universe=universe,
        )
        pending_symbols = [
            symbol for symbol in resolved_symbols if symbol not in checkpoint.completed_symbols
        ]

        summary: dict[str, Any] = {
            "dataset": dataset.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_symbols": len(resolved_symbols),
            "universe": universe.value,
            "processed_symbols": 0,
            "succeeded": 0,
            "failed": 0,
            "zero_rows": 0,
            "missing_tokens": [],
            "errors": [],
            "checkpoint_path": str(checkpoint.path),
            "checkpoint_cleared": False,
        }

        if not pending_symbols:
            logger.info(
                "Kite %s ingestion already complete for %s to %s; nothing pending.",
                dataset.value,
                start_date,
                end_date,
            )
            summary["checkpoint_cleared"] = True
            self._clear_checkpoint(checkpoint.path)
            return summary

        logger.info(
            "Starting Kite %s ingestion for %s to %s: total=%d pending=%d resume=%s checkpoint=%s",
            dataset.value,
            start_date,
            end_date,
            len(resolved_symbols),
            len(pending_symbols),
            resume,
            checkpoint.path,
        )

        completed_since_flush = 0
        started_at = time.monotonic()
        for symbol in pending_symbols:
            summary["processed_symbols"] += 1
            try:
                if self.auth.get_instrument_token(symbol) is None:
                    summary["failed"] += 1
                    summary["missing_tokens"].append(symbol)
                    logger.warning(
                        "Skipping %s for Kite %s ingestion: missing instrument token (%d/%d)",
                        symbol,
                        dataset.value,
                        summary["processed_symbols"],
                        len(pending_symbols),
                    )
                    self._log_progress(dataset, summary, len(pending_symbols), started_at)
                    continue

                records = self._fetch_with_retry(
                    dataset=dataset,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    mode=mode,
                    save_raw=save_raw,
                )

                if records > 0:
                    summary["succeeded"] += 1
                else:
                    summary["failed"] += 1
                    summary["zero_rows"] += 1
                checkpoint.completed_symbols.add(symbol)
                completed_since_flush += 1
                if resume and completed_since_flush >= CHECKPOINT_FLUSH_EVERY:
                    self._persist_checkpoint(checkpoint)
                    logger.info(
                        "Persisted Kite %s checkpoint after %d processed symbols: %s",
                        dataset.value,
                        summary["processed_symbols"],
                        checkpoint.path,
                    )
                    completed_since_flush = 0
                self._log_progress(dataset, summary, len(pending_symbols), started_at)
            except Exception as exc:
                logger.exception("Kite %s ingestion failed for %s", dataset.value, symbol)
                summary["failed"] += 1
                summary["errors"].append({"symbol": symbol, "error": str(exc)})
                self._log_progress(dataset, summary, len(pending_symbols), started_at)

        if resume and checkpoint.completed_symbols:
            self._persist_checkpoint(checkpoint)

        if resume and len(checkpoint.completed_symbols) >= len(resolved_symbols):
            summary["checkpoint_cleared"] = True
            self._clear_checkpoint(checkpoint.path)

        if dataset is IngestionDataset.DAILY and update_features and summary["succeeded"] > 0:
            self._refresh_features(summary, start_date=start_date)

        logger.info(
            "Completed Kite %s ingestion for %s to %s: processed=%d/%d succeeded=%d failed=%d zero_rows=%d elapsed=%.1fs",
            dataset.value,
            start_date,
            end_date,
            summary["processed_symbols"],
            len(pending_symbols),
            summary["succeeded"],
            summary["failed"],
            summary["zero_rows"],
            time.monotonic() - started_at,
        )
        return summary

    def _fetch_with_retry(
        self,
        *,
        dataset: IngestionDataset,
        symbol: str,
        start_date: date,
        end_date: date,
        mode: str,
        save_raw: bool,
    ) -> int:
        """Fetch and write data with exponential-backoff retry for transient errors."""
        last_exc: Exception | None = None
        for attempt in range(1, MAX_FETCH_RETRIES + 1):
            try:
                if dataset is IngestionDataset.DAILY:
                    return self.writer.fetch_and_write_daily(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        mode=mode,
                        save_raw=save_raw,
                    )
                return self.writer.fetch_and_write_5min(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    mode=mode,
                    save_raw=save_raw,
                )
            except Exception as exc:
                last_exc = exc
                if attempt < MAX_FETCH_RETRIES and _is_transient_error(exc):
                    backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(
                        "Kite %s transient error for %s (attempt %d/%d), retrying in %.1fs: %s",
                        dataset.value,
                        symbol,
                        attempt,
                        MAX_FETCH_RETRIES,
                        backoff,
                        exc,
                    )
                    time.sleep(backoff)
                else:
                    raise
        assert last_exc is not None
        raise last_exc  # pragma: no cover — safeguard

    def _resolve_symbols(
        self,
        *,
        symbols: list[str] | None,
        dataset: IngestionDataset,
        start_date: date,
        end_date: date,
        universe: IngestionUniverse = IngestionUniverse.LOCAL_FIRST,
    ) -> list[str]:
        if symbols:
            cleaned = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
            return list(dict.fromkeys(cleaned))

        if universe is IngestionUniverse.CURRENT_MASTER:
            kite_symbols = self.get_symbols_from_kite(exchange="NSE", segment="NSE", refresh=True)
            logger.info(
                "Resolved Kite ingestion universe from current Kite master: %d",
                len(kite_symbols),
            )
            return kite_symbols

        local_symbols = self.get_symbols_from_local_parquet()
        kite_symbols = self.get_symbols_from_kite(exchange="NSE", segment="NSE")
        if local_symbols and kite_symbols:
            kite_symbol_set = set(kite_symbols)
            intersected = [symbol for symbol in local_symbols if symbol in kite_symbol_set]
            if intersected:
                logger.info(
                    "Resolved Kite ingestion universe via local/current intersection: local=%d current=%d selected=%d",
                    len(local_symbols),
                    len(kite_symbols),
                    len(intersected),
                )
                return intersected
        if local_symbols:
            logger.info(
                "Resolved Kite ingestion universe from local parquet symbols: %d",
                len(local_symbols),
            )
            return local_symbols
        logger.info(
            "Resolved Kite ingestion universe from current Kite instruments: %d", len(kite_symbols)
        )
        return kite_symbols

    def _checkpoint_path(
        self,
        *,
        dataset: IngestionDataset,
        start_date: date,
        end_date: date,
        universe: IngestionUniverse,
    ) -> Path:
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        return (
            CHECKPOINT_DIR
            / f"{dataset.value}_{universe.value}_{start_date.isoformat()}_{end_date.isoformat()}.json"
        )

    def _load_checkpoint(
        self,
        *,
        dataset: IngestionDataset,
        start_date: date,
        end_date: date,
        resume: bool,
        universe: IngestionUniverse,
    ) -> CheckpointState:
        path = self._checkpoint_path(
            dataset=dataset, start_date=start_date, end_date=end_date, universe=universe
        )
        if not resume or not path.exists():
            return CheckpointState(path=path, completed_symbols=set())
        payload = json.loads(path.read_text(encoding="utf-8"))
        completed = {
            str(symbol).strip().upper()
            for symbol in payload.get("completed_symbols", [])
            if str(symbol).strip()
        }
        return CheckpointState(path=path, completed_symbols=completed)

    def _persist_checkpoint(self, checkpoint: CheckpointState) -> None:
        payload = {
            "completed_symbols": sorted(checkpoint.completed_symbols),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        checkpoint.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _clear_checkpoint(self, path: Path) -> None:
        if path.exists():
            path.unlink()

    def _log_progress(
        self,
        dataset: IngestionDataset,
        summary: dict[str, Any],
        pending_total: int,
        started_at: float,
    ) -> None:
        processed = int(summary["processed_symbols"])
        if processed != 1 and processed % PROGRESS_LOG_EVERY != 0 and processed != pending_total:
            return

        elapsed = max(time.monotonic() - started_at, 0.0)
        rate = processed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Kite %s progress %d/%d succeeded=%d failed=%d zero_rows=%d elapsed=%.1fs rate=%.2f symbols/s",
            dataset.value,
            processed,
            pending_total,
            summary["succeeded"],
            summary["failed"],
            summary["zero_rows"],
            elapsed,
            rate,
        )

    def _refresh_features(self, summary: dict[str, Any], *, start_date: date) -> None:
        market_db = get_market_db()
        market_db._build_modular_features(force=False, since_date=start_date)
        monitor_rows = market_db.build_market_monitor_incremental(
            since_date=start_date, force=False
        )
        summary["features_refreshed"] = True
        summary["market_monitor_refreshed"] = True
        summary["market_monitor_rows"] = monitor_rows


_kite_scheduler: KiteScheduler | None = None


def get_kite_scheduler() -> KiteScheduler:
    global _kite_scheduler
    if _kite_scheduler is None:
        _kite_scheduler = KiteScheduler()
    return _kite_scheduler
