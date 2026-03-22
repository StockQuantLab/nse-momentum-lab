from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.kite.auth import KiteAuth, get_kite_auth
from nse_momentum_lab.services.kite.writer import KiteWriter, get_kite_writer
from nse_momentum_lab.utils.constants import IngestionDataset

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "raw" / "kite" / "checkpoints"
PARQUET_DAILY_DIR = PROJECT_ROOT / "data" / "parquet" / "daily"
CHECKPOINT_FLUSH_EVERY = 25
BACKFILL_START_DATE = date(2025, 4, 1)


@dataclass(slots=True)
class CheckpointState:
    path: Path
    completed_symbols: set[str]


class KiteScheduler:
    def __init__(self, auth: KiteAuth | None = None, writer: KiteWriter | None = None) -> None:
        self.auth = auth or get_kite_auth()
        self.writer = writer or get_kite_writer()

    def get_ingestion_status(self) -> dict[str, Any]:
        market_db = get_market_db(read_only=True)
        daily_range = None
        five_min_range = None
        if market_db.has_daily:
            row = market_db.con.execute(
                "SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM v_daily"
            ).fetchone()
            if row:
                daily_range = {
                    "min_date": row[0].isoformat() if row[0] else None,
                    "max_date": row[1].isoformat() if row[1] else None,
                    "symbols": int(row[2] or 0),
                }
        if market_db.has_5min:
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
        if not PARQUET_DAILY_DIR.exists():
            return []
        symbols: list[str] = []
        for child in sorted(PARQUET_DAILY_DIR.iterdir()):
            if not child.is_dir():
                continue
            if (child / "all.parquet").exists() or (child / "kite.parquet").exists():
                symbols.append(child.name.strip().upper())
        return symbols

    def get_symbols_from_kite(
        self,
        *,
        exchange: str = "NSE",
        segment: str = "NSE",
    ) -> list[str]:
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
    ) -> dict[str, Any]:
        target_date = trading_date or datetime.now(UTC).date()
        return self.run_daily_range_ingestion(
            symbols=symbols,
            start_date=target_date,
            end_date=target_date,
            update_features=update_features,
            save_raw=save_raw,
            resume=resume,
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
        )

    def run_5min_ingestion(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        save_raw: bool = False,
        resume: bool = True,
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
    ) -> dict[str, Any]:
        resolved_symbols = self._resolve_symbols(symbols)
        checkpoint = self._load_checkpoint(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            resume=resume,
        )
        pending_symbols = [
            symbol for symbol in resolved_symbols if symbol not in checkpoint.completed_symbols
        ]

        summary: dict[str, Any] = {
            "dataset": dataset.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_symbols": len(resolved_symbols),
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
            summary["checkpoint_cleared"] = True
            self._clear_checkpoint(checkpoint.path)
            return summary

        completed_since_flush = 0
        for symbol in pending_symbols:
            summary["processed_symbols"] += 1
            try:
                if self.auth.get_instrument_token(symbol) is None:
                    summary["failed"] += 1
                    summary["missing_tokens"].append(symbol)
                    continue

                if dataset is IngestionDataset.DAILY:
                    records = self.writer.fetch_and_write_daily(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        mode=mode,
                        save_raw=save_raw,
                    )
                else:
                    records = self.writer.fetch_and_write_5min(
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
                    completed_since_flush = 0
            except Exception as exc:
                logger.exception("Kite %s ingestion failed for %s", dataset.value, symbol)
                summary["failed"] += 1
                summary["errors"].append({"symbol": symbol, "error": str(exc)})

        if resume and checkpoint.completed_symbols:
            self._persist_checkpoint(checkpoint)

        if resume and len(checkpoint.completed_symbols) >= len(resolved_symbols):
            summary["checkpoint_cleared"] = True
            self._clear_checkpoint(checkpoint.path)

        if dataset is IngestionDataset.DAILY and update_features and summary["succeeded"] > 0:
            self._refresh_features(summary)

        return summary

    def _resolve_symbols(self, symbols: list[str] | None) -> list[str]:
        if symbols:
            cleaned = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
            return list(dict.fromkeys(cleaned))

        local_symbols = self.get_symbols_from_local_parquet()
        if local_symbols:
            return local_symbols
        return self.get_symbols_from_kite(exchange="NSE", segment="NSE")

    def _checkpoint_path(
        self,
        *,
        dataset: IngestionDataset,
        start_date: date,
        end_date: date,
    ) -> Path:
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        return (
            CHECKPOINT_DIR / f"{dataset.value}_{start_date.isoformat()}_{end_date.isoformat()}.json"
        )

    def _load_checkpoint(
        self,
        *,
        dataset: IngestionDataset,
        start_date: date,
        end_date: date,
        resume: bool,
    ) -> CheckpointState:
        path = self._checkpoint_path(dataset=dataset, start_date=start_date, end_date=end_date)
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

    def _refresh_features(self, summary: dict[str, Any]) -> None:
        market_db = get_market_db()
        row_count = market_db.build_feat_daily_table(force=True)
        summary["features_refreshed"] = True
        summary["feat_daily_rows"] = row_count


_kite_scheduler: KiteScheduler | None = None


def get_kite_scheduler() -> KiteScheduler:
    global _kite_scheduler
    if _kite_scheduler is None:
        _kite_scheduler = KiteScheduler()
    return _kite_scheduler
