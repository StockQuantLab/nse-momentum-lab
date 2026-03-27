from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime
from math import ceil
from typing import Any

from nse_momentum_lab.services.kite.auth import get_kite_auth
from nse_momentum_lab.services.kite.fetcher import HISTORICAL_REQUESTS_PER_SECOND
from nse_momentum_lab.services.kite.scheduler import BACKFILL_START_DATE, get_kite_scheduler
from nse_momentum_lab.utils.constants import IngestionDataset
from nse_momentum_lab.utils.time_utils import IST


def validate_symbols_csv(symbols_csv: str | None, max_symbols: int = 50) -> list[str] | None:
    if symbols_csv is None:
        return None
    symbols = [item.strip().upper() for item in symbols_csv.split(",") if item.strip()]
    if not symbols:
        return None
    if len(symbols) > max_symbols:
        raise ValueError(f"--symbols supports at most {max_symbols} symbols per run")
    return list(dict.fromkeys(symbols))


def _parse_date(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    return date.fromisoformat(value.strip())


def _estimate_backfill_cost(
    symbols: list[str] | None, start_date: date, end_date: date, is_5min: bool
) -> dict[str, Any]:
    """Estimate API calls and time for backfill."""
    scheduler = get_kite_scheduler()
    resolved = scheduler._resolve_symbols(
        symbols=symbols,
        dataset=IngestionDataset.FIVE_MIN if is_5min else IngestionDataset.DAILY,
        start_date=start_date,
        end_date=end_date,
    )
    symbol_count = len(resolved)
    days = (end_date - start_date).days + 1

    if is_5min:
        chunks = ceil(days / 60)
        requests = symbol_count * chunks
    else:
        requests = symbol_count
    estimated_seconds = requests / HISTORICAL_REQUESTS_PER_SECOND

    return {
        "symbols": symbol_count,
        "days": days,
        "requests": requests,
        "estimated_seconds": round(estimated_seconds, 1),
        "estimated_minutes": round(estimated_seconds / 60, 1),
        "is_5min": is_5min,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="Ingest Kite OHLCV into local parquet")
    parser.add_argument("--date", dest="single_date", help="Specific trading date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="start_date", help="Range start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="end_date", help="Range end date (YYYY-MM-DD)")
    parser.add_argument("--today", action="store_true", help="Use today's date for daily ingestion")
    parser.add_argument(
        "--5min", dest="use_5min", action="store_true", help="Run 5-minute ingestion"
    )
    parser.add_argument("--symbols", help="Comma-separated symbol list")
    parser.add_argument("--backfill", action="store_true", help="Use the default backfill window")
    parser.add_argument("--save-raw", action="store_true", help="Persist raw CSV snapshots")
    parser.add_argument(
        "--update-features",
        action="store_true",
        help="Rebuild feat_daily after daily ingestion completes",
    )
    parser.add_argument(
        "--refresh-instruments",
        action="store_true",
        help="Refresh the local instrument cache and exit",
    )
    parser.add_argument("--exchange", default="NSE", help="Exchange code for instrument refresh")
    parser.add_argument("--resume", dest="resume", action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    args = parser.parse_args()

    scheduler = get_kite_scheduler()
    if args.refresh_instruments:
        count = get_kite_auth().refresh_instruments(args.exchange)
        print(json.dumps({"exchange": args.exchange.upper(), "refreshed": count}, indent=2))
        return 0

    symbols = validate_symbols_csv(args.symbols)
    today = datetime.now(IST).date()
    if args.backfill:
        start_date = BACKFILL_START_DATE
        end_date = today
    elif args.today:
        start_date = today
        end_date = today
    elif args.single_date:
        start_date = _parse_date(args.single_date)
        end_date = start_date
    else:
        start_date = _parse_date(args.start_date)
        end_date = _parse_date(args.end_date) or start_date

    if start_date is None or end_date is None:
        parser.error("Provide --today, --date, --from/--to, or --backfill")
    if start_date > end_date:
        parser.error("--from must be on or before --to")

    cost_estimate = _estimate_backfill_cost(symbols, start_date, end_date, args.use_5min)
    logging.info(
        "Backfill cost estimate: %d symbols, %d days, ~%d requests, ~%.1f min",
        cost_estimate["symbols"],
        cost_estimate["days"],
        cost_estimate["requests"],
        cost_estimate["estimated_minutes"],
    )
    if cost_estimate["estimated_minutes"] > 5:
        logging.warning("Estimated time >5 minutes. Use --5min for daily mode.")

    if args.use_5min:
        result = scheduler.run_5min_ingestion(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            save_raw=args.save_raw,
            resume=args.resume,
        )
    else:
        result = scheduler.run_daily_range_ingestion(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            update_features=args.update_features,
            save_raw=args.save_raw,
            resume=args.resume,
        )

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
