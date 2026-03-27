"""CLI utility to inspect low-noise backtest run progress."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

import psycopg

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.utils.time_utils import IST


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show backtest run status/progress")
    parser.add_argument("--exp-id", type=str, default=None, help="Experiment hash to inspect")
    parser.add_argument("--watch", action="store_true", help="Poll status continuously")
    parser.add_argument("--interval", type=float, default=10.0, help="Watch interval in seconds")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser


def fetch_backtest_status(exp_id: str | None) -> dict | None:
    settings = get_settings()
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required. Run via Doppler.")

    where_clause = "WHERE exp_hash = %(exp_id)s" if exp_id else ""
    sql = f"""
    SELECT exp_hash, status, progress_stage, progress_pct, progress_message,
           started_at, heartbeat_at, finished_at
    FROM nseml.exp_run
    {where_clause}
    ORDER BY started_at DESC NULLS LAST, exp_run_id DESC
    LIMIT 1
    """

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS nseml")
            cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_stage text")
            cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_message text")
            cur.execute("ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS progress_pct numeric")
            cur.execute(
                "ALTER TABLE nseml.exp_run ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz"
            )
            cur.execute(sql, {"exp_id": exp_id} if exp_id else {})
            row = cur.fetchone()
            if row is None:
                return None

    now = datetime.now(IST)
    heartbeat_at = row[6]
    heartbeat_age_seconds = None
    if heartbeat_at is not None:
        heartbeat_age_seconds = max(0.0, (now - heartbeat_at).total_seconds())

    return {
        "exp_id": row[0],
        "status": row[1],
        "stage": row[2],
        "progress_pct": float(row[3]) if row[3] is not None else None,
        "message": row[4],
        "started_at": row[5].isoformat() if row[5] else None,
        "heartbeat_at": row[6].isoformat() if row[6] else None,
        "finished_at": row[7].isoformat() if row[7] else None,
        "heartbeat_age_seconds": heartbeat_age_seconds,
    }


def print_status(status: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(status, sort_keys=True))
        return

    progress = status.get("progress_pct")
    progress_txt = "--.-%" if progress is None else f"{progress:5.1f}%"
    hb_age = status.get("heartbeat_age_seconds")
    hb_age_txt = "n/a" if hb_age is None else f"{hb_age:,.0f}s ago"
    print(
        f"{status['exp_id']} | {status['status']:<9} | {progress_txt} | "
        f"{status.get('stage') or '-'} | heartbeat {hb_age_txt} | "
        f"{status.get('message') or ''}"
    )


def main() -> None:
    args = build_parser().parse_args()

    while True:
        status = fetch_backtest_status(args.exp_id)
        if status is None:
            print("No matching backtest run found in Postgres.")
        else:
            print_status(status, as_json=args.json)

        if not args.watch:
            break
        time.sleep(max(args.interval, 1.0))


if __name__ == "__main__":
    main()
