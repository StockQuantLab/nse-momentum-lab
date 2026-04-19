"""Live feed audit for paper trading sessions.

Records every closed 5-min bar seen by the paper engine (one row per
session/symbol/bar) in the ``paper_feed_audit`` DuckDB table.

After a session completes, ``compare_feed_audit()`` diffs the recorded bars
against the EOD-built ``v_5min`` view in ``market.duckdb`` — surfacing missing
bars, price discrepancies, and volume drift.

Usage::

    from nse_momentum_lab.services.paper.scripts.paper_feed_audit import (
        record_closed_candles,
        compare_feed_audit,
    )

    # Called per bar group in paper_replay.py / paper_live.py
    record_closed_candles(
        bar_candles=closed_candle_objects,
        session_id=session_id,
        trade_date=trade_date,
        feed_source="replay",  # "replay" | "kite"
        paper_db=paper_db,
    )

    # Called EOD for drift report
    report = compare_feed_audit(
        trade_date="2026-04-18",
        session_id=session_id,
        paper_db=paper_db,
    )
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Tolerances for OHLCV comparison vs EOD data.
_PRICE_TOLERANCE = 0.01  # 1 paisa absolute difference is acceptable
_VOLUME_TOLERANCE = 0.5  # 50% relative volume difference triggers a flag

_IST_OFFSET = timedelta(hours=5, minutes=30)
_BAR_INTERVAL_SEC = 300  # 5 minutes


def _epoch_to_utc_dt(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=UTC)


def _audit_bar_end(*, feed_source: str, bar_start: float, bar_end: float) -> datetime:
    """Return the datetime used to look up the matching row in v_5min.

    v_5min stores ``candle_time`` as the *end* of the bar (i.e. the timestamp
    that closes the 5-minute window, equivalent to bar_end from the local adapter).

    - For the local/replay feed: ``bar_end`` from the ClosedCandle is already
      the ``candle_time`` stored in v_5min.
    - For the Kite live feed: the Kite API timestamps the bar by its *start*,
      so we use ``bar_start`` which equals ``candle_time - 5 min`` in v_5min
      terms; we add the interval to align it with v_5min's candle_time.
    """
    if feed_source.lower() == "kite":
        return _epoch_to_utc_dt(bar_start + _BAR_INTERVAL_SEC)
    return _epoch_to_utc_dt(bar_end)


def record_closed_candles(
    *,
    bar_candles: list[Any],
    session_id: str,
    trade_date: str,
    feed_source: str,
    paper_db: Any,
    transport: str = "",
) -> int:
    """Persist a batch of closed candles to ``paper_feed_audit``.

    Args:
        bar_candles: List of ``ClosedCandle`` objects for this bar group.
        session_id: Active paper session ID.
        trade_date: ISO date string ``YYYY-MM-DD``.
        feed_source: ``"replay"`` or ``"kite"``.
        paper_db: ``PaperDB`` instance.
        transport: Optional transport label (e.g. ``"websocket"``, ``"local"``).

    Returns:
        Number of rows upserted.
    """
    if not bar_candles or paper_db is None:
        return 0

    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for c in bar_candles:
        try:
            bar_start_f = getattr(c, "bar_start", 0.0) or 0.0
            bar_end_f = getattr(c, "bar_end", 0.0) or 0.0
            first_snap = getattr(c, "first_snapshot_ts", 0.0) or 0.0
            last_snap = getattr(c, "last_snapshot_ts", 0.0) or 0.0

            rows.append(
                {
                    "session_id": session_id,
                    "trade_date": trade_date,
                    "feed_source": feed_source,
                    "transport": transport,
                    "symbol": c.symbol,
                    "bar_start": _epoch_to_utc_dt(bar_start_f) if bar_start_f else None,
                    "bar_end": _epoch_to_utc_dt(bar_end_f) if bar_end_f else now,
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "volume": float(getattr(c, "volume", 0.0) or 0.0),
                    "first_snapshot_ts": _epoch_to_utc_dt(first_snap) if first_snap else None,
                    "last_snapshot_ts": _epoch_to_utc_dt(last_snap) if last_snap else None,
                    "created_at": now,
                }
            )
        except Exception:
            logger.exception(
                "feed_audit: failed to build row for symbol=%s session=%s",
                getattr(c, "symbol", "?"),
                session_id,
            )

    if not rows:
        return 0

    try:
        return paper_db.upsert_feed_audit_rows(rows)
    except Exception:
        logger.exception("feed_audit: upsert failed session=%s", session_id)
        return 0


def compare_feed_audit(
    *,
    trade_date: str,
    paper_db: Any,
    session_id: str | None = None,
    feed_source: str | None = None,
    market_db_path: str = "data/market.duckdb",
) -> dict[str, Any]:
    """Compare recorded feed audit rows against EOD ``v_5min`` data.

    Loads the paper feed audit rows for ``trade_date`` from ``paper_db``, then
    opens ``market_db_path`` (read-only) to fetch the canonical ``v_5min``
    candles for the same symbols and date.  Returns a structured report with:

    - ``missing_bars``: bars present in the live feed but absent from v_5min
    - ``extra_bars``:   bars in v_5min that the live feed never saw
    - ``price_diffs``:  bars where OHLC deviated beyond ``_PRICE_TOLERANCE``
    - ``volume_diffs``: bars where volume deviated beyond ``_VOLUME_TOLERANCE``

    Args:
        trade_date: ISO date string ``YYYY-MM-DD``.
        paper_db: ``PaperDB`` instance.
        session_id: Optional — restrict audit rows to one session.
        feed_source: Optional — restrict audit rows to one feed source.
        market_db_path: Path to ``market.duckdb``.

    Returns:
        Dict with keys ``missing_bars``, ``extra_bars``, ``price_diffs``,
        ``volume_diffs``, ``audit_rows``, ``market_rows``, ``symbols``.
    """
    import duckdb

    audit_rows = paper_db.get_feed_audit_rows(
        trade_date=trade_date,
        session_id=session_id,
        feed_source=feed_source,
    )

    if not audit_rows:
        return {
            "missing_bars": [],
            "extra_bars": [],
            "price_diffs": [],
            "volume_diffs": [],
            "audit_rows": 0,
            "market_rows": 0,
            "symbols": [],
            "note": "No feed audit rows found for this date/session.",
        }

    symbols = sorted({r.symbol for r in audit_rows})

    # Load EOD v_5min candles for the symbols on this date.
    try:
        con = duckdb.connect(market_db_path, read_only=True)
        sym_list = ", ".join(f"'{s.replace(chr(39), '')}'" for s in symbols)
        market_df = con.execute(
            f"""
            SELECT symbol, candle_time, open, high, low, close, volume
            FROM v_5min
            WHERE date = '{trade_date}'
              AND symbol IN ({sym_list})
            ORDER BY symbol, candle_time
            """
        ).fetchall()
        con.close()
    except Exception:
        logger.exception("feed_audit: failed to load v_5min for compare date=%s", trade_date)
        return {
            "missing_bars": [],
            "extra_bars": [],
            "price_diffs": [],
            "volume_diffs": [],
            "audit_rows": len(audit_rows),
            "market_rows": 0,
            "symbols": symbols,
            "error": "Failed to load v_5min — is market.duckdb available?",
        }

    # Build lookup: (symbol, bar_end_hhmm) → market candle.
    market_lookup: dict[tuple[str, str], dict[str, float]] = {}
    for row in market_df:
        sym, candle_time, o, h, lo, cl, vol = row
        if hasattr(candle_time, "strftime"):
            hhmm = candle_time.strftime("%H:%M")
        else:
            hhmm = str(candle_time)[:5]
        market_lookup[(sym, hhmm)] = {"open": o, "high": h, "low": lo, "close": cl, "volume": vol}

    # Build lookup from audit rows: (symbol, bar_end_hhmm).
    audit_lookup: dict[tuple[str, str], Any] = {}
    for r in audit_rows:
        bar_end_dt: datetime | None = r.bar_end if isinstance(r.bar_end, datetime) else None
        bar_start_f = r.bar_start.timestamp() if r.bar_start else 0.0
        bar_end_f = r.bar_end.timestamp() if isinstance(r.bar_end, datetime) else 0.0

        # align to v_5min candle_time
        aligned_dt = _audit_bar_end(
            feed_source=r.feed_source or "replay",
            bar_start=bar_start_f,
            bar_end=bar_end_f,
        )
        # Convert aligned UTC datetime to IST for HH:MM key.
        ist_dt = aligned_dt.astimezone(timezone(_IST_OFFSET))
        hhmm = ist_dt.strftime("%H:%M")
        _ = bar_end_dt  # used for type narrowing only
        audit_lookup[(r.symbol, hhmm)] = r

    missing_bars: list[dict[str, Any]] = []
    extra_bars: list[dict[str, Any]] = []
    price_diffs: list[dict[str, Any]] = []
    volume_diffs: list[dict[str, Any]] = []

    all_keys = set(audit_lookup) | set(market_lookup)
    for key in sorted(all_keys):
        sym, hhmm = key
        in_audit = key in audit_lookup
        in_market = key in market_lookup

        if in_audit and not in_market:
            r = audit_lookup[key]
            missing_bars.append({"symbol": sym, "bar_hhmm": hhmm, "source": "audit_only"})
        elif in_market and not in_audit:
            extra_bars.append({"symbol": sym, "bar_hhmm": hhmm, "source": "market_only"})
        else:
            r = audit_lookup[key]
            m = market_lookup[key]
            for field in ("open", "high", "low", "close"):
                a_val = getattr(r, field)
                m_val = m[field]
                if abs(a_val - m_val) > _PRICE_TOLERANCE:
                    price_diffs.append(
                        {
                            "symbol": sym,
                            "bar_hhmm": hhmm,
                            "field": field,
                            "audit": a_val,
                            "market": m_val,
                            "diff": round(a_val - m_val, 4),
                        }
                    )
            a_vol = r.volume
            m_vol = m["volume"]
            if m_vol > 0 and abs(a_vol - m_vol) / m_vol > _VOLUME_TOLERANCE:
                volume_diffs.append(
                    {
                        "symbol": sym,
                        "bar_hhmm": hhmm,
                        "audit_volume": a_vol,
                        "market_volume": m_vol,
                        "rel_diff": round((a_vol - m_vol) / m_vol, 4),
                    }
                )

    return {
        "trade_date": trade_date,
        "session_id": session_id,
        "missing_bars": missing_bars,
        "extra_bars": extra_bars,
        "price_diffs": price_diffs,
        "volume_diffs": volume_diffs,
        "audit_rows": len(audit_rows),
        "market_rows": len(market_df),
        "symbols": symbols,
    }
