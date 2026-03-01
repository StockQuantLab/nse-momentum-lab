from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass(frozen=True)
class CandleRow:
    """A single candle row (minute or daily)."""

    ts: datetime | None
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


def _norm_col(name: str) -> str:
    return name.strip().lower().replace(" ", "").replace("_", "")


def _parse_int(value: str | float | int | None) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    v = str(value).strip()
    if not v:
        return 0
    return int(float(v.replace(",", "")))


def _parse_float(value: str | float | int | None) -> float:
    if value is None:
        raise ValueError("missing numeric value")
    if isinstance(value, (int, float)):
        return float(value)
    v = str(value).strip()
    if not v:
        raise ValueError("missing numeric value")
    return float(v.replace(",", ""))


def _parse_date(value: str) -> date:
    v = value.strip()
    try:
        return date.fromisoformat(v[:10])
    except ValueError:
        pass

    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v[:10], fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Unrecognized date format: {value!r}")


def _parse_datetime_maybe(value: str, *, assume_minute: bool) -> tuple[datetime | None, date]:
    v = value.strip()
    d = _parse_date(v)
    if not assume_minute:
        return None, d

    # Normalize timezone offsets like "+0530" -> "+05:30" (and same for -HHMM)
    # Common in broker exports.
    v_norm = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", v)

    try:
        dt = datetime.fromisoformat(v_norm.replace("Z", "+00:00"))
        return dt, dt.date()
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ):
        try:
            dt = datetime.strptime(v, fmt)
            return dt, dt.date()
        except ValueError:
            continue

    return None, d


def infer_symbol_from_filename(path: Path) -> str:
    """Best-effort symbol inference: take the stem and trim common suffixes."""

    stem = path.stem
    lowered = stem.lower()
    for suffix in ("_1min", "_5min", "_15min", "_minute", "_intraday", "_day", "_daily"):
        if lowered.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return stem.strip().upper()


def iter_candles_csv(
    path: Path,
    *,
    timeframe: str = "auto",
) -> Iterable[CandleRow]:
    """Yield CandleRow from a CSV file.

    Expected columns (case-insensitive): Date/Datetime, Open, High, Low, Close, Volume.

    timeframe:
      - "day": Date is treated as date-only.
      - "minute": Date is treated as timestamp when possible.
      - "auto": decides based on whether the first row's Date includes a time.
    """

    if timeframe not in {"auto", "day", "minute"}:
        raise ValueError("timeframe must be one of: auto, day, minute")

    with path.open("r", newline="", encoding="utf-8") as f:
        # Robust delimiter detection: these datasets are sometimes TSV.
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        if not reader.fieldnames:
            return

        cols = {_norm_col(c): c for c in reader.fieldnames}
        date_col = (
            cols.get("date") or cols.get("datetime") or cols.get("timestamp") or cols.get("time")
        )
        if not date_col:
            raise ValueError(f"Missing Date column in {path}")

        open_col = cols.get("open")
        high_col = cols.get("high")
        low_col = cols.get("low")
        close_col = cols.get("close")
        vol_col = cols.get("volume")

        missing = [
            name
            for name, col in (
                ("Open", open_col),
                ("High", high_col),
                ("Low", low_col),
                ("Close", close_col),
                ("Volume", vol_col),
            )
            if not col
        ]
        if missing:
            raise ValueError(f"Missing columns {missing} in {path}")

        try:
            first_row = next(reader)
        except StopIteration:
            return

        inferred_minute: bool
        if timeframe == "minute":
            inferred_minute = True
        elif timeframe == "day":
            inferred_minute = False
        else:
            v = (first_row.get(date_col) or "").strip()
            inferred_minute = (" " in v) or ("T" in v) or (":" in v)

        def row_to_candle(row: dict[str, str]) -> CandleRow | None:
            v = row.get(date_col)
            if not v:
                return None
            ts, d = _parse_datetime_maybe(v, assume_minute=inferred_minute)
            return CandleRow(
                ts=ts,
                trading_date=d,
                open=_parse_float(row.get(open_col)),
                high=_parse_float(row.get(high_col)),
                low=_parse_float(row.get(low_col)),
                close=_parse_float(row.get(close_col)),
                volume=_parse_int(row.get(vol_col)),
            )

        first = row_to_candle(first_row)
        if first is not None:
            yield first
        for row in reader:
            candle = row_to_candle(row)
            if candle is not None:
                yield candle


@dataclass
class DailyAgg:
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    first_ts: datetime | None
    last_ts: datetime | None


def aggregate_to_daily(rows: Iterable[CandleRow]) -> list[DailyAgg]:
    """Aggregate minute candles to daily candles.

    Assumes rows for a symbol are in non-decreasing timestamp order when timestamps exist.
    If timestamps are missing, treats rows as already-daily (one per date).
    """

    out: list[DailyAgg] = []
    current: DailyAgg | None = None
    seen_any_ts = False

    for r in rows:
        if r.ts is not None:
            seen_any_ts = True

        if current is None or r.trading_date != current.trading_date:
            if current is not None:
                out.append(current)
            current = DailyAgg(
                trading_date=r.trading_date,
                open=r.open,
                high=r.high,
                low=r.low,
                close=r.close,
                volume=r.volume,
                first_ts=r.ts,
                last_ts=r.ts,
            )
            continue

        if (
            seen_any_ts
            and current.last_ts is not None
            and r.ts is not None
            and r.ts < current.last_ts
        ):
            raise ValueError(
                f"Out-of-order timestamps within {current.trading_date}: {r.ts} < {current.last_ts}"
            )

        current.high = max(current.high, r.high)
        current.low = min(current.low, r.low)
        current.close = r.close
        current.volume += r.volume
        if r.ts is not None:
            current.last_ts = r.ts

    if current is not None:
        out.append(current)
    return out


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
