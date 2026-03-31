from __future__ import annotations

import logging
import traceback
import warnings
from dataclasses import dataclass
from datetime import date, time, timedelta

import numpy as np
import pandas as pd
import vectorbt as vbt

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.engine import (
    DefaultBreakoutExitPolicy,
    ExitPolicy,
    ExitReason,
    PositionSide,
)
from nse_momentum_lab.services.backtest.signal_models import BacktestSignal

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    symbol_id: int
    symbol: str
    entry_date: date
    entry_price: float
    entry_mode: str
    qty: int
    initial_stop: float
    exit_date: date | None = None
    exit_price: float | None = None
    pnl: float | None = None
    pnl_r: float | None = None
    fees: float = 0.0
    slippage_bps: float = 0.0
    mfe_r: float | None = None
    mae_r: float | None = None
    exit_reason: ExitReason | None = None
    exit_rule_version: str = "v1"
    entry_time: time | None = None
    exit_time: time | None = None


@dataclass
class VectorBTConfig:
    """Configuration for VectorBT backtesting engine.

    ENTRY TIMING: Gap-up Breakout Strategy
    ---------------------------------------
    - Signal day selected from daily setup filters
    - Breakout trigger: price >= (1 + threshold) * T-1's close
    - Entry: first trigger touch (typically from 5-minute data)
    - 2LYNCH criteria: Applied on T-1 and prior daily data

    This removes daily-bar entry look-ahead by using intraday execution
    timing while keeping setup qualification daily.

    POSITION SIZING: ATR-based Risk Management
    ------------------------------------------
    - Risk per trade = portfolio_value * risk_per_trade_pct
    - Position size = risk_amount / (entry - stop)
    - Example: Rs 10L portfolio, 1% risk = Rs 10k per trade
    """

    direction: PositionSide = PositionSide.LONG
    initial_stop_atr_mult: float = 2.0
    trail_activation_pct: float = 0.08  # Stockbee: "trailing stop once up 8% plus"
    trail_stop_pct: float = 0.02
    min_hold_days: int = 3  # Stockbee: hold at least 3 days unless stop is hit
    time_stop_days: int = 5  # Stockbee: exit by day 5
    abnormal_profit_pct: float = 0.10  # Abnormal 1-day move; book at least partials
    abnormal_gap_exit_pct: float = 0.20  # Gap-up lock-profit trigger
    follow_through_threshold: float = 0.0  # Disabled: exits handled by stop/time/profit rules
    fees_per_trade: float = 0.001
    slippage_large_bps: float = 5.0
    slippage_mid_bps: float = 10.0
    slippage_small_bps: float = 20.0
    large_bucket_threshold_inr: float = 100_000_000.0
    small_bucket_threshold_inr: float = 20_000_000.0
    breakout_threshold: float = 0.04

    # Position sizing
    risk_per_trade_pct: float = 0.01  # 1% risk per trade
    default_portfolio_value: float = 1_000_000.0  # Rs 10L
    max_position_pct: float = 0.10  # Max 10% in single position

    # Portfolio risk limits
    max_positions: int = 10
    max_drawdown_pct: float = 0.15

    # Intraday / same-day execution params (passed through from BacktestParams)
    abnormal_gap_mode: str = "immediate_exit"
    same_day_r_ladder: bool = True
    same_day_r_ladder_start_r: int = 2
    short_post_day3_buffer_pct: float = 0.0
    # Compatibility toggle: honor runner-provided same-day exit metadata.
    # Kept off by default to preserve current behavior unless explicitly requested.
    respect_same_day_exit_metadata: bool = False


@dataclass
class VectorBTResult:
    strategy_name: str
    entry_mode: str
    trades: list[Trade]
    total_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_r: float = 0.0
    calmar_ratio: float = 0.0
    sortino_ratio: float = 0.0
    median_r: float = 0.0
    r_distribution: dict[str, float] | None = None


class VectorBTEngine:
    def __init__(
        self,
        config: VectorBTConfig | None = None,
        exit_policy: ExitPolicy | None = None,
    ) -> None:
        self.config = config or VectorBTConfig()
        self._exit_policy: ExitPolicy = exit_policy or DefaultBreakoutExitPolicy()

    def load_market_data_from_duckdb(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[date, dict[str, float]]]:
        """
        Load market data from DuckDB (10-100x faster than PostgreSQL).

        Uses query_daily_multi() to avoid N+1 query pattern (batch fetch all symbols at once).

        Returns nested dict for VectorBT:
        {
            "RELIANCE": {
                date(2020, 1, 1): {"open": 100.0, "high": 105.0, ...},
                date(2020, 1, 2): {"open": 105.0, "high": 107.0, ...},
            },
            ...
        }
        """
        db = get_market_db()
        result = {symbol: {} for symbol in symbols}

        # Batch query all symbols at once instead of N+1 individual queries
        df = db.query_daily_multi(symbols, start_date.isoformat(), end_date.isoformat())

        for symbol_df in df.partition_by("symbol", maintain_order=True):
            symbol = str(symbol_df.get_column("symbol")[0])
            dates = symbol_df.get_column("date").to_list()
            opens = symbol_df.get_column("open").to_list()
            highs = symbol_df.get_column("high").to_list()
            lows = symbol_df.get_column("low").to_list()
            closes = symbol_df.get_column("close").to_list()
            volumes = symbol_df.get_column("volume").to_list()
            result[symbol] = {
                trading_date: {
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": int(volume),
                }
                for trading_date, open_price, high_price, low_price, close_price, volume in zip(
                    dates, opens, highs, lows, closes, volumes, strict=False
                )
            }

        return result

    def load_features_from_duckdb(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[date, dict]]:
        """Load pre-computed features from DuckDB."""
        db = get_market_db()

        df = db.get_features_range(symbols, start_date.isoformat(), end_date.isoformat())

        result = {symbol: {} for symbol in symbols}
        for symbol_df in df.partition_by("symbol", maintain_order=True):
            symbol = str(symbol_df.get_column("symbol")[0])
            trading_dates = symbol_df.get_column("trading_date").to_list()
            ret_1d = symbol_df.get_column("ret_1d").to_list()
            ret_5d = symbol_df.get_column("ret_5d").to_list()
            atr_20 = symbol_df.get_column("atr_20").to_list()
            range_pct = symbol_df.get_column("range_pct").to_list()
            close_pos_in_range = symbol_df.get_column("close_pos_in_range").to_list()
            ma_20 = symbol_df.get_column("ma_20").to_list()
            ma_65 = symbol_df.get_column("ma_65").to_list()
            rs_252 = symbol_df.get_column("rs_252").to_list()
            vol_20 = symbol_df.get_column("vol_20").to_list()
            dollar_vol_20 = symbol_df.get_column("dollar_vol_20").to_list()
            result[symbol] = {
                trading_date: {
                    "ret_1d": float(ret1) if ret1 is not None else None,
                    "ret_5d": float(ret5) if ret5 is not None else None,
                    "atr_20": float(atr) if atr is not None else None,
                    "range_pct": float(rng) if rng is not None else None,
                    "close_pos_in_range": float(close_pos) if close_pos is not None else None,
                    "ma_20": float(ma20) if ma20 is not None else None,
                    "ma_65": float(ma65) if ma65 is not None else None,
                    "rs_252": float(rs) if rs is not None else None,
                    "vol_20": float(vol20) if vol20 is not None else None,
                    "dollar_vol_20": float(dollar_vol) if dollar_vol is not None else None,
                }
                for (
                    trading_date,
                    ret1,
                    ret5,
                    atr,
                    rng,
                    close_pos,
                    ma20,
                    ma65,
                    rs,
                    vol20,
                    dollar_vol,
                ) in zip(
                    trading_dates,
                    ret_1d,
                    ret_5d,
                    atr_20,
                    range_pct,
                    close_pos_in_range,
                    ma_20,
                    ma_65,
                    rs_252,
                    vol_20,
                    dollar_vol_20,
                    strict=False,
                )
            }

        return result

    def prepare_price_matrix(
        self,
        price_data: dict[int, dict[date, dict[str, float]]],
        symbols: list[int],
        start_date: date,
        end_date: date,
        field: str,
    ) -> pd.DataFrame:
        return self._prepare_price_matrices(
            price_data=price_data,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            fields=[field],
        )[field]

    def _prepare_price_matrices(
        self,
        *,
        price_data: dict[int, dict[date, dict[str, float]]],
        symbols: list[int],
        start_date: date,
        end_date: date,
        fields: list[str],
    ) -> dict[str, pd.DataFrame]:
        valid_dates = sorted(
            {
                trading_date
                for symbol_id in symbols
                if symbol_id in price_data
                for trading_date in price_data[symbol_id]
                if start_date <= trading_date <= end_date
            }
        )
        if not valid_dates:
            return {field: pd.DataFrame() for field in fields}

        symbol_ids = [symbol_id for symbol_id in symbols if symbol_id in price_data]
        date_to_idx = {trading_date: idx for idx, trading_date in enumerate(valid_dates)}
        matrices = {
            field: np.full((len(valid_dates), len(symbol_ids)), np.nan, dtype=float)
            for field in fields
        }

        for col_idx, symbol_id in enumerate(symbol_ids):
            for trading_date, values in price_data[symbol_id].items():
                row_idx = date_to_idx.get(trading_date)
                if row_idx is None:
                    continue
                for field in fields:
                    raw_value = values.get(field, values.get(field.replace("_adj", ""), np.nan))
                    matrices[field][row_idx, col_idx] = raw_value

        index = pd.DatetimeIndex(valid_dates, name="date")
        return {
            field: pd.DataFrame(matrix, index=index, columns=symbol_ids)
            for field, matrix in matrices.items()
        }

    def prepare_signals(
        self,
        signals: list[tuple[date, int, str, float, dict]],
        price_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """Prepare entry signals DataFrame for VectorBT.

        Optimized with date-to-index dict lookup instead of linear search.
        """
        if price_data.empty:
            return pd.DataFrame()

        symbol_ids = price_data.columns.tolist()
        symbol_map = {sid: i for i, sid in enumerate(symbol_ids)}

        # Build date-to-index lookup once for O(1) access (avoid O(n) linear search per signal)
        date_to_idx = {idx.date(): i for i, idx in enumerate(price_data.index)}

        entries = pd.DataFrame(
            np.zeros_like(price_data.values, dtype=bool),
            index=price_data.index,
            columns=price_data.columns,
        )

        for signal_date, symbol_id, _symbol, _initial_stop, _metadata in signals:
            if symbol_id not in symbol_map:
                continue

            col_idx = symbol_map[symbol_id]
            dt_idx = date_to_idx.get(signal_date)

            if dt_idx is not None:
                entries.iloc[dt_idx, col_idx] = True
        return entries

    def _get_slippage_bps(self, value_traded_inr: float | None) -> float:
        """Get slippage bps based on value traded in INR (Indian markets)."""
        if value_traded_inr is None:
            return self.config.slippage_mid_bps
        if value_traded_inr >= self.config.large_bucket_threshold_inr:
            return self.config.slippage_large_bps
        if value_traded_inr >= self.config.small_bucket_threshold_inr:
            return self.config.slippage_mid_bps
        return self.config.slippage_small_bps

    def _build_slippage_matrix(
        self,
        price_data: pd.DataFrame,
        value_traded_inr: dict[int, float],
    ) -> pd.DataFrame:
        slippage_bps = {}
        for symbol_id in price_data.columns:
            value_traded_inr_20 = value_traded_inr.get(int(symbol_id)) if value_traded_inr else None
            slippage_bps[int(symbol_id)] = self._get_slippage_bps(value_traded_inr_20)

        slippage_values = {
            symbol_id: np.full(len(price_data.index), slippage_bps[int(symbol_id)] / 10000)
            for symbol_id in price_data.columns
        }
        slippage_df = pd.DataFrame(slippage_values, index=price_data.index)
        return slippage_df

    def _build_exit_signals(
        self,
        entries: pd.DataFrame,
        close_df: pd.DataFrame,
        open_df: pd.DataFrame,
        high_df: pd.DataFrame,
        low_df: pd.DataFrame,
        signals: list[tuple[date, int, str, float, dict]],
        delisting_dates: dict[int, date] | None = None,
    ) -> tuple[
        pd.DataFrame,
        pd.DataFrame,
        dict[tuple[int, date], ExitReason],
        dict[tuple[int, date], float],
    ]:
        """Build exit signals for breakout strategy.

        Optimized with pre-extracted NumPy arrays for 5-10x speedup.
        Supports both LONG and SHORT positions.

        ENTRY: Signal metadata entry price (5-minute first-touch) when provided,
        else fallback to daily open.
        INITIAL STOP: Stop level provided in signal payload.
        DELISTING: Force exit if stock is delisted during holding period.
        """
        is_short = self.config.direction == PositionSide.SHORT

        open_arr = open_df.values
        high_arr = high_df.values
        low_arr = low_df.values
        close_arr = close_df.values

        col_index = {col: i for i, col in enumerate(entries.columns)}

        exits = pd.DataFrame(False, index=entries.index, columns=entries.columns)
        order_price = close_df.copy()

        signal_map = {(sid, sdate): init_stop for sdate, sid, _sym, init_stop, _meta in signals}
        signal_meta_map = {(sid, sdate): meta for sdate, sid, _sym, _init_stop, meta in signals}
        signal_direction_map = {
            (sid, sdate): meta.get("direction", "LONG")
            for sdate, sid, _sym, _init_stop, meta in signals
        }

        def _to_exit_reason(raw_value: object, fallback: ExitReason) -> ExitReason:
            if raw_value is None:
                return fallback
            if isinstance(raw_value, ExitReason):
                return raw_value
            try:
                return ExitReason(str(raw_value))
            except ValueError:
                return fallback

        exit_reason_map: dict[tuple[int, date], ExitReason] = {}
        initial_stop_map: dict[tuple[int, date], float] = {}

        num_rows = len(entries.index)

        for symbol_id in entries.columns:
            col_idx = col_index[symbol_id]
            entry_dates = entries.index[entries[symbol_id]].tolist()
            delisting_date = delisting_dates.get(int(symbol_id)) if delisting_dates else None

            for entry_dt in entry_dates:
                entry_idx = entries.index.get_loc(entry_dt)
                signal_date = entry_dt.date()
                initial_stop = signal_map.get((int(symbol_id), signal_date))
                signal_meta = signal_meta_map.get((int(symbol_id), signal_date), {})

                signal_dir = signal_direction_map.get((int(symbol_id), signal_date), "LONG")
                position_is_short = signal_dir == "SHORT" or is_short

                entry_date = entry_dt.date()
                entry_price_override = signal_meta.get("entry_price") if signal_meta else None
                if entry_price_override is not None:
                    entry_price = float(entry_price_override)
                else:
                    entry_price = open_arr[entry_idx, col_idx]

                if pd.isna(entry_price):
                    continue

                if initial_stop is None:
                    direction = PositionSide.SHORT if position_is_short else PositionSide.LONG
                    initial_stop = self._exit_policy.compute_initial_stop(
                        float(entry_price), None, direction
                    )

                order_price.iloc[entry_idx, col_idx] = entry_price
                initial_stop_map[(int(symbol_id), entry_date)] = float(initial_stop)

                if self.config.respect_same_day_exit_metadata:
                    same_day_exit_reason_raw = signal_meta.get("same_day_exit_reason")
                    same_day_exit_price = signal_meta.get("same_day_exit_price")
                    if same_day_exit_reason_raw is not None:
                        exits.iloc[entry_idx, col_idx] = True
                        order_price.iloc[entry_idx, col_idx] = float(
                            same_day_exit_price if same_day_exit_price is not None else initial_stop
                        )
                        exit_reason_map[(int(symbol_id), entry_date)] = _to_exit_reason(
                            same_day_exit_reason_raw,
                            ExitReason.STOP_INITIAL,
                        )
                        continue

                if bool(signal_meta.get("same_day_stop_hit", False)):
                    exits.iloc[entry_idx, col_idx] = True
                    same_day_exit_price = signal_meta.get("same_day_exit_price")
                    order_price.iloc[entry_idx, col_idx] = float(
                        same_day_exit_price if same_day_exit_price is not None else initial_stop
                    )
                    if self.config.respect_same_day_exit_metadata:
                        exit_reason_map[(int(symbol_id), entry_date)] = _to_exit_reason(
                            signal_meta.get("same_day_exit_reason"),
                            ExitReason.STOP_INITIAL,
                        )
                    else:
                        exit_reason_map[(int(symbol_id), entry_date)] = ExitReason.STOP_INITIAL
                    continue

                if position_is_short:
                    if float(entry_price) > float(initial_stop):
                        exits.iloc[entry_idx, col_idx] = True
                        order_price.iloc[entry_idx, col_idx] = entry_price
                        exit_reason_map[(int(symbol_id), entry_date)] = ExitReason.GAP_STOP
                        continue
                else:
                    if float(entry_price) < float(initial_stop):
                        exits.iloc[entry_idx, col_idx] = True
                        order_price.iloc[entry_idx, col_idx] = entry_price
                        exit_reason_map[(int(symbol_id), entry_date)] = ExitReason.GAP_STOP
                        continue

                if position_is_short:
                    min_low = float(entry_price)
                else:
                    max_high = float(entry_price)
                stop_level = float(initial_stop)
                if self.config.respect_same_day_exit_metadata:
                    carry_stop = signal_meta.get("carry_stop_next_session")
                    if carry_stop is not None:
                        carry_stop = float(carry_stop)
                        stop_level = (
                            min(stop_level, carry_stop)
                            if position_is_short
                            else max(stop_level, carry_stop)
                        )
                at_breakeven = False
                trail_active = False
                post_day3_stop_active = False
                exit_date = None
                exit_price = None
                exit_reason = None

                for day_offset in range(1, self.config.time_stop_days + 1):
                    current_idx = entry_idx + day_offset
                    if current_idx >= num_rows:
                        break

                    current_dt = entries.index[current_idx]
                    current_date = current_dt.date()

                    current_open = open_arr[current_idx, col_idx]
                    high = high_arr[current_idx, col_idx]
                    low = low_arr[current_idx, col_idx]
                    close = close_arr[current_idx, col_idx]

                    if delisting_date is not None and current_date >= delisting_date:
                        exit_date = current_dt
                        exit_price = float(close) if not pd.isna(close) else float(entry_price)
                        exit_reason = ExitReason.DELISTING
                        break

                    if pd.isna(high) or pd.isna(low) or pd.isna(close):
                        if delisting_date is not None and current_date >= delisting_date:
                            exit_date = current_dt
                            exit_price = float(entry_price)
                            exit_reason = ExitReason.DELISTING
                            break
                        continue

                    if position_is_short:
                        if not pd.isna(current_open) and float(current_open) <= float(
                            entry_price
                        ) * (1 - self.config.abnormal_gap_exit_pct):
                            exit_date = current_dt
                            exit_price = float(current_open)
                            exit_reason = ExitReason.ABNORMAL_GAP_EXIT
                            break

                        if not pd.isna(current_open) and float(current_open) > stop_level:
                            exit_date = current_dt
                            exit_price = float(current_open)
                            exit_reason = ExitReason.GAP_STOP
                            break

                        min_low = min(min_low, float(low))

                        if float(high) >= stop_level:
                            exit_date = current_dt
                            exit_price = stop_level
                            if trail_active and stop_level < float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            elif post_day3_stop_active and day_offset >= self.config.min_hold_days:
                                exit_reason = ExitReason.STOP_POST_DAY3
                            elif not at_breakeven:
                                exit_reason = ExitReason.STOP_INITIAL
                            elif stop_level == float(entry_price):
                                exit_reason = ExitReason.STOP_BREAKEVEN
                            else:
                                exit_reason = ExitReason.STOP_TRAIL
                            break

                        if not at_breakeven and float(close) < float(entry_price):
                            stop_level = min(stop_level, float(entry_price))
                            at_breakeven = True

                        if min_low <= float(entry_price) * (1 - self.config.trail_activation_pct):
                            trailing_stop = min(
                                float(entry_price),
                                min_low * (1 + self.config.trail_stop_pct),
                            )
                            stop_level = min(stop_level, trailing_stop)
                            trail_active = True

                        if day_offset <= 2 and float(low) <= float(entry_price) * (
                            1 - self.config.abnormal_profit_pct
                        ):
                            exit_date = current_dt
                            exit_price = float(close)
                            exit_reason = ExitReason.ABNORMAL_PROFIT
                            break

                        # After day 3, tighten stop to daily high progression (shorts: stop above entry)
                        if day_offset >= self.config.min_hold_days:
                            tightened = min(stop_level, float(high))
                            post_day3_stop_active = post_day3_stop_active or (
                                tightened < stop_level
                            )
                            stop_level = tightened

                        if float(close) >= stop_level:
                            exit_date = current_dt
                            exit_price = stop_level
                            if trail_active and stop_level < float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            elif post_day3_stop_active and day_offset >= self.config.min_hold_days:
                                exit_reason = ExitReason.STOP_POST_DAY3
                            elif not at_breakeven:
                                exit_reason = ExitReason.STOP_INITIAL
                            elif stop_level < float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            else:
                                exit_reason = ExitReason.STOP_BREAKEVEN
                            break
                    else:
                        if not pd.isna(current_open) and float(current_open) >= float(
                            entry_price
                        ) * (1 + self.config.abnormal_gap_exit_pct):
                            exit_date = current_dt
                            exit_price = float(current_open)
                            exit_reason = ExitReason.ABNORMAL_GAP_EXIT
                            break

                        if not pd.isna(current_open) and float(current_open) < stop_level:
                            exit_date = current_dt
                            exit_price = float(current_open)
                            exit_reason = ExitReason.GAP_STOP
                            break

                        max_high = max(max_high, float(high))

                        if float(low) <= stop_level:
                            exit_date = current_dt
                            exit_price = stop_level
                            if trail_active and stop_level > float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            elif post_day3_stop_active and day_offset >= self.config.min_hold_days:
                                exit_reason = ExitReason.STOP_POST_DAY3
                            elif not at_breakeven:
                                exit_reason = ExitReason.STOP_INITIAL
                            elif stop_level == float(entry_price):
                                exit_reason = ExitReason.STOP_BREAKEVEN
                            else:
                                exit_reason = ExitReason.STOP_TRAIL
                            break

                        if not at_breakeven and float(close) > float(entry_price):
                            stop_level = max(stop_level, float(entry_price))
                            at_breakeven = True

                        if max_high >= float(entry_price) * (1 + self.config.trail_activation_pct):
                            trailing_stop = max(
                                float(entry_price),
                                max_high * (1 - self.config.trail_stop_pct),
                            )
                            stop_level = max(stop_level, trailing_stop)
                            trail_active = True

                        if day_offset <= 2 and float(high) >= float(entry_price) * (
                            1 + self.config.abnormal_profit_pct
                        ):
                            exit_date = current_dt
                            exit_price = float(close)
                            exit_reason = ExitReason.ABNORMAL_PROFIT
                            break

                        # After day 3, tighten stop to daily low progression
                        if day_offset >= self.config.min_hold_days:
                            tightened = max(stop_level, float(low))
                            post_day3_stop_active = post_day3_stop_active or (
                                tightened > stop_level
                            )
                            stop_level = tightened

                        if float(close) <= stop_level:
                            exit_date = current_dt
                            exit_price = stop_level
                            if trail_active and stop_level > float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            elif post_day3_stop_active and day_offset >= self.config.min_hold_days:
                                exit_reason = ExitReason.STOP_POST_DAY3
                            elif not at_breakeven:
                                exit_reason = ExitReason.STOP_INITIAL
                            elif stop_level > float(entry_price):
                                exit_reason = ExitReason.STOP_TRAIL
                            else:
                                exit_reason = ExitReason.STOP_BREAKEVEN
                            break

                if exit_date is None:
                    last_idx = min(entry_idx + self.config.time_stop_days, num_rows - 1)
                    exit_date = entries.index[last_idx]
                    exit_price = float(close_arr[last_idx, col_idx])
                    exit_reason = ExitReason.TIME_EXIT

                exit_idx = entries.index.get_loc(exit_date)
                exits.iloc[exit_idx, col_idx] = True
                order_price.iloc[exit_idx, col_idx] = exit_price
                exit_reason_map[(int(symbol_id), entry_date)] = exit_reason

        return exits, order_price, exit_reason_map, initial_stop_map

    def _normalize_signal(
        self, signal: BacktestSignal | tuple
    ) -> tuple[date, int, str, float, dict]:
        """Normalize signal to tuple format for internal processing.

        Accepts both BacktestSignal objects and legacy tuples for backward compatibility.
        """
        if isinstance(signal, BacktestSignal):
            return signal.to_tuple()
        return signal

    def run_backtest(
        self,
        strategy_name: str,
        signals: list[BacktestSignal | tuple[date, int, str, float, dict]],
        price_data: dict[int, dict[date, dict[str, float]]],
        value_traded_inr: dict[int, float],
        delisting_dates: dict[int, date] | None = None,
    ) -> VectorBTResult:
        """Run backtest for gap-up breakout strategy.

        SIGNAL FORMAT:
            Accepts both BacktestSignal objects and legacy tuples:
            - BacktestSignal(signal_date, symbol_id, symbol, initial_stop, metadata)
            - (signal_date, symbol_id, symbol, initial_stop, metadata)

        ENTRY TIMING:
            - Signal date T from daily setup
            - Entry price comes from signal metadata (intraday first-touch)
            - Fallback to daily open if metadata not provided

        SURVIVORSHIP BIAS:
            - delisting_dates: Dict of symbol_id -> delisting_date
            - Force exit on delisting date if position is open

        Args:
            strategy_name: Name of the strategy
            signals: List of BacktestSignal or (date, symbol_id, symbol, initial_stop, metadata)
                     metadata may include entry_price and same_day_stop_hit
            price_data: Dict of symbol_id -> {date -> {open_adj, close_adj, high_adj, low_adj}}
            value_traded_inr: Dict of symbol_id -> 20-day avg value traded
            delisting_dates: Dict of symbol_id -> delisting_date (optional)

        Returns:
            VectorBTResult with trades and metrics
        """
        if not signals:
            return VectorBTResult(strategy_name=strategy_name, entry_mode="gap_open", trades=[])

        # Normalize signals to tuple format for processing
        normalized_signals = [self._normalize_signal(s) for s in signals]

        symbol_ids = list(price_data.keys())
        if not symbol_ids:
            return VectorBTResult(strategy_name=strategy_name, entry_mode="gap_open", trades=[])

        min_date = min(s[0] for s in normalized_signals)
        max_date = max(s[0] for s in normalized_signals)
        max_date = max_date + timedelta(
            days=self.config.time_stop_days + 5
        )  # Buffer for weekends/holidays

        price_matrices = self._prepare_price_matrices(
            symbols=symbol_ids,
            price_data=price_data,
            start_date=min_date,
            end_date=max_date,
            fields=["close_adj", "open_adj", "high_adj", "low_adj"],
        )
        close_df = price_matrices["close_adj"]
        open_df = price_matrices["open_adj"]
        high_df = price_matrices["high_adj"]
        low_df = price_matrices["low_adj"]

        if close_df.empty:
            return VectorBTResult(strategy_name=strategy_name, entry_mode="gap_open", trades=[])

        # Signal date equals entry date; actual entry price can be overridden
        # per signal via metadata (entry_price).
        entries = self.prepare_signals(normalized_signals, close_df)

        if entries.empty or not entries.any().any():
            return VectorBTResult(strategy_name=strategy_name, entry_mode="gap_open", trades=[])

        close = close_df.bfill().ffill()
        open_prices = open_df.bfill().ffill()
        high_prices = high_df.bfill().ffill()
        low_prices = low_df.bfill().ffill()

        exits, order_price, exit_reason_map, initial_stop_map = self._build_exit_signals(
            entries,
            close,
            open_prices,
            high_prices,
            low_prices,
            normalized_signals,
            delisting_dates,
        )

        slippage = self._build_slippage_matrix(close, value_traded_inr)

        # Resolve same-day entry-exit conflicts: exit takes priority.
        # With cash_sharing=True, VectorBT's default is to keep the existing position
        # when entry and exit both fire on the same date. This causes trades like
        # KESARENT (entry Jul 2, ABNORMAL_PROFIT exit Jul 5) to run for 192 days
        # because a new breakout entry on Jul 5 blocks the exit signal.
        entries = entries & ~exits

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Object has multiple columns",
                    category=UserWarning,
                )
                # Using configured portfolio value for realistic position sizing
                # Position sizing: allocate fixed value per position
                # With Rs 10L portfolio and max 10 positions = Rs 1L per trade
                position_value = self.config.default_portfolio_value / self.config.max_positions
                is_short = self.config.direction == PositionSide.SHORT
                pf = vbt.Portfolio.from_signals(
                    close=close,
                    entries=None if is_short else entries,
                    exits=None if is_short else exits,
                    short_entries=entries if is_short else None,
                    short_exits=exits if is_short else None,
                    open=open_prices,
                    price=order_price,
                    fees=self.config.fees_per_trade,
                    slippage=slippage,
                    init_cash=self.config.default_portfolio_value,
                    size=position_value,
                    size_type="value",
                    group_by=True,
                    cash_sharing=True,
                    freq="D",
                )

                stats = pf.stats()

            if isinstance(stats, pd.DataFrame):
                stats = stats.iloc[0]

            total_return = (
                stats["Total Return [%]"] / 100 if pd.notna(stats["Total Return [%]"]) else 0.0
            )
            sharpe = stats["Sharpe Ratio"] if pd.notna(stats["Sharpe Ratio"]) else 0.0
            max_dd = stats["Max Drawdown [%]"] / 100 if pd.notna(stats["Max Drawdown [%]"]) else 0.0
            win_rate = stats["Win Rate [%]"] / 100 if pd.notna(stats["Win Rate [%]"]) else 0.0

            trades = self._extract_trades(
                pf,
                close,
                value_traded_inr,
                exit_reason_map,
                initial_stop_map,
            )

            profit_factor = pf.get_profit_factor() if hasattr(pf, "get_profit_factor") else 0.0
            avg_r = self._calculate_avg_r(trades)
            median_r = self._calculate_median_r(trades)
            calmar_ratio = self._calculate_calmar_ratio(total_return, max_dd)
            r_distribution = self._calculate_r_distribution(trades)

            try:
                daily_returns = pf.returns() if hasattr(pf, "returns") else None
                if daily_returns is not None and len(daily_returns) > 0:
                    if hasattr(daily_returns, "values"):
                        returns_list = daily_returns.values.flatten().tolist()
                    else:
                        returns_list = (
                            list(daily_returns) if hasattr(daily_returns, "__iter__") else []
                        )
                    sortino_ratio = self._calculate_sortino_ratio(returns_list)
                else:
                    sortino_ratio = 0.0
            except (AttributeError, ValueError, TypeError) as e:
                # Known exceptions from returns calculation or sortino computation
                logger.debug("Could not calculate sortino ratio: %s", e)
                sortino_ratio = 0.0

            return VectorBTResult(
                strategy_name=strategy_name,
                entry_mode="gap_open",
                trades=trades,
                total_return=total_return,
                sharpe_ratio=sharpe,
                max_drawdown=max_dd,
                win_rate=win_rate,
                profit_factor=profit_factor,
                avg_r=avg_r,
                calmar_ratio=calmar_ratio,
                sortino_ratio=sortino_ratio,
                median_r=median_r,
                r_distribution=r_distribution,
            )

        except Exception as e:
            logger.error(f"VectorBT backtest failed: {e}")
            return VectorBTResult(strategy_name=strategy_name, entry_mode="gap_open", trades=[])

    def _extract_trades(
        self,
        pf: vbt.Portfolio,
        close: pd.DataFrame,
        value_traded_inr: dict[int, float],
        exit_reason_map: dict[tuple[int, date], ExitReason],
        initial_stop_map: dict[tuple[int, date], float],
    ) -> list[Trade]:
        trades: list[Trade] = []

        try:
            trade_records = getattr(pf.trades, "records_readable", None)
            if trade_records is None or trade_records.empty:
                return trades

            for _, record in trade_records.iterrows():
                col = None
                if "col" in record:
                    col = record["col"]
                elif "Column" in record:
                    col = record["Column"]

                # VectorBT returns the actual column value (symbol_id), not the index
                # The DataFrame columns are the symbol_ids from price_matrix
                symbol_id = int(col) if col is not None else 0
                if symbol_id == 0:
                    continue  # Skip invalid symbol_ids

                symbol = str(symbol_id)

                # Entry Timestamp is a pd.Timestamp, get the index in close DataFrame
                entry_ts = record.get("Entry Timestamp")
                if entry_ts is not None and pd.notna(entry_ts):
                    # Find the index of this timestamp in close.index
                    entry_idx = close.index.get_loc(entry_ts) if entry_ts in close.index else None
                elif "entry_idx" in record:
                    entry_idx = int(record["entry_idx"])
                else:
                    entry_idx = None

                # Exit Timestamp
                exit_ts = record.get("Exit Timestamp")
                if exit_ts is not None and pd.notna(exit_ts):
                    exit_idx = close.index.get_loc(exit_ts) if exit_ts in close.index else None
                elif "exit_idx" in record:
                    exit_idx = int(record["exit_idx"])
                else:
                    exit_idx = None

                # If still no valid entry_idx, skip
                if entry_idx is None:
                    continue

                entry_date = (
                    close.index[entry_idx].date()
                    if entry_idx < len(close.index)
                    else close.index[0].date()
                )
                exit_date = (
                    close.index[exit_idx].date()
                    if exit_idx is not None and exit_idx < len(close.index)
                    else None
                )

                entry_price = float(record.get("Avg Entry Price", np.nan))
                if pd.isna(entry_price) and "entry_price" in record:
                    entry_price = float(record["entry_price"])

                exit_price = float(record.get("Avg Exit Price", np.nan))
                if pd.isna(exit_price) and "exit_price" in record:
                    exit_price = float(record["exit_price"])

                qty = int(record.get("Size", 1))

                pnl = record.get("PnL", None)
                pnl_value = float(pnl) if pnl is not None and pd.notna(pnl) else None

                value_traded_inr_20 = value_traded_inr.get(symbol_id) if value_traded_inr else None
                slippage_bps = self._get_slippage_bps(value_traded_inr_20)

                entry_key = (symbol_id, entry_date)
                initial_stop = initial_stop_map.get(entry_key, entry_price * 0.98)
                exit_reason = exit_reason_map.get(entry_key)

                # R-multiple uses actual risk (entry - stop), not hardcoded 2%
                risk_per_share = (
                    entry_price - initial_stop if entry_price > initial_stop else entry_price * 0.02
                )

                trade = Trade(
                    symbol_id=symbol_id,
                    symbol=symbol,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    entry_mode="gap_open",
                    qty=qty,
                    initial_stop=initial_stop,
                    exit_date=exit_date,
                    exit_price=exit_price if pd.notna(exit_price) else None,
                    pnl=pnl_value,
                    pnl_r=(pnl_value / (risk_per_share * qty))
                    if pnl_value and risk_per_share > 0 and qty > 0
                    else None,
                    fees=float(record.get("Entry Fees", 0.0)) + float(record.get("Exit Fees", 0.0)),
                    slippage_bps=slippage_bps,
                    exit_reason=exit_reason,
                )
                trades.append(trade)

        except Exception as e:
            logger.warning("Could not extract detailed trades: %s", e)
            logger.warning(traceback.format_exc())

        return trades

    def _calculate_avg_r(self, trades: list[Trade]) -> float:
        if not trades:
            return 0.0

        r_values = [t.pnl_r for t in trades if t.pnl_r is not None]
        return float(np.mean(r_values)) if r_values else 0.0

    def _calculate_median_r(self, trades: list[Trade]) -> float:
        if not trades:
            return 0.0

        r_values = [t.pnl_r for t in trades if t.pnl_r is not None]
        return float(np.median(r_values)) if r_values else 0.0

    def _calculate_calmar_ratio(self, annualized_return: float, max_drawdown: float) -> float:
        if max_drawdown == 0:
            return 0.0
        return annualized_return / max_drawdown

    def _calculate_sortino_ratio(self, returns: list[float], risk_free_rate: float = 0.02) -> float:
        if not returns:
            return 0.0

        returns_arr = np.array(returns)
        excess_returns = returns_arr - risk_free_rate / 252

        downside_returns = excess_returns[excess_returns < 0]
        if len(downside_returns) == 0:
            return 0.0

        downside_std = np.std(downside_returns)
        if downside_std == 0:
            return 0.0

        return float(np.mean(excess_returns) / downside_std * np.sqrt(252))

    def _calculate_r_distribution(self, trades: list[Trade]) -> dict[str, float]:
        if not trades:
            return {}

        r_values = [t.pnl_r for t in trades if t.pnl_r is not None]
        if not r_values:
            return {}

        r_arr = np.array(r_values)

        percentiles = [10, 25, 50, 75, 90]
        result = {}

        for p in percentiles:
            result[f"r_p{p}"] = float(np.percentile(r_arr, p))

        positive_r = r_arr[r_arr > 0]
        negative_r = r_arr[r_arr < 0]

        result["win_rate_r"] = float(len(positive_r) / len(r_arr)) if r_arr.size > 0 else 0.0
        result["avg_winner_r"] = float(np.mean(positive_r)) if positive_r.size > 0 else 0.0
        result["avg_loser_r"] = float(np.mean(negative_r)) if negative_r.size > 0 else 0.0
        result["max_winner_r"] = float(np.max(r_arr)) if r_arr.size > 0 else 0.0
        result["max_loser_r"] = float(np.min(r_arr)) if r_arr.size > 0 else 0.0

        return result


def run_vectorbt_backtest(
    strategy_name: str,
    signals: list[tuple[date, int, str, float, dict]],
    price_data: dict[int, dict[date, dict[str, float]]],
    value_traded_inr: dict[int, float],
    delisting_dates: dict[int, date] | None = None,
) -> VectorBTResult:
    """Run backtest for gap-up breakout strategy.

    ENTRY TIMING:
        - Signal detected at T's open (gap-up breakout)
        - Entry at T's open price (same day)
        - NO look-ahead bias

    SURVIVORSHIP BIAS:
        - delisting_dates: Dict of symbol_id -> delisting_date
        - Force exit on delisting if position is open

    Args:
        strategy_name: Name of the strategy
        signals: List of (date, symbol_id, symbol, initial_stop, metadata)
        price_data: Dict of symbol_id -> {date -> {open_adj, close_adj, high_adj, low_adj}}
        value_traded_inr: Dict of symbol_id -> 20-day avg value traded
        delisting_dates: Dict of symbol_id -> delisting_date (optional)

    Returns:
        VectorBTResult with trades and metrics
    """
    engine = VectorBTEngine()
    return engine.run_backtest(
        strategy_name, signals, price_data, value_traded_inr, delisting_dates
    )
