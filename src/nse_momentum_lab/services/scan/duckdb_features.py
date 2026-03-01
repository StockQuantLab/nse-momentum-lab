"""DuckDB-based feature engine for fast loading of pre-computed features."""

from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.scan.features import DailyFeatures


class DuckDBFeatureEngine:
    """
    Load pre-computed features from DuckDB materialized tables.

    This is much faster than computing features on-the-fly.
    All features are pre-computed in the feat_daily table.
    """

    def __init__(self):
        self.db = get_market_db()

    def load_features_for_symbols(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, list[DailyFeatures]]:
        """
        Load pre-computed features for multiple symbols from DuckDB.

        Returns:
            Dict mapping symbol name to list of DailyFeatures
        """
        # Load all features in one query
        df = self.db.get_features_range(symbols, start_date.isoformat(), end_date.isoformat())

        if df.is_empty():
            return {}

        # Group by symbol and convert to DailyFeatures
        result = {}
        for symbol in symbols:
            symbol_df = df.filter(pl.col("symbol") == symbol).sort("trading_date")

            features_list = []
            for row in symbol_df.iter_rows(named=True):
                features = DailyFeatures(
                    symbol_id=0,  # Not used in 2LYNCH
                    trading_date=row["trading_date"],
                    close=0.0,  # Will be loaded separately if needed
                    high=0.0,
                    low=0.0,
                    ret_1d=float(row["ret_1d"]) if row["ret_1d"] else None,
                    ret_5d=float(row["ret_5d"]) if row["ret_5d"] else None,
                    atr_20=float(row["atr_20"]) if row["atr_20"] else None,
                    range_pct=float(row["range_pct"]) if row["range_pct"] else None,
                    close_pos_in_range=float(row["close_pos_in_range"])
                    if row["close_pos_in_range"]
                    else None,
                    ma_20=float(row["ma_20"]) if row["ma_20"] else None,
                    ma_65=float(row["ma_65"]) if row["ma_65"] else None,
                    ma_7=float(row["ma_7"]) if row.get("ma_7") is not None else None,
                    ma_65_sma=float(row["ma_65_sma"]) if row.get("ma_65_sma") is not None else None,
                    rs_252=float(row["rs_252"]) if row["rs_252"] else None,
                    vol_20=float(row["vol_20"]) if row["vol_20"] else None,
                    dollar_vol_20=float(row["dollar_vol_20"]) if row["dollar_vol_20"] else None,
                    # 2LYNCH filter features
                    r2_65=float(row["r2_65"]) if row.get("r2_65") is not None else None,
                    atr_compress_ratio=float(row["atr_compress_ratio"])
                    if row.get("atr_compress_ratio") is not None
                    else None,
                    range_percentile=float(row["range_percentile"])
                    if row.get("range_percentile") is not None
                    else None,
                    vol_dryup_ratio=float(row["vol_dryup_ratio"])
                    if row.get("vol_dryup_ratio") is not None
                    else None,
                    prior_breakouts_90d=int(row["prior_breakouts_90d"])
                    if row.get("prior_breakouts_90d") is not None
                    else None,
                )
                features_list.append(features)

            result[symbol] = features_list

        return result

    def load_daily_prices_for_symbols(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, list[tuple[date, float, float, float, float, int]]]:
        """
        Load daily OHLCV data from DuckDB.

        Uses query_daily_multi() to avoid N+1 query pattern (batch fetch all symbols at once).

        Returns:
            Dict mapping symbol name to list of (date, open, high, low, close, volume) tuples
        """
        if not symbols:
            return {}

        # Batch query all symbols at once instead of N+1 individual queries
        df = self.db.query_daily_multi(symbols, start_date.isoformat(), end_date.isoformat())

        result = {}
        if not df.is_empty():
            for symbol in symbols:
                symbol_df = df.filter(pl.col("symbol") == symbol)
                if not symbol_df.is_empty():
                    result[symbol] = [
                        (
                            row["date"],
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            row["volume"],
                        )
                        for row in symbol_df.iter_rows(named=True)
                    ]

        return result

    def get_features_for_date(
        self,
        symbol: str,
        trading_date: date,
    ) -> DailyFeatures | None:
        """Get features for a specific symbol and date."""
        features_dict = self.db.get_features(symbol, trading_date.isoformat())

        if not features_dict:
            return None

        return DailyFeatures(
            symbol_id=0,
            trading_date=trading_date,
            close=0.0,
            high=0.0,
            low=0.0,
            ret_1d=features_dict.get("ret_1d"),
            ret_5d=features_dict.get("ret_5d"),
            atr_20=features_dict.get("atr_20"),
            range_pct=features_dict.get("range_pct"),
            close_pos_in_range=features_dict.get("close_pos_in_range"),
            ma_20=features_dict.get("ma_20"),
            ma_65=features_dict.get("ma_65"),
            rs_252=features_dict.get("rs_252"),
            vol_20=features_dict.get("vol_20"),
            dollar_vol_20=features_dict.get("dollar_vol_20"),
        )
