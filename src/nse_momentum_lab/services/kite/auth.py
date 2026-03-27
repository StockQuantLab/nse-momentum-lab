from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.services.kite.client import KiteConnectClient

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW_KITE_DIR = PROJECT_ROOT / "data" / "raw" / "kite"
INSTRUMENTS_DIR = RAW_KITE_DIR / "instruments"
NSE_ALLOWLIST_PATH = PROJECT_ROOT / "data" / "NSE_EQUITY_SYMBOLS.csv"


class KiteAuth:
    """Singleton-style access to authenticated Kite REST helpers."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._client: KiteConnectClient | None = None
        self._instrument_rows: dict[str, list[dict[str, Any]]] = {}
        self._instrument_tokens: dict[tuple[str, str], int] = {}
        self._miss_cache: set[tuple[str, str]] = set()
        self._refresh_attempted: set[str] = set()

    def is_authenticated(self) -> bool:
        return bool(self._settings.kite_api_key and self._settings.kite_access_token)

    def get_kite_client(self) -> KiteConnectClient:
        if not self.is_authenticated():
            raise RuntimeError(
                "Kite credentials are not available. Set KITE_API_KEY and KITE_ACCESS_TOKEN via Doppler."
            )
        if self._client is None:
            self._client = KiteConnectClient(
                api_key=self._settings.kite_api_key or "",
                api_secret=self._settings.kite_api_secret,
                access_token=self._settings.kite_access_token,
                login_url=self._settings.kite_login_url,
                api_root=self._settings.kite_api_root,
            )
        return self._client

    def get_instrument_master_path(self, exchange: str = "NSE") -> Path:
        INSTRUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        return INSTRUMENTS_DIR / f"{exchange.strip().upper()}.csv"

    def refresh_instruments(self, exchange: str = "NSE") -> int:
        exchange_key = exchange.strip().upper()
        client = self.get_kite_client()
        rows = client.instruments(exchange_key)
        filtered_rows = self._filter_instruments(exchange_key, rows)
        path = self.get_instrument_master_path(exchange_key)
        self._write_instrument_cache(path, filtered_rows)
        self._instrument_rows.pop(exchange_key, None)
        self._rebuild_token_cache(exchange_key, filtered_rows)
        self._miss_cache = {key for key in self._miss_cache if key[0] != exchange_key}
        self._refresh_attempted.discard(exchange_key)
        logger.info(
            "Refreshed Kite instrument cache for %s: %d rows", exchange_key, len(filtered_rows)
        )
        return len(filtered_rows)

    def get_instruments(self, exchange: str = "NSE") -> list[dict[str, Any]]:
        exchange_key = exchange.strip().upper()
        cached = self._instrument_rows.get(exchange_key)
        if cached is not None:
            return cached

        path = self.get_instrument_master_path(exchange_key)
        if not path.exists():
            if not self.is_authenticated():
                return []
            self.refresh_instruments(exchange_key)

        rows = self._read_instrument_cache(path)
        self._instrument_rows[exchange_key] = rows
        self._rebuild_token_cache(exchange_key, rows)
        return rows

    def get_instrument_token(self, tradingsymbol: str, exchange: str = "NSE") -> int | None:
        exchange_key = exchange.strip().upper()
        symbol_key = tradingsymbol.strip().upper()
        if not symbol_key:
            return None

        cache_key = (exchange_key, symbol_key)
        token = self._instrument_tokens.get(cache_key)
        if token is not None:
            return token

        self.get_instruments(exchange_key)
        token = self._instrument_tokens.get(cache_key)
        if token is not None:
            return token

        if cache_key in self._miss_cache or not self.is_authenticated():
            return None

        if exchange_key in self._refresh_attempted:
            self._miss_cache.add(cache_key)
            return None

        self._refresh_attempted.add(exchange_key)
        self.refresh_instruments(exchange_key)
        token = self._instrument_tokens.get(cache_key)
        if token is None:
            self._miss_cache.add(cache_key)
        return token

    def _filter_instruments(
        self,
        exchange: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if exchange != "NSE":
            return rows

        allowlist = self._load_nse_allowlist()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            tradingsymbol = str(row.get("tradingsymbol") or "").strip().upper()
            segment = str(row.get("segment") or "").strip().upper()
            row_exchange = str(row.get("exchange") or "").strip().upper()
            if not tradingsymbol:
                continue
            if row_exchange and row_exchange != "NSE":
                continue
            if segment and segment != "NSE":
                continue
            if allowlist and tradingsymbol not in allowlist:
                continue
            filtered.append(row)
        return filtered

    def _load_nse_allowlist(self) -> set[str]:
        if not NSE_ALLOWLIST_PATH.exists():
            return set()

        allowlist: set[str] = set()
        with NSE_ALLOWLIST_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw_row in reader:
                row = {str(key).strip(): value for key, value in raw_row.items()}
                if str(row.get("SERIES") or "").strip().upper() != "EQ":
                    continue
                symbol = str(row.get("SYMBOL") or "").strip().upper()
                if symbol:
                    allowlist.add(symbol)
        return allowlist

    def _write_instrument_cache(self, path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = (
            list(rows[0].keys())
            if rows
            else [
                "instrument_token",
                "exchange_token",
                "tradingsymbol",
                "name",
                "last_price",
                "expiry",
                "strike",
                "tick_size",
                "lot_size",
                "instrument_type",
                "segment",
                "exchange",
            ]
        )
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _read_instrument_cache(self, path: Path) -> list[dict[str, Any]]:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]

    def _rebuild_token_cache(self, exchange: str, rows: list[dict[str, Any]]) -> None:
        self._instrument_tokens = {
            key: value for key, value in self._instrument_tokens.items() if key[0] != exchange
        }
        for row in rows:
            symbol = str(row.get("tradingsymbol") or "").strip().upper()
            token_raw = str(row.get("instrument_token") or "").strip()
            if not symbol or not token_raw:
                continue
            try:
                token = int(token_raw)
            except ValueError:
                continue
            self._instrument_tokens[(exchange, symbol)] = token


_kite_auth: KiteAuth | None = None


def get_kite_auth() -> KiteAuth:
    global _kite_auth
    if _kite_auth is None:
        _kite_auth = KiteAuth()
    return _kite_auth
