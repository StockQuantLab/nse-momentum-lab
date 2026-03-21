from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx


class KiteAPIError(RuntimeError):
    """Raised when Kite returns an error payload or transport failure."""


@dataclass(slots=True)
class KiteAuthSession:
    api_key: str
    access_token: str | None = None
    public_token: str | None = None
    user_id: str | None = None
    login_url: str = "https://kite.zerodha.com/connect/login?v=3"
    api_root: str = "https://api.kite.trade"


class KiteConnectClient:
    """Small, dependency-light Kite Connect v3/v4 REST client.

    This mirrors the official client surface closely enough for paper/live
    scaffolding while keeping auth and transport fully explicit in this repo.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str | None = None,
        access_token: str | None = None,
        login_url: str = "https://kite.zerodha.com/connect/login?v=3",
        api_root: str = "https://api.kite.trade",
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.login_url_base = login_url
        self.api_root = api_root.rstrip("/")
        self._client = client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> KiteConnectClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def set_access_token(self, access_token: str) -> None:
        self.access_token = access_token

    def set_api_secret(self, api_secret: str) -> None:
        self.api_secret = api_secret

    def login_url(self) -> str:
        parsed = urlparse(self.login_url_base)
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        params["api_key"] = self.api_key
        return urlunparse(parsed._replace(query=urlencode(params)))

    def generate_session(self, request_token: str, api_secret: str | None = None) -> dict[str, Any]:
        secret = api_secret or self.api_secret
        if not secret:
            raise KiteAPIError("Kite API secret is required to generate a session")

        checksum = hashlib.sha256(f"{self.api_key}{request_token}{secret}".encode()).hexdigest()
        payload = self._request_json(
            "POST",
            "/session/token",
            data={
                "api_key": self.api_key,
                "request_token": request_token,
                "checksum": checksum,
            },
        )
        if isinstance(payload, dict) and "access_token" in payload:
            self.access_token = str(payload["access_token"])
        return payload

    def invalidate_access_token(self) -> dict[str, Any]:
        return self._request_json(
            "DELETE",
            "/session/token",
            data={"api_key": self.api_key, "access_token": self.access_token or ""},
        )

    def profile(self) -> dict[str, Any]:
        return self._request_json("GET", "/user/profile")

    def margins(self, segment: str | None = None) -> dict[str, Any]:
        path = "/user/margins" if not segment else f"/user/margins/{segment}"
        return self._request_json("GET", path)

    def holdings(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/portfolio/holdings")
        return self._payload_to_list(payload)

    def positions(self) -> dict[str, Any]:
        return self._request_json("GET", "/portfolio/positions")

    def orders(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/orders")
        return self._payload_to_list(payload)

    def trades(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/trades")
        return self._payload_to_list(payload)

    def order_history(self, order_id: str | int) -> list[dict[str, Any]]:
        payload = self._request_json("GET", f"/orders/{order_id}")
        return self._payload_to_list(payload)

    def order_trades(self, order_id: str | int) -> list[dict[str, Any]]:
        payload = self._request_json("GET", f"/orders/{order_id}/trades")
        return self._payload_to_list(payload)

    def instruments(self, exchange: str | None = None) -> list[dict[str, str]]:
        path = "/instruments" if exchange is None else f"/instruments/{exchange}"
        response = self._request("GET", path, expect_json=False)
        text = response.text
        if not text.strip():
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    def quote(self, instruments: Sequence[str] | Iterable[str]) -> dict[str, Any]:
        return self._quote_like("/quote", instruments)

    def ohlc(self, instruments: Sequence[str] | Iterable[str]) -> dict[str, Any]:
        return self._quote_like("/quote/ohlc", instruments)

    def ltp(self, instruments: Sequence[str] | Iterable[str]) -> dict[str, Any]:
        return self._quote_like("/quote/ltp", instruments)

    def historical_data(
        self,
        instrument_token: int,
        interval: str,
        from_date: date | datetime | str,
        to_date: date | datetime | str,
        *,
        continuous: bool = False,
        oi: bool = False,
    ) -> list[dict[str, Any]]:
        payload = self._request_json(
            "GET",
            f"/instruments/historical/{instrument_token}/{interval}",
            params={
                "from": self._format_date(from_date),
                "to": self._format_date(to_date),
                "continuous": 1 if continuous else 0,
                "oi": 1 if oi else 0,
            },
        )
        if isinstance(payload, dict):
            candles = payload.get("candles")
            if isinstance(candles, list):
                return candles
        return list(payload) if isinstance(payload, list) else []

    def place_order(self, **payload: Any) -> dict[str, Any]:
        variety = payload.pop("variety")
        return self._request_json("POST", f"/orders/{variety}", data=self._clean_payload(payload))

    def modify_order(self, order_id: str | int, **payload: Any) -> dict[str, Any]:
        variety = payload.pop("variety")
        return self._request_json(
            "PUT", f"/orders/{variety}/{order_id}", data=self._clean_payload(payload)
        )

    def cancel_order(self, order_id: str | int, variety: str) -> dict[str, Any]:
        return self._request_json("DELETE", f"/orders/{variety}/{order_id}")

    def get_gtts(self, from_index: int = 0, limit: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"from": from_index}
        if limit is not None:
            params["limit"] = limit
        return self._request_json("GET", "/gtt/triggers", params=params)

    def place_gtt(self, **payload: Any) -> dict[str, Any]:
        return self._request_json("POST", "/gtt/triggers", data=self._clean_payload(payload))

    def modify_gtt(self, trigger_id: int, **payload: Any) -> dict[str, Any]:
        return self._request_json(
            "PUT", f"/gtt/triggers/{trigger_id}", data=self._clean_payload(payload)
        )

    def delete_gtt(self, trigger_id: int) -> dict[str, Any]:
        return self._request_json("DELETE", f"/gtt/triggers/{trigger_id}")

    def _quote_like(self, path: str, instruments: Sequence[str] | Iterable[str]) -> dict[str, Any]:
        params = [("i", instrument) for instrument in self._normalize_instruments(instruments)]
        payload = self._request_json("GET", path, params=params)
        return payload if isinstance(payload, dict) else {}

    def _normalize_instruments(self, instruments: Sequence[str] | Iterable[str]) -> list[str]:
        items = [instrument.strip() for instrument in instruments if str(instrument).strip()]
        return list(dict.fromkeys(items))

    def _format_date(self, value: date | datetime | str) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return value

    def _clean_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in payload.items() if value is not None}

    def _payload_to_list(self, payload: dict[str, Any] | list[Any]) -> list[Any]:
        if isinstance(payload, list):
            return payload
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, list):
            return data
        if data is None:
            return []
        return [data]

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        data: Any | None = None,
        expect_json: bool = True,
    ) -> httpx.Response:
        headers = {"X-Kite-Version": "3"}
        if self.access_token:
            headers["Authorization"] = f"token {self.api_key}:{self.access_token}"

        response = self._client.request(
            method,
            f"{self.api_root}{path}",
            params=params,
            data=data,
            headers=headers,
        )
        response.raise_for_status()
        if expect_json:
            self._raise_for_api_error(response)
        return response

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        data: Any | None = None,
    ) -> dict[str, Any] | list[Any]:
        response = self._request(method, path, params=params, data=data, expect_json=True)
        payload = response.json()
        self._raise_for_api_error_payload(payload)
        if isinstance(payload, dict) and "data" in payload:
            return payload["data"]
        return payload

    def _raise_for_api_error(self, response: httpx.Response) -> None:
        payload = response.json()
        self._raise_for_api_error_payload(payload)

    def _raise_for_api_error_payload(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("status") == "error":
            message = payload.get("message") or payload.get("error_type") or "Kite API error"
            raise KiteAPIError(str(message))
