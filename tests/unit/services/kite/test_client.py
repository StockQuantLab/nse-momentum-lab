from __future__ import annotations

import httpx

from nse_momentum_lab.services.kite.client import KiteConnectClient


class _DummyClient:
    def __init__(self) -> None:
        self.last_request: dict[str, object] | None = None

    def request(self, method, url, params=None, data=None, headers=None):
        self.last_request = {
            "method": method,
            "url": url,
            "params": params,
            "data": data,
            "headers": headers,
        }
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"status": "success", "data": {"access_token": "token-123"}},
        )

    def close(self) -> None:
        return None


class TestKiteConnectClient:
    def test_login_url_appends_api_key(self) -> None:
        client = KiteConnectClient(
            api_key="kite-key",
            api_secret="kite-secret",
            client=_DummyClient(),
        )
        assert "api_key=kite-key" in client.login_url()

    def test_generate_session_sets_access_token(self) -> None:
        dummy = _DummyClient()
        client = KiteConnectClient(
            api_key="kite-key",
            api_secret="kite-secret",
            client=dummy,
        )

        payload = client.generate_session("request-token")

        assert payload["access_token"] == "token-123"
        assert client.access_token == "token-123"
        assert dummy.last_request is not None
        assert dummy.last_request["method"] == "POST"
        assert dummy.last_request["url"] == "https://api.kite.trade/session/token"
