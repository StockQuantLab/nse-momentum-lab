from __future__ import annotations

from types import SimpleNamespace

from nse_momentum_lab.services.kite.auth import KiteAuth


def test_get_instrument_token_refreshes_only_once_per_exchange(monkeypatch) -> None:
    monkeypatch.setattr(
        "nse_momentum_lab.services.kite.auth.get_settings",
        lambda: SimpleNamespace(kite_api_key=None, kite_access_token=None),
    )
    auth = KiteAuth()
    refresh_calls: list[str] = []

    monkeypatch.setattr(auth, "is_authenticated", lambda: True)
    monkeypatch.setattr(auth, "get_instruments", lambda exchange="NSE": [])

    def _refresh(exchange: str = "NSE") -> int:
        refresh_calls.append(exchange)
        return 0

    monkeypatch.setattr(auth, "refresh_instruments", _refresh)

    assert auth.get_instrument_token("UNKNOWN1") is None
    assert auth.get_instrument_token("UNKNOWN2") is None
    assert refresh_calls == ["NSE"]
