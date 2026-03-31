from __future__ import annotations

import pytest

from nse_momentum_lab.services.kite.token_workflow import (
    KiteTokenWorkflowError,
    build_doppler_secret_command,
    extract_request_token,
)


def test_extract_request_token_from_callback_url() -> None:
    value = (
        "http://127.0.0.1:8004/auth/kite/callback?request_token=abc123&action=login&status=success"
    )
    assert extract_request_token(value) == "abc123"


def test_extract_request_token_accepts_raw_token() -> None:
    assert extract_request_token("abc123") == "abc123"


def test_extract_request_token_rejects_callback_without_token() -> None:
    with pytest.raises(KiteTokenWorkflowError):
        extract_request_token("http://127.0.0.1:8004/auth/kite/callback?status=success")


def test_build_doppler_secret_command_quotes_value() -> None:
    command = build_doppler_secret_command("abc123")
    assert command == "doppler secrets set KITE_ACCESS_TOKEN 'abc123'"
