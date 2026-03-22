from __future__ import annotations

from nse_momentum_lab.services.kite.token_workflow import _decode_subprocess_output


def test_decode_subprocess_output_handles_utf8() -> None:
    assert _decode_subprocess_output("ok".encode("utf-8")) == "ok"


def test_decode_subprocess_output_handles_cp1252_bytes() -> None:
    assert _decode_subprocess_output(bytes([0x93, 0x94])) != ""
