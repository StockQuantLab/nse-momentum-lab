"""Shared unit-test fixtures and environment guards."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

_UNIT_TMP_ROOT = Path(__file__).resolve().parents[2] / ".unit-tmp"
_UNIT_TMP_ROOT.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def tmp_path() -> Path:
    """Return a repo-local temporary directory for tests that need one."""
    path = _UNIT_TMP_ROOT / f"tmp-{uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=False)
    return path
