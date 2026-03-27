"""Shared unit-test fixtures and environment guards."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


_UNIT_TMP_ROOT = Path(__file__).resolve().parents[2] / ".unit-tmp"
_UNIT_TMP_ROOT.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def tmp_path() -> Path:
    """Return a repo-local temporary directory for tests that need one."""
    return Path(tempfile.mkdtemp(prefix="tmp-", dir=_UNIT_TMP_ROOT))
