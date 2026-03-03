"""
Hash utility functions for consistent hashing across the codebase.

Consolidates duplicated SHA256 hashing patterns found in:
- services/backtest/duckdb_backtest_runner.py
- services/backtest/persistence.py
- services/backtest/registry.py
- services/dataset/manifest.py
- cli/backtest_batch.py
- cli/pipeline.py
- db/market_db.py
"""

import hashlib
import json
from typing import Any


def compute_short_hash(
    data: dict[str, Any] | str | bytes,
    length: int = 16,
    sort_keys: bool = True,
) -> str:
    """
    Compute SHA256 hash and return first N characters.

    Args:
        data: Input data as dict, string, or bytes.
        length: Number of characters to return from hash.
        sort_keys: For dict input, sort keys before encoding.

    Returns:
        First `length` characters of SHA256 hex digest.

    Examples:
        >>> compute_short_hash({"a": 1, "b": 2})
        "a1b2c3d4e5f6g7h8"
        >>> compute_short_hash("hello", length=8)
        "2cf24dba"
    """
    if isinstance(data, dict):
        blob = json.dumps(data, sort_keys=sort_keys).encode()
    elif isinstance(data, str):
        blob = data.encode()
    else:
        blob = data

    return hashlib.sha256(blob).hexdigest()[:length]


def compute_full_hash(data: dict[str, Any] | str | bytes) -> str:
    """
    Compute full SHA256 hash.

    Args:
        data: Input data as dict, string, or bytes.

    Returns:
        Full SHA256 hex digest.

    Examples:
        >>> compute_full_hash("hello")
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    """
    if isinstance(data, dict):
        blob = json.dumps(data, sort_keys=True).encode()
    elif isinstance(data, str):
        blob = data.encode()
    else:
        blob = data

    return hashlib.sha256(blob).hexdigest()


def compute_composite_hash(*parts: str | dict[str, Any], length: int = 16) -> str:
    """
    Compute hash from multiple parts joined by colons.

    Useful for creating composite identifiers from multiple hashes.

    Args:
        *parts: Variable number of strings or dicts to hash.
        length: Number of characters to return from hash.

    Returns:
        First `length` characters of SHA256 hex digest.

    Examples:
        >>> compute_composite_hash("abc123", "def456")
        "1a2b3c4d5e6f7g8h"
    """
    blob = ":".join(
        part if isinstance(part, str) else json.dumps(part, sort_keys=True) for part in parts
    ).encode()

    return hashlib.sha256(blob).hexdigest()[:length]
