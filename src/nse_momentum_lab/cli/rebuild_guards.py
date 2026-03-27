from __future__ import annotations

import argparse


def require_full_rebuild_ack(
    parser: argparse.ArgumentParser,
    *,
    force: bool,
    allow_full_rebuild: bool,
    operation: str,
    incremental_hint: str,
) -> None:
    """Reject destructive full rebuilds unless the operator explicitly acknowledges them."""
    if not force or allow_full_rebuild:
        return

    parser.error(
        f"{operation} with --force is destructive and expensive. "
        f"Use {incremental_hint} for normal short-window refreshes. "
        "If you intentionally want the full rebuild, add --allow-full-rebuild."
    )
