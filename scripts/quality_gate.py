from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class GateStep:
    name: str
    cmd: list[str]
    auto_stage: bool = False  # If True, stage changed files after command


def _run_step(step: GateStep) -> None:
    printable = " ".join(shlex.quote(token) for token in step.cmd)
    print(f"\n[QUALITY] {step.name}")
    print(f"[CMD] {printable}")
    result = subprocess.run(step.cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)

    # Auto-stage formatted files if requested
    if step.auto_stage:
        # Stage any files that were formatted
        subprocess.run(
            ["git", "add", "-u"],  # Stage all modified tracked files
            check=False,
        )
        print("[QUALITY] >>> Formatted files staged automatically")


def build_steps(
    with_integration: bool,
    with_full: bool,
    with_format_check: bool,
    auto_format: bool = False,
) -> list[GateStep]:
    steps = [
        GateStep("Ruff Lint", ["uv", "run", "ruff", "check", "src", "apps"]),
        GateStep("Mypy", ["uv", "run", "mypy", "src", "tests"]),
    ]

    # For pre-commit: auto-format and stage (frictionless)
    # For pre-push/manual: check-only (fails fast, no silent changes)
    if with_format_check:
        if auto_format:
            # Auto-format and stage changed files
            steps.insert(
                1,
                GateStep(
                    "Ruff Format",
                    ["uv", "run", "ruff", "format", "src", "apps"],
                    auto_stage=True,
                ),
            )
        else:
            # Check-only mode (fails if formatting needed)
            steps.insert(
                1,
                GateStep(
                    "Ruff Format Check",
                    ["uv", "run", "ruff", "format", "--check", "src", "apps"],
                ),
            )

    if with_full:
        # Full suite already includes unit + integration tests.
        steps.append(GateStep("Full Test Suite", ["uv", "run", "pytest", "-q"]))
        return steps

    steps.append(GateStep("Unit Tests", ["uv", "run", "pytest", "tests/unit", "-v"]))
    if with_integration:
        steps.append(
            GateStep("Integration Tests", ["uv", "run", "pytest", "tests/integration", "-v"])
        )
    return steps


def main() -> None:
    parser = argparse.ArgumentParser(description="Run git/push quality gates.")
    parser.add_argument(
        "--with-integration",
        action="store_true",
        help="Run integration tests in addition to unit tests (ignored when --with-full is set).",
    )
    parser.add_argument(
        "--with-full",
        action="store_true",
        help="Run full test suite (covers unit + integration); skips separate unit/integration steps.",
    )
    parser.add_argument(
        "--with-format-check",
        action="store_true",
        help="Include ruff format check.",
    )
    parser.add_argument(
        "--auto-format",
        action="store_true",
        help="Auto-format and stage changes (for pre-commit hooks).",
    )
    args = parser.parse_args()

    print("[QUALITY] Starting quality gate run")
    if args.with_full and args.with_integration:
        print(
            "[QUALITY] Note: --with-integration ignored because --with-full already includes integration."
        )

    for step in build_steps(
        args.with_integration, args.with_full, args.with_format_check, args.auto_format
    ):
        _run_step(step)

    print("\n[QUALITY] All selected gates passed.")


if __name__ == "__main__":
    main()
