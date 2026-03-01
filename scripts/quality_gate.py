from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class GateStep:
    name: str
    cmd: list[str]


def _run_step(step: GateStep) -> None:
    printable = " ".join(shlex.quote(token) for token in step.cmd)
    print(f"\n[QUALITY] {step.name}")
    print(f"[CMD] {printable}")
    result = subprocess.run(step.cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def build_steps(with_integration: bool, with_full: bool, with_format_check: bool) -> list[GateStep]:
    steps = [
        GateStep("Ruff Lint", ["uv", "run", "ruff", "check", "src", "apps"]),
        GateStep("Mypy", ["uv", "run", "mypy", "src", "tests"]),
    ]
    if with_format_check:
        steps.insert(
            1,
            GateStep(
                "Ruff Format Check", ["uv", "run", "ruff", "format", "--check", "src", "apps"]
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
        help="Include global ruff format check.",
    )
    args = parser.parse_args()

    print("[QUALITY] Starting quality gate run")
    if args.with_full and args.with_integration:
        print(
            "[QUALITY] Note: --with-integration ignored because --with-full already includes integration."
        )
    for step in build_steps(args.with_integration, args.with_full, args.with_format_check):
        _run_step(step)
    print("\n[QUALITY] All selected gates passed.")


if __name__ == "__main__":
    main()
