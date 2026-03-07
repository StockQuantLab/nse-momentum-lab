# ADR-015: Signal & Trade Lifecycle State Machine

Status: Accepted

## Context

Signals evolve into paper trades, then closed trades. State transitions must be auditable.

## Decision

Formal state machine: NEW → QUALIFIED → ALERTED → ENTERED → MANAGED → EXITED → ARCHIVED.

## Consequences

UI and agents must respect transitions.
