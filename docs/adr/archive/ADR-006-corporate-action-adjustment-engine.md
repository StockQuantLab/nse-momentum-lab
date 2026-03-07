# ADR-006: Corporate-Action Adjustment Engine

Status: Accepted

## Context

Unadjusted prices distort returns and stop logic.

## Decision

Backward-adjust all OHLC for splits, bonuses, dividends, rights.

Refinement:
- Dividend handling for Phase 1 is clarified in ADR-019 (dividends stored as events; TRI later; dividend price adjustment optional behind a feature flag).

## Rationale

Preserves continuity for historical signals.

## Consequences

Full-history recompute required when new action discovered.

## Risks

Incorrect ratios causing spurious returns.
