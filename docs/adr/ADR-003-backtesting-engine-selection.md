# ADR-003: Backtesting Engine Selection

Status: Accepted

## Context

The research loop requires fast multi-asset evaluation and parameter sweeps over thousands of NSE symbols.

## Decision

vectorbt is the canonical backtesting engine.

## Alternatives Considered

- Backtrader
- Zipline
- Custom loop engines

## Rationale

- Vectorized NumPy core.
- Walk-forward friendly.
- Easy to parallelize.

## Consequences

- Data must be pivoted to wide matrices.
- Memory pressure for long universes.

## Migration

Intraday engines may be added later without replacing vectorbt.
