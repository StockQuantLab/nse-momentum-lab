# ADR-019: Dividend Adjustment vs Total Return (TRI)

Status: Accepted

## Context

The system needs corporate-action adjusted time series for indicator correctness and reproducible backtests. Splits/bonus/rights are multiplicative and must be handled robustly. Dividends are cash distributions that can be handled either by adjusting historical prices for continuity or by computing a total return series.

Incorrect dividend adjustment is a common source of silent backtest distortion, especially when coverage is incomplete.

## Decision

Phase 1 implementation will:

- Fully adjust OHLCV for splits/bonus/rights.
- Store dividends as corporate action events.
- Use price-only adjusted series for core scans/backtests.
- Add total return (TRI) series later as an optional feature.

Dividend price-adjustment for continuity may be added later behind a feature flag if/when dividend coverage and correctness is validated.

## Rationale

- This project’s primary horizon is short (3–5 day momentum bursts), where dividends typically have minor impact relative to the move.
- Dividend data quality issues can create larger distortions than the dividend effect itself.
- Separating dividends keeps assumptions explicit and makes results easier to audit.

## Consequences

- Core metrics are “price return” unless TRI is enabled.
- The experiment registry must record whether TRI is used.

## Migration

Introduce TRI without changing the raw/adjusted price tables by adding a separate derived series/table and recording the mode in experiment metadata.
