# ADR-013: Database Schema & Partitioning

Status: Accepted

## Context

OHLCV data, scans, and trades will grow to billions of rows over multi-year research. Poor schema design will cripple performance.

## Decision

- Time-series tables partitioned by month.
- Primary keys: (symbol_id, trading_date).
- Separate raw vs adjusted tables.
- Foreign keys to experiment registry.

## Rationale

- Fast range scans.
- Cheap archival.

## Consequences

Migration tooling required.
