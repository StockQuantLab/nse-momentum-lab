# ADR-001: Market Data Sources & Licensing

**Status**: Accepted
**Date**: 2026-02-15

## Context

The system must support long-horizon research, survivorship-bias-free backtests, and real-time paper trading. Indian equity data is fragmented across official exchange downloads, broker feeds, and paid vendors. Some sources impose redistribution restrictions.

## Decision

- Historical daily OHLCV will be sourced from **Zerodha** (via manual download from Jio Cloud repository).
- Corporate-action and delisting files will be sourced from NSE.
- Real-time paper-trading data will come from broker websocket feeds.
- Paid vendors may be added only as secondary feeds.

## Rationale

- Zerodha is a reliable broker data source with comprehensive historical coverage.
- Manual download avoids NSE website throttling and CAPTCHA issues.
- Broker feeds are already licensed to the account holder.

## Consequences / Constraints

- Manual download process for initial dataset (one-time setup).
- Corporate-action rebuild pipelines are mandatory.
- Symbol lifecycle tables must be maintained.

## External Dependencies

| System | Purpose |
|---|---|
| Zerodha (Jio Cloud) | Historical files (2015-2025) |
| NSE Website | Corporate actions, delisting |
| Broker API | Live bars |

## Risks

- Manual download process may be time-consuming for large datasets.
- Missing delisted symbols.

## Migration

Additional vendor ingestion can be added in parallel without changing downstream schema.
