# ADR-005: NSE Ingestion Pipeline

**Status**: DEPRECATED

**Deprecated**: 2025-02-10
**Reason**: Project migrated to Zerodha data source (manual download from Jio Cloud)
**See**: [Zerodha Data Setup](../guides/ZERODHA_DATA_SETUP.md)

---

Status: Accepted (Historical)

## Context

NSE publishes dozens of daily files; only a subset is relevant for equities research.

## Decision

Downloader service fetches:
- equity bhavcopies
- symbol directories
- corporate actions
- delisted lists

Raw files archived to MinIO; parsed rows persisted to Postgres.

## S3 / MinIO Layout

market-data/
 └── nse/
     ├── bhavcopy/{yyyy}/{mm}/
     ├── corp-actions/{type}/
     └── symbols/

## Consequences

- Daily cron jobs.
- Idempotent re-runs.
