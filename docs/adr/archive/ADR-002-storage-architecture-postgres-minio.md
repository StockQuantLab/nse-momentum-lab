# ADR-002: Storage Architecture (Postgres + MinIO)

**Status**: Accepted
**Date**: 2026-02-20
**Updated**: 2026-03-06 (clarified hybrid architecture per ADR-009)

## Context

The platform must persist time-series, scans, trades, experiment metadata, agent memory, and large artifacts such as equity curves and raw ZIP files.

## Decision

- **PostgreSQL** is the system of record for operational data (experiments, signals, paper trading, reference data).
- **DuckDB + Parquet** stores market data and features (see ADR-009 for details).
- **MinIO** (S3 compatible) stores raw and derived artifacts.

## Rationale

- PostgreSQL provides strong transactional semantics for experiments and signals.
- DuckDB excels at analytical queries on time-series market data (100-1000x faster than PostgreSQL).
- S3 layout mirrors production cloud patterns.

## Consequences

- docker-compose required.
- Schema migrations must be versioned for PostgreSQL.
- Market data ingestion writes to Parquet files directly.

## Interfaces

- Operational services access PostgreSQL via SQLAlchemy.
- Analytics services access DuckDB for market data and features.
- Artifacts uploaded via S3 SDK to MinIO.

## Risks

- Disk exhaustion; schema drift.
