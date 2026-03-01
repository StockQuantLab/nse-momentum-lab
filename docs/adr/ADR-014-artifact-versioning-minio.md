# ADR-014: Artifact Versioning in MinIO

Status: Accepted

## Context

Backtests generate large binary outputs that must be immutable and reproducible.

## Decision

- All artifacts stored under content-addressed prefixes.
- SHA-256 checksums recorded in Postgres.

## Layout

artifacts/
 └── experiments/{hash}/
     ├── equity.parquet
     ├── trades.parquet
     └── charts/

## Consequences

No overwrites permitted.
