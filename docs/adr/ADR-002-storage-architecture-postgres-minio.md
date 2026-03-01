# ADR-002: Storage Architecture (Postgres + MinIO)

Status: Accepted

## Context

The platform must persist time-series, scans, trades, experiment metadata, agent memory, and large artifacts such as equity curves and raw ZIP files.

## Decision

- PostgreSQL is the system of record.
- MinIO (S3 compatible) stores raw and derived artifacts.

## Rationale

- Strong transactional semantics for experiments.
- S3 layout mirrors production cloud patterns.

## Consequences

- docker-compose required.
- Schema migrations must be versioned.

## Interfaces

- All services access Postgres via SQLAlchemy.
- Artifacts uploaded via S3 SDK.

## Risks

- Disk exhaustion; schema drift.
