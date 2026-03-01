# ADR-012: Deployment & Operations

Status: Accepted

## Context

System runs locally in Docker.

## Decision

docker-compose stack: Postgres, MinIO, agents, API, scheduler, dashboard.

## Consequences

Nightly backups; versioned configs.
