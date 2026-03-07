# ADR-009: Experiment Registry

Status: Accepted

## Context

Repeated parameter searches waste compute.

## Decision

Registry table stores dataset hash, parameters, metrics, artifact URIs.

## Consequences

Agents must query registry before launching jobs.
