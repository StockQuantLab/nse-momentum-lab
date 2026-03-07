# ADR-004: Agent Orchestration (phidata + GLM)

Status: Accepted

## Context

Research cycles repeat daily: ingest → scan → backtest → evaluate → deploy → monitor. Manual orchestration does not scale.

## Decision

- phidata orchestrates workflows.
- GLM models perform reasoning, summarization, anomaly analysis only.
- No price math inside LLM calls.

## Rationale

- Deterministic computation stays in Python.
- LLM cost controlled.

## Consequences

Agent state stored in Postgres.
