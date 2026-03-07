# ADR Index

This folder contains Architecture Decision Records (ADRs) for nse-momentum-lab.

## Active ADRs

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| **ADR-001** | [Data & Storage Architecture](ADR-001-data-and-storage.md) | Accepted | 2026-03-06 |
| **ADR-002** | [Ingestion & Adjustment](ADR-002-ingestion-and-adjustment.md) | Accepted | 2026-03-06 |
| **ADR-003** | [Backtesting System](ADR-003-backtesting-system.md) | Accepted | 2026-03-06 |
| **ADR-004** | [Paper Trading & Risk Governance](ADR-004-paper-trading-and-risk.md) | Accepted | 2026-03-06 |
| **ADR-005** | [Operations & Quality](ADR-005-operations-and-quality.md) | Accepted | 2026-03-06 |
| **ADR-006** | [User Interface & Agents](ADR-006-ui-and-agents.md) | Accepted | 2026-03-06 |

---

## Architecture Overview

The consolidated ADRs organize decisions by domain:

| ADR | Domain | Covers |
|-----|--------|--------|
| **ADR-001** | Data & Storage | Market data sources, DuckDB+Parquet analytics, PostgreSQL operations, MinIO artifacts |
| **ADR-002** | Ingestion | Data pipeline, corporate action adjustment, dividend handling |
| **ADR-003** | Backtesting | VectorBT engine, 2LYNCH strategy, experiment registry, walk-forward testing |
| **ADR-004** | Risk | Paper trading engine, state machine, kill-switch, governance |
| **ADR-005** | Operations | Deployment, data quality, monitoring |
| **ADR-006** | UI & Agents | NiceGUI dashboard, phidata orchestration, chat assistant |

---

## Archive

Historical ADRs (superseded by consolidated ADRs above) are preserved in `archive/`:

| Original ADR | Consolidated Into | Archive File |
|--------------|------------------|-------------|
| ADR-001 | ADR-001 | `archive/ADR-001-market-data-sources-and-licensing.md` |
| ADR-002 | ADR-001 | `archive/ADR-002-storage-architecture-postgres-minio.md` |
| ADR-003 | ADR-003 | `archive/ADR-003-backtesting-engine-selection.md` |
| ADR-004 | ADR-006 | `archive/ADR-004-agent-orchestration-phidata-glm.md` |
| ADR-005 | ADR-002 | `archive/ADR-005-nse-ingestion-pipeline.md` (deprecated) |
| ADR-006 | ADR-002 | `archive/ADR-006-corporate-action-adjustment-engine.md` |
| ADR-007 | ADR-003 | `archive/ADR-007-strategy-spec-4pct-2lynch.md` |
| ADR-008 | ADR-003 | `archive/ADR-008-walk-forward-testing-framework.md` |
| ADR-009 (main) | ADR-001 | `archive/ADR-009-database-optimization-duckdb-parquet.md` |
| ADR-009 (exp) | ADR-003 | `archive/ADR-009-experiment-registry.md` |
| ADR-010 | ADR-004 | `archive/ADR-010-paper-trading-engine.md` |
| ADR-011 | ADR-006 | `archive/ADR-011-dashboard-architecture.md` |
| ADR-012 | ADR-005 | `archive/ADR-012-deployment-and-ops.md` |
| ADR-013 | ADR-001 | `archive/ADR-013-db-schema-and-partitioning.md` |
| ADR-014 | ADR-001 | `archive/ADR-014-artifact-versioning-minio.md` |
| ADR-015 | ADR-004 | `archive/ADR-015-signal-trade-state-machine.md` |
| ADR-016 | ADR-004 | `archive/ADR-016-risk-governance-kill-switch.md` |
| ADR-017 | ADR-005 | `archive/ADR-017-data-quality-and-reconciliation.md` |
| ADR-018 | ADR-005 | `archive/ADR-018-monitoring-and-alerting.md` |
| ADR-019 | ADR-002 | `archive/ADR-019-dividend-adjustment-and-total-return.md` |
| ADR-020 | ADR-006 | `archive/ADR-020-interactive-chat-assistant.md` |
| ADR-021 | ADR-003 | `archive/ADR-021-custom-backtest-engine-selection.md` |

---

## Migration Notes

- **2026-03-06**: Consolidated 23 small ADRs into 6 comprehensive ADRs
- Original ADRs preserved in `archive/` for historical reference
- No functional changes - consolidation only
- All cross-references updated to point to new ADRs

---

## ADR Template

When creating a new ADR, use this template:

```markdown
# ADR-XXX: [Title]

**Status**: [Proposed | Accepted | Deprecated | Replaced]
**Date**: YYYY-MM-DD
**Replaces**: (if applicable)

## Context

[What is the situation that is requiring a decision?]

## Decision

[What are we doing and why?]

## Rationale

[Why did we make this decision? What alternatives did we consider?]

## Consequences

[What does this mean for the project? What changes?]

## Risks & Mitigations

[What could go wrong? How will we prevent it?]
```
