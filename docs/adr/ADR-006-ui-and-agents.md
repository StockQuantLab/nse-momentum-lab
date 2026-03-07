# ADR-006: User Interface & Agent Assistants

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-004 (Agents), ADR-011 (Dashboard), ADR-020 (Chat Assistant)

---

## Overview

This ADR defines the user interface architecture and AI agent assistant capabilities for NSE Momentum Lab.

---

## 1. Dashboard Architecture

### 1.1 Decision: NiceGUI

**NiceGUI** is the UI framework for the research dashboard.

**Rationale:**
- Persistent server-side state (no page re-runs)
- DuckDB-friendly (single connection, no threading issues)
- Fast startup (<1s vs Streamlit's 5-10s)
- Modern reactive UI with Vue.js frontend

**Migration History**: Streamlit → NiceGUI (2026-03-01)

### 1.2 Dashboard Pages

| Page | Path | Purpose |
|------|------|---------|
| **Home** | `/` | KPI overview, recent experiments |
| **Backtest Results** | `/backtest` | Experiment details, trade lists, equity curves |
| **Trade Analytics** | `/trade_analytics` | Trade-level analysis |
| **Compare Experiments** | `/compare` | Side-by-side experiment comparison |
| **Strategy Analysis** | `/strategy` | Strategy-level metrics |
| **Scans** | `/scans` | Scan results, candidate lists |
| **Paper Ledger** | `/paper_ledger` | Paper trading positions, P&L |
| **Data Quality** | `/data_quality` | Data validation status, quality metrics |
| **Pipeline** | `/pipeline` | Job status, run history |
| **Daily Summary** | `/daily_summary` | Daily rollups, what happened today |

### 1.3 Dashboard Access

```bash
# Start dashboard
doppler run -- uv run nseml-dashboard

# Access at
http://localhost:8501
```

### 1.4 FastAPI Backend

Read-only API for dashboard:

| Endpoint | Purpose |
|----------|---------|
| `/api/experiments` | List experiments, get details |
| `/api/trades` | Get trade data for experiments |
| `/api/metrics` | Get experiment metrics |
| `/api/status` | System health check |

API runs on port 8004.

---

## 2. Agent Orchestration

### 2.1 Decision: phidata + GLM

**phidata** orchestrates agent workflows.
**GLM models** perform reasoning, summarization, anomaly analysis.

**Key Constraint**: **No price math inside LLM calls**.

### 2.2 Determinism Boundary

| Component | Location | Type |
|-----------|---------|------|
| **Price math** | Python (deterministic) | Computed values |
| **LLM tasks** | GLM (probabilistic) | Summarization, analysis |

### 2.3 Agent Storage

Agent state stored in PostgreSQL for auditability and persistence.

---

## 3. Interactive Chat Assistant

### 3.1 Capabilities

The chat assistant can:

| Capability | Description | Safety |
|------------|-------------|--------|
| **Ask questions** | Query results, metrics, logs | Read-only |
| **Compare experiments** | Side-by-side analysis | Read-only |
| **Summarize logs** | Explain failures, anomalies | Read-only |
| **Trigger safe actions** | Enqueue jobs, acknowledge alerts | Restricted |

### 3.2 Hard Rules

- ❌ No price math via chatbot
- ❌ No dataset mutation
- ❌ No strategy parameter changes without confirmation
- ✅ All actions reference run IDs (`exp_hash`, `scan_run_id`) for provenance

### 3.3 UI Integration

- **NiceGUI Chat Page**: Interactive chat interface
- **CLI Entry** (optional): Quick queries from command line

### 3.4 Tool Boundaries

Agent tools must be explicitly defined and audited:

| Tool | Category | Validation |
|------|----------|------------|
| Query experiments | Read-only | Returns run metadata |
| Get trades | Read-only | Returns trade data |
| Enqueue backtest | Restricted | Requires confirmation |
| Get system status | Read-only | Returns health/status |

---

## 4. Research Workflow Support

### 4.1 Daily Research Cycle

```
1. Ingest → 2. Scan → 3. Backtest → 4. Evaluate → 5. Deploy → 6. Monitor
```

### 4.2 Agent Automation

Agents can assist with:

| Task | Automation |
|------|------------|
| **Parameter sweeps** | Grid search across parameter space |
| **Data quality** | Detect anomalies, flag for review |
| **Report generation** | Daily summaries, failure analysis |
| **Health checks** | Monitor pipeline, alert on issues |

### 4.3 Determinism Enforcement

All trading logic must remain in Python:
- Signal generation: SQL/Python
- Backtesting: VectorBT/Python
- Position management: Python state machine
- Risk limits: Python validation

LLMs only:
- Summarize results
- Explain anomalies
- Suggest optimizations (not auto-applied)

---

## 5. Implementation

### 5.1 Key Files

| Component | File |
|-----------|------|
| **Dashboard** | `apps/nicegui/main.py` |
| **Pages** | `apps/nicegui/pages/` |
| **Agent Orchestration** | `agents/` (future) |
| **API** | `src/nse_momentum_lab/api/` |

### 5.2 Dashboard Development

| Page | File |
|------|------|
| Home | `pages/home.py` |
| Backtest | `pages/backtest_results.py` |
| Trade Analytics | `pages/trade_analytics.py` |
| Compare | `pages/compare_experiments.py` |
| Strategy | `pages/strategy_analysis.py` |
| Scans | `pages/scans.py` |
| Paper Ledger | `pages/paper_ledger.py` |
| Data Quality | `pages/data_quality.py` |
| Pipeline | `pages/pipeline.py` |
| Daily Summary | `pages/daily_summary.py` |

---

## 6. State Management

### 6.1 Dashboard State

NiceGUI maintains server-side state:
- Database connection (DuckDB + PostgreSQL)
- User session data
- Cached query results

### 6.2 State Management

```python
# apps/nicegui/state/db.py
class DBState:
    """Shared database state for dashboard"""

    @staticmethod
    def get_experiments():
        """Load experiments from PostgreSQL"""

    @staticmethod
    def get_backtest_trades(exp_hash):
        """Load trades for experiment from PostgreSQL"""
```

### 6.3 Reactive Updates

- Dashboard polls for updates
- Long-running operations show progress
- State changes trigger UI refreshes

---

## 7. User Experience

### 7.1 Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Fast loading** | <1s startup, optimized queries |
| **Responsive** | WebSocket updates for progress |
| **Intuitive** | Clear navigation, visual feedback |
| **Professional** | Enterprise-grade styling |

### 7.2 Performance Targets

| Metric | Target |
|--------|--------|
| Dashboard startup | <1s |
| Experiment load | <2s |
| Trade list pagination | <500ms |
| Chart render | <1s |

---

## 8. Future Enhancements

### 8.1 Potential Additions

| Feature | Status | Priority |
|---------|--------|----------|
| Telegram alerts | ⏳ Future | Medium |
| Email reports | ⏳ Future | Medium |
| Mobile support | ⏳ Future | Low |
| Multi-user support | ⏳ Future | Low |

### 8.2 When to Reconsider

| Trigger | Consideration |
|--------|----------------|
| NiceGUI becomes limiting | Evaluate alternatives (Taipy, custom) |
| Team expands | Multi-user support, role-based access |
| Cloud deployment | Port to cloud-native architecture |

---

## 9. Consequences

### Positive
- ✅ Fast, interactive research interface
- ✅ Natural language interface for operations
- ✅ Agent assistance for common tasks
- ✅ Clear separation of deterministic vs probabilistic operations

### Trade-offs
- ⚠️ Agent requires careful boundary definition
- ⚠️ Chat interface may be over-engineering for single user

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Agent makes trading decisions | Hard rule: no price math, tools only |
| State synchronization issues | Server-side state, no client-side complexity |
| Over-engineering | Start simple, add features as needed |

---

## 10. Related Documents

- **ADR-005**: Operations & Quality
- **docs/guides/DASHBOARD_GUIDE.md**: Dashboard usage
- `apps/nicegui/`: Implementation

---

*This ADR consolidates and supersedes: ADR-004, ADR-011, ADR-020*
