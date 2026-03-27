# ADR-005: Operations & Quality

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-012 (Deployment), ADR-017 (Data Quality), ADR-018 (Monitoring)

---

## Overview

This ADR defines the operational infrastructure, data quality framework, and monitoring/alerting systems for NSE Momentum Lab.

---

## 1. Deployment & Infrastructure

### 1.1 Docker Stack

| Service | Purpose | Port |
|---------|---------|------|
| **PostgreSQL** | Operational data storage | 5434 |
| **MinIO** | Artifact storage | API: 9003, Console: 9004 |
| **NiceGUI Dashboard** | Research UI | 8501 |
| **FastAPI** | Backend API | 8004 |

### 1.2 Startup Commands

```bash
# Start all services
doppler run -- docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f
```

### 1.3 Environment Management

- **No .env files** - Use Doppler for secrets injection
- **doppler run** wrapper for all commands
- **Versioned configs** - Record runtime versions in experiment metadata

---

## 2. Data Quality Framework

### 2.1 Quality Gates

Data must pass these checks before ingestion:

| Check | Validation | Action on Failure |
|-------|------------|-------------------|
| **Row Count** | Expected range for trading date | Quarantine day, alert |
| **OHLC Constraints** | low ≤ min(open,close) ≤ high | Flag suspect symbols |
| **Price Sanity** | All prices > 0 | Reject data |
| **Volume Sanity** | Volume ≥ 0 | Zero volume OK on holidays |
| **Continuity** | No gaps in symbol coverage | Flag gaps for review |

### 2.2 Anomaly Detection

Automatic detection of:

| Anomaly Type | Detection Method |
|--------------|------------------|
| **Price jumps** | ret_1d > ±20% without corporate action | Flag for review |
| **Volume spikes** | vol_dryup_ratio > 5.0 | Flag for review |
| **Stale data** | No update for 5+ trading days | Alert operator |

### 2.3 Quarantine Process

Failed data is quarantined:
- Flagged in data quality tables
- Excluded from backtests
- Manual review required before re-inclusion

---

## 3. Monitoring & Alerting

### 3.1 Metrics Collection

| Metric Type | Examples |
|-------------|----------|
| **Pipeline** | Ingestion duration, records processed, errors |
| **Backtest** | Run count, duration, errors, P&L metrics |
| **System** | CPU, memory, disk usage |
| **Business** | Daily trades, win rate, drawdown |

### 3.2 Alert Channels

| Channel | Use Case |
|---------|----------|
| **Dashboard** | Real-time status, KPIs |
| **Email** | Daily summaries, critical failures |
| **Telegram** | (Future) Immediate alerts |

### 3.3 Alert Rules

| Alert | Trigger | Action |
|-------|---------|--------|
| **Critical** | Ingestion failure | Immediate notification |
| **Warning** | Data quality issue | Flag for review |
| **Info** | Daily summary | Log only |

### 3.4 Alert Fatigue Prevention

- **Aggregation**: Batch alerts within time window
- **Threshold tuning**: Adjust sensitivity to reduce noise
- **Escalation**: Only escalate persistent issues

---

## 4. Operations Procedures

### 4.1 Daily Operations Checklist

```bash
# 1. Check data ingestion
doppler run -- uv run python -c "
from nse_momentum_lab.db.market_db import get_market_db
db = get_market_db()
print(db.get_latest_trading_date())
"

# 2. Run daily backtest (if scheduled)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest

# 3. Check dashboard
# Visit http://localhost:8501
```

### 4.2 Backup Strategy

| Data Type | Backup Frequency | Retention |
|-----------|------------------|------------|
| PostgreSQL | Daily | 30 days |
| MinIO Artifacts | Continuous | 90 days |
| DuckDB Files | On-demand | Forever (Parquet is source) |

### 4.3 Recovery Procedures

| Scenario | Recovery Action |
|----------|-----------------|
| **PostgreSQL failure** | Restore from latest backup |
| **MinIO failure** | Restart service, artifacts cached in Parquet |
| **Data corruption** | Restore from Zerodha source |

---

## 5. Quality Gates

### 5.1 Pre-Commit Checks

Run before pushing code:

```bash
# Linting
uv run ruff check .

# Type checking
uv run mypy src/ --ignore-missing-imports

# Tests
uv run pytest tests/unit/ -q
```

### 5.2 CI/CD Pipeline

GitHub Actions validates:
- Dependencies (`uv sync --locked`)
- Linting (`ruff check .`)
- Tests (`pytest -q`)
- Type checking (`mypy src tests`)

### 5.3 Quality Status

**Current Status**: All gates passing ✅

| Gate | Status | Last Run |
|------|--------|----------|
| ruff | ✅ 0 errors | 2026-03-06 |
| mypy | ✅ 0 errors (88 files) | 2026-03-06 |
| pytest | ✅ unit suite passes | 2026-03-27 |

---

## 6. Logging & Observability

### 6.1 Log Levels

| Level | Usage |
|-------|-------|
| **DEBUG** | Detailed execution trace |
| **INFO** | Normal operations, progress |
| **WARNING** | Anomalies that don't stop execution |
| **ERROR** | Failures requiring intervention |
| **CRITICAL** | System-wide failures |

### 6.2 Structured Logging

Use structured logs for machine parsing:

```python
logger.info("backtest_completed",
    exp_hash=exp_hash,
    trades=len(trades),
    win_rate=win_rate,
    duration_seconds=duration)
```

---

## 7. Incident Response

### 7.1 Incident Categories

| Severity | Response Time | Escalation |
|----------|---------------|------------|
| **P1** | Immediate | Engineering lead |
| **P2** | 4 hours | Tech lead |
| **P3** | 1 day | Team lead |

### 7.2 Runbook

Common incidents and responses:

| Incident | Response |
|----------|----------|
| **DuckDB locked** | Kill dashboard, retry |
| **Out of memory** | Reduce universe size, restart |
| **Data gap** | Re-run ingestion for affected date |
| **Strange backtest results** | Verify parameters, check for bad data |

---

## 8. Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| Docker Compose | ✅ Complete | `docker-compose.yml` |
| Quality Validation | ✅ Complete | `services/ingest/quality.py` |
| Data Quality Dashboard | ✅ Complete | `apps/nicegui/pages/data_quality.py` |
| Monitoring | ⏳ Partial | Basic logging, alerts TBD |
| Backups | ⏳ Manual | Automated backups TBD |

---

## 9. Related Documents

- **ADR-001**: Data & Storage Architecture
- **ADR-004**: Paper Trading & Risk
- `guides/LOCAL_STACK.md`: Local development setup

---

*This ADR consolidates and supersedes: ADR-012, ADR-017, ADR-018*
