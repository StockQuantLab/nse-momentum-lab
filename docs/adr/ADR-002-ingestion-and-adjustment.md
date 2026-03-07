# ADR-002: Ingestion & Adjustment

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-005 (NSE Ingestion), ADR-006 (Corporate Actions), ADR-019 (Dividends)

---

## Overview

This ADR defines the data ingestion pipeline and corporate action adjustment strategy for NSE Momentum Lab.

---

## 1. Data Ingestion

### 1.1 Data Sources

| Data Type | Source | Format | Method |
|-----------|--------|--------|--------|
| Historical OHLCV | Zerodha (Jio Cloud) | Parquet | Manual download |
| Corporate Actions | NSE Website | CSV | Manual download |
| Symbol Reference | NSE Website | CSV | Manual download |

**Note**: ADR-005 (NSE Ingestion Pipeline) was deprecated in favor of Zerodha data source.

### 1.2 Ingestion Process

```bash
# 1. Download Zerodha data from Jio Cloud
# 2. Convert to Parquet format (already done)
# 3. Store to data/parquet/daily/{SYMBOL}/all.parquet
```

Ingestion implementation: `src/nse_momentum_lab/services/ingest/`

### 1.3 Data Quality Checks

Before ingestion, data must pass quality gates:

- **Completeness**: Expected row count range for trading date
- **OHLC constraints**: `low <= min(open,close) <= max(open,close) <= high`
- **No negative prices**: All OHLC values > 0
- **Volume sanity**: Volume >= 0 (zero volume on holidays is acceptable)

Quality implementation: `src/nse_momentum_lab/services/ingest/quality.py`

---

## 2. Corporate Action Adjustment

### 2.1 What Requires Adjustment?

| Action Type | Adjustment Required |
|--------------|---------------------|
| **Splits** | Yes (multiplicative) |
| **Bonuses** | Yes (multiplicative) |
| **Rights Issues** | Yes (multiplicative) |
| **Dividends** | No (stored as events only in Phase 1) |

### 2.2 Adjustment Method

**Backward adjustment**: All historical prices are adjusted by a cumulative factor.

```
adj_factor[earliest] = 1.0
adj_ratio = (old_price_after_action) / (old_price_before_action)
adj_factor[current] = adj_factor[previous] × adj_ratio

adj_price = raw_price × adj_factor[current]
```

### 2.3 Adjustment Order

Events are applied in **chronological order** to ensure correct cumulative factors.

### 2.4 Implementation

Adjustment logic: `src/nse_momentum_lab/services/adjust/logic.py`

Key class:
```python
class CorpAction:
    symbol_id: int
    ex_date: date
    action_type: str  # SPLIT, BONUS, RIGHT, DIVIDEND
    ratio_num: float   # Numerator
    ratio_den: float   # Denominator
    cash_amount: float # For dividends
```

---

## 3. Dividend Handling (Phase 1)

### 3.1 Dividend Policy

**Phase 1 Decision**: Dividends are **NOT** priced into the adjusted series.

**Rationale**:
- Short holding period (3-5 days) minimizes dividend impact
- Dividend data quality issues can create larger distortions than the dividend effect itself
- Keeps assumptions explicit and results auditable

### 3.2 Storage

Dividends are stored as **events only** in the `ca_event` table:

```sql
ca_event(
    event_id,
    symbol_id,
    ex_date,
    action_type = 'DIVIDEND',
    cash_amount,
    currency = 'INR'
)
```

### 3.3 Future: Total Return Index (TRI)

Phase 2 may add TRI as an optional feature:
- Separate derived series/table
- Recorded in experiment metadata
- Does not change raw/adjusted price tables

---

## 4. Data Pipeline

### 4.1 Pipeline Components

```
Zerodha Parquet Files
    │
    ▼
Quality Validation
    │
    ├─▶ Pass → Load to DuckDB
    │       │
    │       ▼
    │   Feature Computation (feat_daily)
    │
    └─▶ Fail → Quarantine + Alert
```

### 4.2 Idempotency

- Use `(symbol_id, trading_date)` as primary key
- Upsert by primary key
- Raw file checksum tracked; re-run allowed if checksum differs

### 4.3 Incremental Updates

- New trading days append to existing Parquet files
- Feature table rebuilds only affected date ranges
- Adjustments trigger full-history recompute for affected symbol

---

## 5. Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| Data Ingestion | ✅ Complete | `src/nse_momentum_lab/services/ingest/` |
| Quality Validation | ✅ Complete | `src/nse_momentum_lab/services/ingest/quality.py` |
| Adjustment Engine | ✅ Complete | `src/nse_momentum_lab/services/adjust/logic.py` |
| Corporate Action Storage | ✅ Complete | PostgreSQL `ca_event` table |
| Dividend Event Storage | ✅ Complete | PostgreSQL `ca_event` table |
| TRI Computation | ⏳ Future | Optional Phase 2 feature |

---

## 6. Consequences

### Positive
- ✅ Adjusted series preserves continuity across corporate actions
- ✅ Quality gates prevent bad data from entering system
- ✅ Explicit dividend handling avoids silent distortions

### Trade-offs
- ⚠️ Full-history recompute required when new action discovered
- ⚠️ Manual download process for initial dataset

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Incorrect ratios causing spurious returns | Validation checks against reference prices |
| Missing corporate actions | Manual review, cross-reference with NSE |
| Dividend data quality | Explicit event-only storage; TRI optional later |

---

## 7. Related Documents

- **ADR-001**: Data & Storage Architecture
- **ADR-003**: Backtesting System
- **guides/ZERODHA_DATA_SETUP.md**: Data ingestion guide

---

*This ADR consolidates and supersedes: ADR-005, ADR-006, ADR-019*
