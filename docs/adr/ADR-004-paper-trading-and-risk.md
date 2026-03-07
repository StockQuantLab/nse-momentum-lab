# ADR-004: Paper Trading & Risk Governance

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-010 (Paper Trading), ADR-015 (State Machine), ADR-016 (Risk Governance)

---

## Overview

This ADR defines the paper trading system and risk governance framework for NSE Momentum Lab.

---

## 1. Paper Trading Engine

### 1.1 Purpose

Validate strategies live without risking capital.

### 1.2 Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Simulated fills only** | No real orders submitted to broker |
| **Conservative modeling** | Slippage + fees modeled per liquidity bucket |
| **Alert-driven** | Notifications via dashboard/Telegram |
| **No auto-execution** | Manual review required for live trading |

### 1.3 Slippage Model

Liquidity-bucket based slippage (20-day rolling traded value):

| Bucket | Value Traded | Slippage |
|--------|--------------|----------|
| Large cap | >₹100M/day | 5 bps |
| Mid cap | ₹10-100M/day | 10 bps |
| Small cap | <₹10M/day | 20 bps |

### 1.4 Execution Assumptions

- **EOD fills** for all orders (no intraday execution simulation)
- **Limit orders** fill at next day's open if within range
- **Market orders** fill at closing price (day-end simulation)

---

## 2. Signal & Trade State Machine

### 2.1 State Transitions

```
NEW → QUALIFIED → ALERTED → ENTERED → MANAGED → EXITED → ARCHIVED
```

| State | Description |
|-------|-------------|
| **NEW** | Signal created from scan result |
| **QUALIFIED** | Basic validation passed |
| **ALERTED** | Notification sent, awaiting entry decision |
| **ENTERED** | Position opened, entry price recorded |
| **MANAGED** | Active position, monitoring exits |
| **EXITED** | Position closed, final P&L calculated |
| **ARCHIVED** | Historical record, no further state changes |

### 2.2 State Machine Rules

- Transitions must be auditable
- UI must respect valid transitions
- Each state change generates an event log entry

### 2.3 State Machine Implementation

| Table | Purpose |
|-------|---------|
| `signal` | Signal metadata, current state |
| `paper_order` | Order lifecycle |
| `paper_fill` | Fill records (price, qty, fees, slippage) |
| `paper_position` | Active positions |

---

## 3. Risk Governance

### 3.1 Portfolio Limits

| Limit | Value | Purpose |
|-------|-------|---------|
| **Max Drawdown Cap** | 20% | Stop trading if portfolio DD exceeds 20% |
| **Daily Loss Cap** | 3% | Stop trading if daily loss exceeds 3% |
| **Max Position Size** | 20% of capital | Single position limit |

### 3.2 Kill Switch

Automatic strategy pause on breach:

1. **Portfolio DD > 20%**: Immediate stop, review required
2. **Daily loss > 3%**: Pause for remainder of day
3. **Consecutive losers (5+)**: Manual review required

### 3.3 Resume Requirements

- Human approval required to resume trading after kill-switch
- Root cause analysis required for breaches
- Parameter adjustments may be required

---

## 4. Signal Generation

### 4.1 Signal Sources

Signals are generated from:
- Scan results (4% breakouts passing 2LYNCH filters)
- Manual entry (operator override)
- API endpoints (future)

### 4.2 Signal Metadata

Each signal includes:
- Symbol ID
- As-of date
- Strategy hash
- Initial stop price
- Entry mode (next-open, same-day-close)
- Metadata JSON (filters passed, gap %, etc.)

---

## 5. Order & Fill Simulation

### 5.1 Order Lifecycle

```
Signal → Order → Fill(s) → Position Update → Exit Signal → Order → Fill → Position Close
```

### 5.2 Order Types

| Type | Behavior |
|------|----------|
| **Market** | Fills at closing price (EOD simulation) |
| **Limit** | Fills next day if open is within range |
| **Stop-Limit** | Trailing stop, limit at stop price |

### 5.3 Fill Recording

Every fill records:
- Fill timestamp
- Fill price
- Quantity
- Fees
- Slippage (bps)

---

## 6. Position Management

### 6.1 Position States

| State | Description |
|-------|-------------|
| **PENDING** | Order submitted, waiting for fill |
| **OPEN** | Position opened, monitoring exits |
| **CLOSED** | Position exited, final P&L calculated |

### 6.2 Exit Triggers

Positions exit via:
- **Stop Loss**: Initial stop or trailing stop hit
- **Take Profit**: Target price reached
- **Time Stop**: Holding period exceeded
- **Manual**: Operator decision
- **Kill Switch**: Portfolio risk limit breach

---

## 7. Implementation

### 7.1 Key Files

| File | Purpose |
|------|---------|
| `services/paper/engine.py` | Paper trading engine |
| `services/backtest/duckdb_backtest_runner.py` | Signal generation |
| `db/models.py` | Signal, Order, Fill, Position models |

### 7.2 CLI Commands

```python
# View paper trading positions
from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import PaperPosition

# List active positions
positions = session.query(PaperPosition).filter_by_state("OPEN")
```

---

## 8. Monitoring & Alerts

### 8.1 Dashboard Integration

NiceGUI dashboard provides:
- **Paper Ledger page**: All positions, P&L summary
- **Daily Summary**: Today's signals, fills, P&L
- **Data Quality**: Signal quality metrics

### 8.2 Alert Channels

- **Dashboard**: Real-time updates
- **Telegram** (future): Push notifications for signals/exits
- **Email** (future): Daily summaries

---

## 9. Risk Metrics

### 9.1 Tracked Metrics

| Metric | Description |
|--------|-------------|
| **Win Rate** | % of profitable trades |
| **Profit Factor** | Gross profit / gross loss |
| **Calmar Ratio** | Annual return / max drawdown |
| **R-Multiple** | P&L / initial risk (per trade) |
| **Max Drawdown** | Largest peak-to-trough decline |

### 9.2 Reporting

Daily reports include:
- New signals generated
- Positions entered/exited
- Daily P&L
- Risk limit status
- Alerts triggered

---

## 10. Consequences

### Positive
- ✅ Risk-free validation of strategies
- ✅ Clear audit trail of all decisions
- ✅ Automatic risk limits prevent catastrophic losses

### Trade-offs
- ⚠️ EOD execution simulation may not match intraday reality
- ⚠️ Slippage estimates are approximations

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Paper trading overestimates performance | Conservative slippage, realistic fills |
| Gap risk not captured | Model worst-case scenarios |
| Missing intraday opportunities | Acceptable for validation phase |

---

## 11. Related Documents

- **ADR-003**: Backtesting System
- **ADR-005**: Operations & Monitoring
- `services/paper/engine.py`: Implementation

---

*This ADR consolidates and supersedes: ADR-010, ADR-015, ADR-016*
