# Strategy Onboarding Guide

This guide explains how to add a new trading strategy to NSE Momentum Lab, from definition to production-ready backtesting.

---

## Overview

NSE Momentum Lab strategies are implemented using a **hybrid Python + declarative spec** model:

| Layer | Purpose | Example |
|-------|---------|---------|
| Declarative | Filters, thresholds, candidate conditions | SQL-based candidate query |
| Python | Complex logic, stateful exits, custom ranking | Entry resolver, exit policy |

---

## The 2LYNCH Filter Stack is the Golden Standard

**Before implementing any breakout or breakdown strategy, read this.**

The 2LYNCH filter stack (H, N, 2, Y, C, L — requiring 5 of 6) is the core edge of this system. All breakout/breakdown strategies in this repo **must** apply the same filter stack. The breakout threshold (4%, 2%, etc.) is a configurable parameter — the filters are non-negotiable.

This means:
- `thresholdbreakout` at `--breakout-threshold 0.04` must produce **identical results** to the canonical 4% breakout baseline. If they diverge, a filter has drifted — investigate.
- Any new breakout strategy you add should inherit the same 6-filter SQL from the existing queries.
- Do not substitute `ret_5d <= 0` for `filter_2`. The correct formula uses `ret_1d_lag1` and `ret_1d_lag2` computed **inline** with `LAG()` — not from `feat_daily`.

### filter_2 is the easy one to get wrong

```sql
-- CORRECT (2LYNCH standard): "not up 2 days in a row"
(ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2

-- WRONG: This is a 5-day momentum check, not the 2-day check
(ret_5d <= 0 OR ret_5d IS NULL) AS filter_2
```

`ret_1d_lag1` and `ret_1d_lag2` must be computed inline in the CTE:
```sql
(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
/ NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
```

And the candidate filter clause must include: `AND ret_1d_lag1 IS NOT NULL` (requires 4+ rows of history).

### For SHORT (breakdown) strategies, mirror each filter

| Filter | LONG | SHORT |
|--------|------|-------|
| H | `close_pos_in_range >= 0.70` | `close_pos_in_range <= 0.30` |
| N | narrow OR `prev_close < prev_open` | narrow OR `prev_close > prev_open` |
| 2 | `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0` | `ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0` |
| Y | `prior_breakouts_30d <= 2` | `prior_breakouts_30d <= 2` |
| C | `vol_dryup_ratio < 1.3` | `vol_dryup_ratio < 1.3` |
| L | 2/3: close>MA20, ret_5d>0, R²≥0.70 | 2/3: close<MA20, ret_5d<0, R²≥0.70 |

See the complete queries in `strategy_families.py` as the reference implementation.

---

---

## Strategy Definition Contract

Every strategy must implement the `StrategyDefinition` contract:

```python
from nse_momentum_lab.services.backtest.strategy_registry import StrategyDefinition
from nse_momentum_lab.services.backtest.engine import PositionSide

my_strategy = StrategyDefinition(
    # Identity
    name="MyStrategy",           # Display name
    version="1.0.0",             # Semantic version
    description="Brief description",
    family="my_family",          # Strategy family name

    # Direction
    direction=PositionSide.LONG,  # or SHORT

    # Optional components
    strategy_label=lambda year: f"MyStrategy_{year}",
    build_candidate_query=_build_my_candidate_query,
    default_params={...},
)
```

---

## Step-by-Step Implementation

### Step 1: Define the Candidate Query

The candidate query identifies potential trade opportunities.

**Location**: `src/nse_momentum_lab/services/backtest/strategy_families.py`

```python
def _build_my_strategy_candidate_query(
    params: BacktestParams,
    symbols: list[str],
    start: date,
    end: date,
) -> tuple[str, list[object]]:
    """
    Build SQL query for my strategy candidates.

    Returns:
        (sql_query, parameter_bindings)
    """
    symbols_placeholders = ",".join("?" for _ in symbols)

    query = f"""
        WITH base AS (
            SELECT
                symbol, date AS trading_date,
                open, high, low, close, volume,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                close * volume AS value_traded_inr
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        ),
        filtered AS (
            SELECT * FROM base
            WHERE close >= ?
              AND value_traded_inr >= ?
              AND volume >= ?
        )
        SELECT * FROM filtered
        ORDER BY trading_date, symbol
    """

    params_tuple = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(params.min_price),
        float(params.min_value_traded_inr),
        float(params.min_volume),
    ]

    return query, params_tuple
```

**Candidate Query Requirements**:
- Must return columns: `symbol`, `trading_date`, `open`, `high`, `low`, `close`, `volume`, `prev_close`
- Additional columns can be added for filtering (e.g., technical indicators)
- Use parameterized queries (not string formatting)
- Return results ordered by `trading_date, symbol`

---

### Step 2: Register the Strategy

Add to `_STRATEGY_REGISTRY` in `strategy_registry.py`:

```python
from nse_momentum_lab.services.backtest.strategy_families import _build_my_strategy_candidate_query

_STRATEGY_REGISTRY: dict[str, StrategyDefinition] = {
    # ... existing strategies ...

    "mystrategy": StrategyDefinition(
        name="MyStrategy",
        version="1.0.0",
        description="My custom momentum strategy",
        family="my_family",
        direction=PositionSide.LONG,
        strategy_label=lambda year: f"MyStrategy_{year}",
        build_candidate_query=_build_my_strategy_candidate_query,
        default_params={
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50_000,
            "my_custom_param": 0.05,
        },
    ),
}
```

---

### Step 3: Add Strategy-Specific Parameters

If your strategy needs custom parameters, extend `BacktestParams`:

```python
@dataclass
class BacktestParams:
    # ... existing parameters ...

    # MyStrategy-specific
    my_threshold: float = 0.05
    my_lookback: int = 20
```

Or access via `default_params` in the strategy definition.

---

### Step 4: Implement Entry Resolution (Optional)

For complex entry timing (e.g., intraday triggers), implement a custom entry resolver:

```python
from nse_momentum_lab.services.backtest.engine import EntryResolver

class MyStrategyEntryResolver(EntryResolver):
    def resolve_entry(
        self,
        candidate: dict,
        five_min_candles: list[dict],
        params: BacktestParams,
    ) -> EntryResolution | None:
        """
        Determine entry price and time from 5-minute data.

        Returns None if no valid entry found.
        """
        # Custom entry logic here
        for candle in five_min_candles:
            if self._is_trigger(candle, params):
                return EntryResolution(
                    price=candle["close"],
                    time=datetime.fromisoformat(candle["timestamp"]),
                    stop=self._calculate_stop(candidate, candle),
                )
        return None
```

---

### Step 5: Define Exit Policy (Optional)

For custom exit logic beyond the default stops:

```python
from nse_momentum_lab.services.backtest.engine import ExitPolicy

class MyStrategyExitPolicy(ExitPolicy):
    def should_exit(
        self,
        position: Position,
        current_bar: dict,
        params: BacktestParams,
    ) -> ExitSignal | None:
        """
        Check if position should be exited.

        Returns None if no exit signal.
        """
        # Custom exit logic
        if self._hit_custom_target(position, current_bar):
            return ExitSignal(
                reason="CUSTOM_TARGET",
                price=current_bar["close"],
            )
        return None
```

**Standard Exit Reasons** (use when applicable):
- `STOP_INITIAL` - Initial stop hit
- `STOP_TRAIL` - Trailing stop hit
- `TIME_EXIT` - Time-based exit
- `GAP_STOP` - Gap through stop
- `ABNORMAL_PROFIT` - Abnormal gain (e.g., >20% gap)
- `TARGET_EXIT` - Profit target hit

---

### Step 6: Add Quality Thresholds

Define validation thresholds for your strategy in `validation.py`:

```python
STRATEGY_THRESHOLDS: dict[str, QualityThresholds] = {
    # ... existing ...

    "my_family": QualityThresholds(
        min_trades=50,          # Adjust based on expected signal frequency
        min_trades_per_year=10,
        max_max_drawdown_pct=50.0,
        max_annual_return_pct=400.0,
        max_holding_days=30,
    ),
}
```

---

## Step 7: Test Your Strategy

### Unit Test

```python
# tests/unit/test_my_strategy.py
import pytest
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy

def test_my_strategy_registered():
    strategy = resolve_strategy("mystrategy")
    assert strategy.name == "MyStrategy"
    assert strategy.direction == PositionSide.LONG

def test_my_strategy_candidate_query():
    from nse_momentum_lab.cli.backtest import BacktestParams

    params = BacktestParams(strategy="mystrategy")
    strategy = resolve_strategy("mystrategy")

    query, bindings = strategy.build_candidate_query(
        params,
        ["RELIANCE"],
        date(2024, 1, 1),
        date(2024, 1, 31),
    )

    assert query is not None
    assert len(bindings) > 0
```

### Integration Test

```python
# tests/integration/test_my_strategy_backtest.py
import pytest
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)

@pytest.mark.integration
def test_my_strategy_full_run():
    params = BacktestParams(
        strategy="mystrategy",
        start_year=2024,
        end_year=2024,
        universe_size=100,  # Small universe for testing
    )

    runner = DuckDBBacktestRunner()
    result = runner.run(params)

    assert result["total_trades"] > 0
    assert result["max_drawdown_pct"] < 80  # Sanity check
```

---

## Step 8: Run Full Backtest

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy mystrategy \
  --start-year 2015 \
  --end-year 2025 \
  --universe-size 500
```

---

## Strategy Onboarding Checklist

- [ ] Candidate query implemented in `strategy_families.py`
- [ ] Strategy registered in `strategy_registry.py`
- [ ] Default parameters defined
- [ ] Entry resolver implemented (if needed)
- [ ] Exit policy implemented (if needed)
- [ ] Quality thresholds defined in `validation.py`
- [ ] Unit tests written and passing
- [ ] Integration test runs successfully
- [ ] Full 10-year backtest completes
- [ ] Results pass quality gates
- [ ] Documentation updated (this section for custom strategies)

---

## Common Patterns

### Pattern 1: Configurable Threshold

```python
# In query
WHERE ((close - prev_close) / prev_close) >= ?

# In bindings
float(params.breakout_threshold),  # e.g., 0.04 for 4%
```

### Pattern 2: Multi-Condition Filter

```python
# In query
WHERE
    close >= prev_close * 1.04  -- 4% breakout
    AND volume >= AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) * 1.5  -- Volume surge
    AND close > MA_20  -- Above MA20
```

### Pattern 3: Time-Based Entry

```python
# First 60 minutes only
WHERE
    timestamp >= '09:15:00'
    AND timestamp <= '10:15:00'
```

### Pattern 4: Short Side Strategy

```python
StrategyDefinition(
    name="MyShortStrategy",
    direction=PositionSide.SHORT,  # Key difference
    # ... rest of config ...
)
```

For shorts, ensure:
- P&L calculation is inverted (engine handles this)
- Stop logic uses `high` instead of `low`
- Exit logic accounts for short circuit limits

---

## Troubleshooting

### "No trades generated"

1. Check candidate query returns rows:
   ```python
   # Test query directly
   query, bindings = strategy.build_candidate_query(...)
   df = db.con.execute(query, bindings).fetchdf()
   print(f"_candidates: {len(df)}")
   ```

2. Verify filters aren't too restrictive

3. Check date range has data

### "Unexpected P&L values"

1. Verify `direction` is set correctly (LONG vs SHORT)

2. Check entry/exit prices are reasonable

3. Review exit reasons for anomalies

4. For SHORT strategies: `pnl_pct` is direction-aware — a profitable SHORT trade has `exit_price < entry_price`, so the formula is `(entry - exit)/entry`. VectorBT's `win_rate` and `total_return` are always correct. The only risk of sign errors is in any hand-computed metrics — the backtest runner handles this automatically for `pnl_pct` and `profit_factor`.

### "Strategy not found"

1. Check strategy key in registry (lowercase normalization)

2. Verify no import errors in `strategy_families.py`

---

## Example: Complete Simple Strategy

```python
# strategy_families.py

def _build_rsi_oversold_candidate_query(
    params: BacktestParams,
    symbols: list[str],
    start: date,
    end: date,
) -> tuple[str, list[object]]:
    symbols_placeholders = ",".join("?" for _ in symbols)

    query = f"""
        WITH rsi AS (
            SELECT
                symbol, date AS trading_date,
                close, open, high, low, volume,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                -- Simplified RSI-like calculation
                (close - LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date), 0) AS change_14
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        )
        SELECT * FROM rsi
        WHERE change_14 <= -0.10  -- Price down 10% over 14 days
          AND close >= ?  -- Min price filter
        ORDER BY trading_date, symbol
    """

    params_tuple = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(params.min_price),
    ]

    return query, params_tuple
```

---

## Next Steps

After onboarding your strategy:

1. **Run sensitivity analysis** on key parameters
2. **Conduct walk-forward testing** to validate out-of-sample performance
3. **Add strategy-specific features** if using derived features
4. **Document strategy logic** in team wiki
5. **Set up monitoring** for production deployment
