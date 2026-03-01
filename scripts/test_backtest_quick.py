#!/usr/bin/env python3
"""Quick backtest test with synthetic signals."""

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine


async def test_backtest():
    print("=" * 60)
    print("VECTORBT BACKTEST TEST - SYNTHETIC SIGNALS")
    print("=" * 60)

    config = VectorBTConfig()
    engine = VectorBTEngine(config)

    price_data = {}
    base_date = date(2024, 1, 1)

    for symbol_id in [1, 2, 3]:
        price_data[symbol_id] = {}
        for i in range(100):
            d = base_date + timedelta(days=i)
            base_price = 100 + i + (symbol_id * 10)
            price_data[symbol_id][d] = {
                "open": base_price + 1,
                "high": base_price + 5,
                "low": base_price - 3,
                "close": base_price + 2,
                "close_adj": base_price + 2,
                "open_adj": base_price + 1,
                "high_adj": base_price + 5,
                "low_adj": base_price - 3,
                "volume": 1000000 * (symbol_id + 1),
                "value_traded": 100000000 * (symbol_id + 1),
            }

    signals = []
    for symbol_id in [1, 2, 3]:
        entry_date = base_date + timedelta(days=30)
        initial_stop = price_data[symbol_id][entry_date]["close"] * 0.95
        signals.append((entry_date, symbol_id, f"SYM{symbol_id}", initial_stop, {}))

        entry_date2 = base_date + timedelta(days=60)
        initial_stop2 = price_data[symbol_id][entry_date2]["close"] * 0.95
        signals.append((entry_date2, symbol_id, f"SYM{symbol_id}", initial_stop2, {}))

    print(f"Created {len(signals)} synthetic signals")

    dollar_vol = {
        1: 100000000.0,
        2: 200000000.0,
        3: 300000000.0,
    }

    for entry_mode in ["close", "open"]:
        print(f"\n{'=' * 60}")
        print(f"Testing entry_mode={entry_mode}")
        print(f"{'=' * 60}")

        result = engine.run_backtest(
            strategy_name=f"TEST_4P_2LYNCH_{entry_mode}",
            entry_mode=entry_mode,
            signals=signals,
            price_data=price_data,
            dollar_vol=dollar_vol,
        )

        print("\nResults:")
        print(f"  Total Return: {result.total_return * 100:.2f}%")
        print(f"  Sharpe Ratio: {result.sharpe_ratio:.3f}")
        print(f"  Max Drawdown: {result.max_drawdown * 100:.2f}%")
        print(f"  Win Rate: {result.win_rate * 100:.2f}%")
        print(f"  Profit Factor: {result.profit_factor:.3f}")
        print(f"  Avg R: {result.avg_r:.3f}")
        print(f"  Total Trades: {len(result.trades)}")

        for i, trade in enumerate(result.trades[:10], 1):
            print(f"\n  Trade {i}: {trade.symbol}")
            print(f"    Entry: {trade.entry_date} @ ₹{trade.entry_price:.2f}")
            if trade.exit_date:
                print(f"    Exit:  {trade.exit_date} @ ₹{trade.exit_price:.2f}")
                print(f"    PnL: ₹{trade.pnl:.2f} | R: {trade.pnl_r:.2f}")
                print(f"    Slippage: {trade.slippage_bps:.1f} bps | Fees: ₹{trade.fees:.2f}")
                print(f"    Exit Reason: {trade.exit_reason.value if trade.exit_reason else 'N/A'}")
            else:
                print("    Status: OPEN")

    print(f"\n{'=' * 60}")
    print("BACKTEST TEST COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(test_backtest())
