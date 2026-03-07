"""Compare multiple backtest experiments side-by-side.

Usage:
    uv run python scripts/compare_backtest_runs.py \
        --baseline 21d35d9b903b7921 \
        --exps 30min:c515777b45ac0bf4 45min:642f18cdf8c8e7f8 60min:0ba48f889c978516
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _pct(v: float | None) -> str:
    if v is None:
        return "    N/A"
    return f"{v:+8.2f}%"


def _fmt(v: float | None, decimals: int = 2) -> str:
    if v is None:
        return "    N/A"
    return f"{v:8.{decimals}f}"


def _int(v: int | None) -> str:
    if v is None:
        return "    N/A"
    return f"{v:8,}"


def fetch_summary(db, exp_id: str, label: str) -> dict:
    trades = db.con.execute(
        "SELECT * FROM bt_trade WHERE exp_id = ?", [exp_id]
    ).pl()
    yearly = db.con.execute(
        "SELECT * FROM bt_yearly_metric WHERE exp_id = ? ORDER BY year", [exp_id]
    ).pl()
    exp_row = db.con.execute(
        "SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]
    ).fetchdf()
    if exp_row.empty:
        raise SystemExit(f"Experiment {exp_id} not found in database.")
    exp = exp_row.iloc[0]

    total_trades = len(trades)
    if total_trades == 0:
        return {"label": label, "exp_id": exp_id, "total_trades": 0,
                "win_rate": 0.0, "annualised": 0.0, "total_ret": 0.0,
                "max_dd": 0.0, "profit_factor": 0.0, "avg_r": 0.0,
                "median_r": 0.0, "avg_hold": 0.0, "yearly": yearly,
                "worst": pl.DataFrame(), "exit_counts": pl.DataFrame()}

    wins   = trades.filter(pl.col("pnl_pct") > 0)
    losses = trades.filter(pl.col("pnl_pct") < 0)
    win_rate = len(wins) / total_trades * 100

    gain = wins["pnl_pct"].sum() if len(wins) else 0.0
    loss = abs(losses["pnl_pct"].sum()) if len(losses) else 0.0
    profit_factor = gain / loss if loss else 0.0

    years_active = yearly.filter(pl.col("trades") > 0)
    total_ret = years_active["return_pct"].sum() if len(years_active) else 0.0
    n_years   = len(years_active)
    annualised = total_ret / n_years if n_years else 0.0
    max_dd = yearly["max_dd_pct"].max() if len(yearly) else 0.0

    avg_r    = trades["pnl_r"].mean()    or 0.0
    median_r = trades["pnl_r"].median() or 0.0
    avg_hold = trades["holding_days"].mean() or 0.0

    worst = (trades.sort("pnl_pct").head(10)
             .select(["entry_date","symbol","entry_price","exit_price",
                      "pnl_pct","pnl_r","holding_days","exit_reason"]))
    exit_counts = (
        trades.group_by("exit_reason")
        .agg(pl.len().alias("n"),
             pl.col("pnl_pct").mean().alias("avg_pnl"),
             pl.col("pnl_r").mean().alias("avg_r"))
        .sort("n", descending=True)
    )

    return {
        "label": label, "exp_id": exp_id,
        "start_year": int(exp.get("start_year", 0)),
        "end_year":   int(exp.get("end_year",   0)),
        "total_trades": total_trades,
        "win_rate": win_rate,
        "annualised": annualised,
        "total_ret": total_ret,
        "max_dd": max_dd,
        "profit_factor": profit_factor,
        "avg_r": avg_r,
        "median_r": median_r,
        "avg_hold": avg_hold,
        "yearly": yearly,
        "worst": worst,
        "exit_counts": exit_counts,
    }


def print_multi_comparison(summaries: list[dict]) -> None:
    labels = [s["label"] for s in summaries]
    width = 100

    print("\n" + "=" * width)
    print("  MULTI-RUN BACKTEST COMPARISON  (2015–2025, 500 stocks, 5/6 filters)")
    for s in summaries:
        print(f"  [{s['label']:6s}]  exp={s['exp_id']}  trades={s['total_trades']:,}")
    print("=" * width)

    # ── KPI table ──────────────────────────────────────────────────────────
    col_w = 12
    hdr = f"  {'KPI':<30}" + "".join(f"{label:>{col_w}}" for label in labels)
    print(f"\n{hdr}")
    print("  " + "-" * (width - 2))

    def row(label: str, vals: list[str]) -> str:
        return f"  {label:<30}" + "".join(f"{v:>{col_w}}" for v in vals)

    kpis = [
        ("Total Trades",      [_int(s["total_trades"]) for s in summaries]),
        ("Win Rate",          [_pct(s["win_rate"]) for s in summaries]),
        ("Annualised Return", [_pct(s["annualised"]) for s in summaries]),
        ("Total Return (∑yr)",[_pct(s["total_ret"]) for s in summaries]),
        ("Max Drawdown",      [_pct(s["max_dd"]) for s in summaries]),
        ("Profit Factor",     [_fmt(s["profit_factor"]) for s in summaries]),
        ("Avg R (pnl_r)",     [_fmt(s["avg_r"], 3) for s in summaries]),
        ("Median R",          [_fmt(s["median_r"], 3) for s in summaries]),
        ("Avg Holding Days",  [_fmt(s["avg_hold"], 1) for s in summaries]),
    ]
    for label, vals in kpis:
        print(row(label, vals))

    # ── Year-by-year ───────────────────────────────────────────────────────
    print(f"\n  {'Year':<6}" + "".join(
        f"  {'trd':>4} {'ret%':>7} {'win%':>6}"[: col_w * 3]
        .ljust(col_w * 3 - 2)
        for _ in summaries
    ))

    # Build header row
    yr_hdr = f"\n  {'Year':<6}"
    for s in summaries:
        yr_hdr += f"  [{s['label']:>5}] {'trd':>5} {'ret%':>8} {'win%':>6}"
    print(yr_hdr)
    print("  " + "-" * (width - 2))

    all_yearly = {s["label"]: {int(r["year"]): r
                                for r in s["yearly"].iter_rows(named=True)}
                  for s in summaries}
    years = sorted({yr for yd in all_yearly.values() for yr in yd})
    for yr in years:
        line = f"  {yr:<6}"
        for s in summaries:
            yd = all_yearly[s["label"]]
            r  = yd.get(yr, {})
            tr = r.get("trades", 0) or 0
            rp = r.get("return_pct", 0.0) or 0.0
            wr = r.get("win_rate_pct", 0.0) or 0.0
            line += f"  [{s['label']:>5}] {tr:>5,}  {rp:>+7.2f}%  {wr:>5.1f}%"
        print(line)

    # ── Worst-trade comparison ─────────────────────────────────────────────
    for s in summaries:
        print(f"\n  WORST 10 TRADES — {s['label']}  (exp {s['exp_id']})")
        print(f"  {'Date':<12} {'Symbol':<14} {'Entry':>8} {'Exit':>8} "
              f"{'PnL%':>8} {'R':>6} {'Days':>5}  Exit Reason")
        for r in s["worst"].iter_rows(named=True):
            print(f"  {r['entry_date']!s:<12} {r['symbol']!s:<14} "
                  f"{r['entry_price']:>8.2f} {r['exit_price']:>8.2f} "
                  f"{r['pnl_pct']:>7.2f}% {r['pnl_r']:>5.2f}R {r['holding_days']:>5}"
                  f"  {r['exit_reason']}")

    # ── Exit reason comparison ─────────────────────────────────────────────
    print("\n  EXIT REASON BREAKDOWN")
    all_reasons = sorted({
        r["exit_reason"]
        for s in summaries
        for r in s["exit_counts"].iter_rows(named=True)
    })
    hdr2 = f"  {'Reason':<26}"
    for s in summaries:
        hdr2 += f"  [{s['label']:>5}] {'n':>5} {'avg%':>7}"
    print(hdr2)
    print("  " + "-" * (width - 2))

    for reason in all_reasons:
        line = f"  {reason!s:<26}"
        for s in summaries:
            ec = {r["exit_reason"]: r for r in s["exit_counts"].iter_rows(named=True)}
            r = ec.get(reason, {})
            n   = r.get("n", 0) or 0
            avg = r.get("avg_pnl", 0.0) or 0.0
            line += f"  [{s['label']:>5}] {n:>5,}  {avg:>+6.2f}%"
        print(line)

    print("\n" + "=" * width)
    print("  RECOMMENDATION GUIDE")
    print("  • More trades = better compounding, but lower quality per trade")
    print("  • Higher win rate + profit factor = better strategy robustness")
    print("  • Max Drawdown is the critical risk metric for live trading")
    print("  • Optimal: maximise (Annualised Return / Max Drawdown) = Calmar Ratio")
    calmar = [(s["label"], s["annualised"] / s["max_dd"] if s["max_dd"] else 0,
               s["annualised"], s["max_dd"], s["total_trades"], s["win_rate"])
              for s in summaries]
    calmar.sort(key=lambda x: -x[1])
    print(f"\n  {'Label':<8} {'Calmar':>8} {'Ann Ret':>10} {'Max DD':>9} {'Trades':>8} {'Win%':>7}")
    for label, calmar_r, ann, dd, tr, wr in calmar:
        print(f"  {label:<8} {calmar_r:>8.2f}  {ann:>+8.2f}%  {dd:>+7.2f}%  {tr:>8,}  {wr:>6.1f}%")
    best = calmar[0][0]
    print(f"\n  Best Calmar Ratio: [{best}]")
    print("=" * width)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=str, required=True,
                        help="Baseline experiment ID (label auto-set to 'baseline')")
    parser.add_argument("--exps", type=str, nargs="+",
                        help="label:exp_id pairs, e.g. 30min:c515... 45min:642f... 60min:0ba4...")
    args = parser.parse_args()

    db = get_market_db()

    summaries = []

    # Baseline
    summaries.append(fetch_summary(db, args.baseline, "OLD"))

    # Additional experiments
    if args.exps:
        for item in args.exps:
            if ":" in item:
                label, exp_id = item.split(":", 1)
            else:
                label = item[:8]
                exp_id = item
            summaries.append(fetch_summary(db, exp_id, label))

    print_multi_comparison(summaries)


if __name__ == "__main__":
    main()
