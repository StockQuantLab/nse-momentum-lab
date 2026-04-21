import json

import duckdb

con = duckdb.connect("data/backtest.duckdb", read_only=True)

rows = con.execute(
    """
    SELECT
        count(*) FILTER (WHERE pnl_pct > -0.20) as normal,
        count(*) FILTER (WHERE pnl_pct BETWEEN -1.0 AND -0.20) as large_loss,
        count(*) FILTER (WHERE pnl_pct < -1.0) as extreme_loss,
        round(median(pnl_pct)*100,2) as median_pct
    FROM bt_trade WHERE exp_id='cb4c9b7abc289048' AND exit_reason='STOP_INITIAL'
    """
).fetchone()
print(f"STOP_INITIAL dist: normal={rows[0]} large={rows[1]} extreme={rows[2]} median={rows[3]}%")

print("\nExtreme outliers (pnl < -50%, STOP_INITIAL):")
rows2 = con.execute(
    """
    SELECT symbol, entry_date, round(entry_price,2), round(exit_price,2), round(pnl_pct*100,2)
    FROM bt_trade WHERE exp_id='cb4c9b7abc289048' AND pnl_pct < -0.50 AND exit_reason='STOP_INITIAL'
    ORDER BY pnl_pct LIMIT 15
    """
).fetchall()
for r in rows2:
    print(f"  {r[0]:12} {r[1]} entry={r[2]} exit={r[3]} pnl={r[4]}%")

row = con.execute(
    "SELECT params_json FROM bt_experiment WHERE exp_id='cb4c9b7abc289048'"
).fetchone()
p = json.loads(row[0])
print("\nKey params:")
for k in [
    "breakout_legacy_h_carry_rule",
    "same_day_r_ladder",
    "same_day_r_ladder_start_r",
    "entry_cutoff_minutes",
    "orh_window_minutes",
]:
    print(f"  {k}: {p.get(k)}")

con.close()
