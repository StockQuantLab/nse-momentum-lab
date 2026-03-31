# OHLC Repair Manifest

This file tracks known, date-scoped OHLC anomalies that are intentionally left
out of the live feature rebuild so they can be repaired in a separate, targeted
pass.

## 2024-06-25

- Scope: 217 symbols
- Pattern: only the `09:15` candle on `2024-06-25`
- Symptom: open price sits outside the candle high/low band
- Breakdown:
  - `O < L` for 162 symbols
  - `O > H` for 54 symbols
- Source assessment: Kite API data artifact for that specific trading date
- Status: deferred for targeted repair
- Next action: create a date-specific repair script or exclusion pass after the
  current intraday rebuild completes

Notes:
- This is distinct from the legacy timestamp alignment issue.
- Do not fold this into the normal `2025-2026` feature rebuild path.
