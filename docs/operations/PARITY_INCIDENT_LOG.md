# Parity Incident Log

Template for documenting live-vs-replay and live-vs-backtest divergences.
File a new entry for each confirmed parity gap.

---

## INC-001 — (title)

| Field | Value |
|-------|-------|
| Date found | YYYY-MM-DD |
| Session ID | |
| Trade date | YYYY-MM-DD |
| Strategy | |
| Direction | LONG / SHORT |
| Severity | HIGH / MEDIUM / LOW |

### Symptom

What was observed (e.g. "live engine closed at stop but replay held through").

### Root cause

Investigation result (e.g. "feed audit shows bar close 149.25 vs v_5min close 149.30").

### Reproduction steps

1. `nseml-paper replay --session-id SID --trade-date YYYY-MM-DD --pack-source feed_audit`
2. Compare fills/positions against live session output.

### Fix

Commit hash or PR that resolved the divergence, or "N/A — data gap".

### Prevention

What test, check, or process change prevents recurrence.

---

## INC-002 — (title)

(Repeat the template above for each incident.)
