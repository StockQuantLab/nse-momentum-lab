# AI Agents (phidata) — Design + Operator Guide

This repo uses **phidata** to orchestrate repeatable research workflows and provide a local “research assistant” chat interface.

Current operator-facing workflows that agents should understand:
- Kite token exchange and ingestion now live under installable CLI entrypoints: `nseml-kite-token` and `nseml-kite-ingest`
- Paper trading is session-based and gated by completed walk-forward checks
- The NiceGUI `/paper_ledger` page is the operator view for walk-forward results, replay sessions, live sessions, watchlist state, and recent activity

Important constraint:
- **LLMs never compute price series, indicators, or trades.**
- All trading math is deterministic Python code and is fully auditable.

## What “agents” mean in this project

Agents are *operators* and *analysts* around a deterministic pipeline:

- Operators: decide what job to run next, retry failures, summarize logs, alert on anomalies.
- Analysts: explain scan/backtest outputs, compare strategy variants, summarize walk-forward results.

Agents do **not**:
- invent fills
- change historical prices
- override the state machine
- place real broker orders (Phase 1)

## Do we need a chatbot?

Yes — recommended.

Two interaction modes are supported:

1) **Background agents** (scheduled)
- Run nightly pipeline tasks (ingest → validate → adjust → features → scan → backtest → report).
- Emit structured logs + metrics + DB rows.

2) **Interactive chatbot** (human-in-the-loop)
- A local chat UI used to:
  - ask questions (“show top candidates for date X and why”)
  - compare results (“open vs close sibling metrics”)
  - request safe actions (“rerun scan for date X”, “generate report”)
- The chatbot never directly mutates the market dataset; it can only enqueue jobs or write non-sensitive notes.

Phase 1 implementation recommendation:
- Provide the chatbot in **NiceGUI** as a dedicated page (migrated from Streamlit 2026-03-01).
- Also provide a CLI entrypoint (useful for automation): `uv run nse-agent -q "..."`.

## Agent roster (Phase 1)

These are logical roles; they can be implemented as separate phidata agents or as one “router agent” with tools.

- **Ops Agent**
  - Checks job status tables, quarantines, missing files.
  - Summarizes failures and proposes reruns.

- **Scan Analyst Agent**
  - Explains why symbols passed/failed (reads `reason_json`).
  - Generates candidate digests (no price math).

- **Backtest Analyst Agent**
  - Compares experiment runs and sibling variants (open vs close).
  - Summarizes walk-forward stability, regime behavior, and drawdown clusters.

- **Risk/Monitor Agent**
  - Watches paper-trade ledger and risk governance state.
  - Alerts when kill-switch triggers or drift metrics degrade.

## Tools the agents are allowed to use

Agents should be implemented with a small, explicit toolset:

Read-only tools:
- Query Postgres for scans/experiments/paper ledger
- Fetch artifacts from MinIO (equity curves, trades)
- Read structured logs and metrics

Write tools (restricted):
- Enqueue a pipeline job (create a `job_run` row)
- Acknowledge alerts
- Add an “analysis note” to an experiment run

Hard rule:
- Any tool that could change datasets (e.g., rewriting raw OHLCV) must be admin-only and never invoked by the chatbot.

## Memory and provenance

- Agent conversation memory (if enabled) is stored in Postgres.
- Every agent response should cite:
  - `scan_run_id` / `exp_hash`
  - the date range and strategy variant

## Model routing

- Use an OpenAI-compatible endpoint (GLM via your provider), optionally behind LiteLLM.
- Keep the model in “analysis only” mode: summarization, anomaly detection, comparisons.

## Local execution (Doppler + uv)

Examples:

- Run a CLI query:
  - `doppler run -- uv run nse-agent -q "Show latest scan candidates"`

- Run the NiceGUI dashboard:
  - `doppler run -- uv run nseml-dashboard`

## UI Testing with Playwright CLI

Install playwright-cli and its skills:
```powershell
pip install playwright-cli
playwright-cli install --skills
```

Start the dashboard:
```powershell
doppler run -- uv run nseml-dashboard
```

Common commands:
```powershell
# Open browser to dashboard
playwright-cli open http://localhost:8501

# Navigate to a page
playwright-cli goto http://localhost:8501/scans

# Get element references for interaction
playwright-cli snapshot

# Click an element (use ref from snapshot YAML output)
playwright-cli click e22

# Take a screenshot
playwright-cli screenshot

# Check console errors
playwright-cli console

# Close browser
playwright-cli close
```

The dashboard runs on port 8501 by default.

**Available pages:**
- `/` - Home
- `/backtest` - Backtest Results
- `/compare` - Compare Experiments
- `/strategy` - Strategy Analysis
- `/scans` - Scans
- `/data_quality` - Data Quality
- `/pipeline` - Run Pipeline
- `/paper_ledger` - Paper Ledger
- `/daily_summary` - Daily Summary
- `/market_monitor` - Market Monitor

**Known issues (non-critical):**
- Theme warnings about empty color values in sidebar config - cosmetic only

## phidata usage

phidata is fine for Phase 1 because:
- it provides agent/tool patterns without forcing a heavy framework
- it’s easy to keep deterministic logic outside the LLM

If you ever decide to change later:
- LangGraph / PydanticAI / “plain tools + FastAPI” are viable alternatives.

The key requirement is not the framework — it’s the **determinism boundary** and **auditable tool calls**.
