# ADR-011: Dashboard Architecture

Status: Accepted (revised 2026-03-01)

## Context

Researchers need visibility into scans, trades, equity curves, degradation.

Original decision was FastAPI backend + Streamlit frontend. Streamlit was abandoned due to:
- Connection loss on every re-run (requires `@st.cache_resource` hacks)
- Threading bug: `@st.cache_data` runs callbacks concurrently, causing DuckDB NULL dereference
- Too slow for interactive what-if analysis

## Decision

NiceGUI (server-side reactive UI) with DuckDB direct queries.

- Entry point: `apps/nicegui/main.py` via `nseml-dashboard` CLI
- Persistent sidebar navigation with Material Design icons
- `@ui.refreshable` for in-page state changes (e.g. experiment selector)
- Plotly charts with consistent dark theme

## Consequences

Server-side state persists across interactions (no page re-runs).
Single DuckDB connection shared across all pages via `state/__init__.py`.
