# ADR-020: Interactive Chat Assistant (Local)

Status: Accepted

## Context

The system includes multiple moving parts (ingestion, adjustment, scans, backtests, registry, paper trading). Researchers benefit from a natural-language interface to:

- ask questions about results
- compare experiment variants
- summarize logs and failures
- trigger safe reruns and report generation

The project is local-first and should not introduce operational complexity or risk to deterministic computations.

## Decision

Implement a **local interactive chatbot** for research and operations support.

- UI: NiceGUI (migrated from Streamlit 2026-03-01)
- Optional: CLI entrypoint for quick queries
- Capabilities: read-only analysis + restricted safe actions (enqueue jobs, acknowledge alerts)
- Hard rule: no price math, no dataset mutation via chatbot

## Rationale

- Faster iteration and debugging for Phase 1.
- Keeps deterministic trading logic in Python while still leveraging LLMs for summarization and comparison.

## Consequences

- Agent toolset must be explicitly defined and audited.
- Responses should reference run IDs (`scan_run_id`, `exp_hash`) for provenance.

## Alternatives considered

- No chatbot: slower ops/debugging.
- Full web app chat: more boilerplate than NiceGUI for Phase 1.
