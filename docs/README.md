# nse-momentum-lab Documentation

## Current Status

**Phase 1 Complete** ✅
- Local-first EOD pipeline with market data ingestion
- Corporate action adjustment (splits/bonus/rights)
- Feature computation + 4% breakout + 2LYNCH scan engine
- Backtest engine with experiment registry
- Paper trading ledger + risk governance
- Daily summary + failure analysis rollups
- FastAPI read APIs + NiceGUI dashboard
- Pre-commit hooks for linting and testing

## Data Source

**Primary Data Source**: Zerodha historical equity data (2015-2025)
- Downloaded from Jio Cloud repository
- Manually downloaded and ingested via vendor candle pipeline
- See [Zerodha Data Setup](guides/ZERODHA_DATA_SETUP.md) for details
- Data stored in Parquet format for fast analytics with DuckDB

## Start Here

### New Users
- **[Quick Start](guides/QUICK_START.md)** - Fast path to running the system
- **[README](../README.md)** - Project overview and installation
- **[Zerodha Data Setup](guides/ZERODHA_DATA_SETUP.md)** - Ingesting market data

### Technical Documentation
- **[Technical Design](TECHNICAL_DESIGN.md)** - Implementation details
- **[Architecture Decision Records](adr/ADR-INDEX.md)** - Design decisions
- **[Roadmap](ROADMAP.md)** - Phase 1/2/3 checklist

### Developer Resources
- **[Agents Runbook](../agents.md)** - Common issues + commands (repo root)
- **[Developer Notes](dev/AGENTS.md)** - AI agent design and architecture

### Guides
- **[Local Stack Setup](guides/LOCAL_STACK.md)** - Docker + Doppler
- **[Port Configuration](guides/PORT_CONFIGURATION.md)** - Port mappings
- **[Dashboard Architecture](adr/ADR-011-dashboard-architecture.md)** - NiceGUI dashboard design
- **[Viewing Results](guides/VIEWING_RESULTS.md)** - Where to find scan results
- **[Command Reference](reference/COMMANDS.md)** - All CLI commands

### Reference
- **[Command Reference](reference/COMMANDS.md)** - Complete command listing
- **[Project Overview](overview/HighLevelProjectOverview.md)** - High-level architecture

## Archive

Historical status documents and summaries are preserved in [archive/](archive/).

## Repo Layout

```
nse-momentum-lab/
├── src/nse_momentum_lab/     # Main package
│   ├── agents/                # AI agents (future)
│   ├── api/                   # FastAPI application
│   ├── cli/                   # Command-line interfaces
│   ├── db/                    # Database models + session
│   └── services/              # Pipeline workers
├── apps/                      # Entry points (NiceGUI dashboard, FastAPI)
├── db/init/                   # Database schema
├── docs/                      # This documentation
├── scripts/                   # Operational scripts
└── tests/                     # Test suite
```

## Document Index

### Architecture Decisions (ADRs)
- [ADR Index](adr/ADR-INDEX.md)
- [Market Data Sources](adr/ADR-001-market-data-sources-and-licensing.md)
- [Storage Architecture](adr/ADR-002-storage-architecture-postgres-minio.md)
- [Corporate Action Adjustment](adr/ADR-006-corporate-action-adjustment-engine.md)
- [Strategy Spec: 4% + 2LYNCH](adr/ADR-007-strategy-spec-4pct-2lynch.md)
- [DuckDB + Parquet Architecture](adr/ADR-009-database-optimization-duckdb-parquet.md)
- [Dashboard Architecture](adr/ADR-011-dashboard-architecture.md)

### Strategy Guides
- **[2LYNCH Strategy Guide](2LYNCH_STRATEGY_GUIDE.md)** - Complete strategy documentation
- **[2LYNCH Filters Summary](2LYNCH_FILTERS_SUMMARY.md)** - Filter reference

### Guides
- [Quick Start](guides/QUICK_START.md)
- [Zerodha Data Setup](guides/ZERODHA_DATA_SETUP.md)
- [Local Stack Setup](guides/LOCAL_STACK.md)
- [Doppler Setup](guides/DOPPLER_SETUP.md)
- [Port Configuration](guides/PORT_CONFIGURATION.md)
- [Dashboard Architecture](adr/ADR-011-dashboard-architecture.md)
- [AI Agent Integration](guides/AI_AGENT_INTEGRATION.md) (Future)

### Developer Docs
- [Agents Design](dev/AGENTS.md)
