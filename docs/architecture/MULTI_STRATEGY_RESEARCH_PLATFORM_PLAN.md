# `docs/architecture/MULTI_STRATEGY_RESEARCH_PLATFORM_PLAN.md`

## Summary

NSE Momentum Lab should evolve from a fast but mostly 2LYNCH-specific backtester into a general momentum research platform. The new architecture must support multiple long and short strategies over the same data lake and execution engine, including configurable threshold breakouts, configurable threshold breakdowns, episodic pivots, and future rule/event-driven strategies.

The design target is:

- **Data**: append-friendly, partitioned, versioned, and incrementally refreshable
- **Features**: reusable core features plus strategy-derived features
- **Strategies**: hybrid Python + declarative specs, registered by name/version
- **Execution**: strategy-agnostic runner with pluggable entry and exit policies
- **Research**: batch backtests, optimization, sensitivity, anchored walk-forward, rolling walk-forward
- **Operations**: Postgres lineage, MinIO artifacts, DuckDB compute, local cache only as a developer convenience

Chosen defaults:

- Strategy authoring model: **hybrid Python + DSL/spec**
- Direction support: **long and short from the first architecture pass**
- Storage contract: **Postgres = metadata/system of record, MinIO = canonical data/artifacts, DuckDB = compute/query engine**
- First supported strategy families:
  - `indian_2lynch`
  - `threshold_breakout`
  - `threshold_breakdown`
  - `episodic_pivot`

---

## Current-State Assessment

The current repo already has strong pieces that should be preserved:

- DuckDB reads local or MinIO-backed Parquet efficiently
- VectorBT execution is fast enough for multi-year, multi-symbol backtests
- Experiment lineage and artifact publishing exist
- Batch backtesting and checkpoint/resume exist
- Optimizer, walk-forward, and sensitivity modules exist in some form

The main limitations are architectural, not computational:

- Production runner is still hardcoded around `Indian2LYNCH`
- Candidate SQL is embedded directly in the runner and assumes 4% breakout logic
- Current `feat_daily` materialization includes 2LYNCH-specific assumptions
- Scan/rules layer is also 2LYNCH-oriented
- Ingestion/update path is still closer to “rebuild scripts” than a production incremental pipeline
- Walk-forward and optimization exist but are not unified under one generic research protocol
- Strategy direction, execution semantics, and exit reasons are still shaped by one breakout strategy family

This means the platform is fast today, but not yet reusable for broad strategy research.

---

## Architectural Principles

- Preserve the current speed profile. Generalization must not make the current 10-year runs materially slower.
- Separate **data**, **features**, **strategy definition**, **execution**, and **research protocol**.
- No strategy-specific assumptions in shared core layers unless they are explicitly declared as derived strategy features.
- All research outputs must be reproducible from:
  - dataset version
  - feature-set version
  - strategy version
  - engine config
  - code hash
- New market data should append cleanly and trigger only incremental rebuilds and selective reruns.
- UI/API/CLI should resolve strategies dynamically from a registry, not from hardcoded strategy-specific routes or labels.

---

## Target System Design

## 1. Data Platform

### 1.1 Canonical storage model

Use a three-layer data model:

- **Bronze**: raw vendor payloads and raw event feeds in MinIO
- **Silver**: normalized Parquet datasets for market data and event data
- **Gold**: DuckDB materializations and strategy-ready feature sets

Canonical ownership:

- MinIO stores bronze and silver datasets and snapshot artifacts
- Postgres stores manifests, job state, experiment lineage, and run metadata
- DuckDB provides zero-copy or near-zero-copy analytical access over silver/gold layers
- Local `data/` remains a cache/dev mirror, not the authoritative source

### 1.2 Partitioning contract

Define partitions for incremental refresh:

- Daily OHLCV:
  - `silver/daily/symbol=<SYMBOL>/year=<YYYY>/part-*.parquet`
- 5-minute OHLCV:
  - `silver/5min/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-*.parquet`
- Events:
  - `silver/events/type=<EVENT_TYPE>/year=<YYYY>/month=<MM>/part-*.parquet`

Rules:

- Writes are append-or-replace at partition level only
- No full-dataset rewrite for small updates
- Each partition must be independently checksum-verifiable

### 1.3 Dataset manifests and freshness

Extend dataset lineage so every dataset has:

- dataset kind
- partition key
- dataset hash
- partition hash
- row count
- min/max trading date
- produced-at timestamp
- producing code hash
- source URI
- status (`READY`, `FAILED`, `STALE`, `SUPERSEDED`)

Expected behavior:

- Adding new 2026 data creates new silver partitions and new manifest rows
- Existing 2015-2025 partitions remain untouched unless repair is explicitly requested
- UI/API can answer:
  - what is the latest available market date
  - which partitions are stale or failed
  - which backtests used outdated datasets

---

## 2. Ingestion and Incremental Update Pipeline

### 2.1 Replace placeholder ingestion with real jobs

The current no-op ingestion worker must be replaced with a real pipeline that can:

- discover new raw files in bronze
- normalize to silver schema
- validate dates, trading sessions, duplicates, nulls, and symbol mapping
- publish partitioned Parquet
- update manifests in Postgres
- optionally mirror to local cache

### 2.2 Required job types

Implement these job types as first-class pipeline units:

- `raw_ingest_daily`
- `raw_ingest_5min`
- `raw_ingest_events`
- `silver_validate`
- `gold_materialize_features`
- `gold_refresh_strategy_views`
- `research_rerun_impacted`

Each job must persist:

- job id
- job kind
- inputs
- outputs
- partition scope
- status
- start/end timestamps
- error info
- code hash

### 2.3 Incremental rebuild behavior

When new data is added, the system must rebuild only:

- newly affected silver partitions
- gold feature partitions touched by those updates
- any dependent rolling windows that overlap the new partitions
- only the experiments or walk-forward folds explicitly selected for rerun

Example rule for daily features:

- If `feat_daily_core` has a 252-day rolling dependency and 2026-01 data arrives, rebuild:
  - 2026 partitions
  - any late-2025 overlap needed for rolling-window warmup
- Do not rebuild 2015-2024

Example rule for 5-minute trigger features:

- If 2026-03 5-minute data arrives, rebuild only March 2026 partitions for intraday trigger features and any configured short overlap window if required

### 2.4 Repair mode

Support a repair mode for data corrections:

- `repair partition`
- `repair symbol`
- `repair date range`
- `full rebuild`

The default must always be the smallest valid scope.

---

## 3. Feature Store Architecture

## 3.1 Replace single monolithic feature table with layered feature sets

Current `feat_daily` should be refactored into named feature families.

### 3.1.1 `feat_daily_core`

Shared by most strategies. Includes:

- returns (`ret_1d`, `ret_2d`, `ret_5d`, `ret_10d`, `ret_20d`, `ret_63d`, `ret_252d`)
- volatility (`atr_14`, `atr_20`, true range, realized vol)
- trend (`ma_10`, `ma_20`, `ma_50`, `ma_65`, `ma_200`, slope, regression diagnostics)
- liquidity (`vol_20`, `dollar_vol_20`, turnover proxies)
- gap features (`gap_open_vs_prev_close`, `gap_high_vs_prev_close`, `gap_low_vs_prev_close`)
- candle structure (`range_pct`, `close_pos_in_range`, body ratio, wick ratios)
- position in range (`range_percentile_63`, `range_percentile_252`)
- breakout/breakdown counters with parameter-free definitions where possible

### 3.1.2 `feat_intraday_core`

Shared by strategies that use 5-minute entry timing:

- opening range high/low
- first trigger time
- first break of prior high/low
- intraday volume percentile
- intraday range expansion
- first-hour high/low
- trigger-to-stop distance
- entry cutoff windows

### 3.1.3 `feat_event_core`

Required for episodic pivots and event-driven strategies:

- event type
- event timestamp/date
- event freshness
- earnings date
- earnings gap context
- post-event drift window markers
- event surprise or placeholder fields for future vendor enrichment

### 3.1.4 `feat_strategy_derived`

Strategy-specific derived views/tables built from core features:

- 2LYNCH-specific young breakout counters
- breakout threshold-specific counters
- episodic pivot event qualifiers
- any filters that are not universal enough to belong in core

## 3.2 Feature registry

Introduce a registry where each feature set declares:

- name
- version
- input datasets
- dependency feature sets
- required lookback
- build SQL or build function
- partition grain
- incremental refresh policy
- output schema

This registry is the source of truth for materialization order and incremental rebuild planning.

## 3.3 Feature storage policy

- Core features should be persisted when expensive and widely reused
- Cheap derived values may remain virtual DuckDB views if that improves iteration speed
- Strategy-specific features should never silently pollute core schemas

---

## 4. Strategy Framework

## 4.1 Strategy definition contract

Introduce a formal `StrategyDefinition` interface. Every strategy must declare:

- `name`
- `version`
- `family`
- `description`
- `direction_mode`
- `required_datasets`
- `required_feature_sets`
- `parameter_schema`
- `default_params`
- `candidate_generator`
- `entry_resolver`
- `exit_policy`
- `universe_selector`
- `ranking_policy` if candidates must be ranked
- `position_policy` if exposure or concurrency is strategy-specific

## 4.2 Hybrid authoring model

Use two layers:

- **Declarative spec** for:
  - filter clauses
  - thresholds
  - candidate conditions
  - feature dependencies
  - ranking rules
  - default universe constraints
- **Python class** for:
  - event-driven rules
  - custom ranking
  - complex entry timing
  - stateful exit rules
  - bespoke trade invalidation logic

This gives fast authoring for common strategies and full flexibility for complex ones.

## 4.3 First-class strategy families

### 4.3.1 `indian_2lynch`
- Preserve current behavior as a plugin, not as a system-wide assumption
- Keep current stop stack and intraday breakout entry logic
- Validate against today’s known baseline metrics within tolerance

### 4.3.2 `threshold_breakout`
- Configurable threshold, for example `0.02`, `0.03`, `0.04`
- Configurable reference:
  - prior close
  - prior high
  - multi-day high
  - opening range high
- Configurable entry timing:
  - open
  - first trigger touch
  - first 5-minute close above threshold
- Supports long direction by default, but interface remains symmetric

### 4.3.3 `threshold_breakdown`
- Mirror of breakout strategy with short-side semantics
- Thresholds are configurable
- Must support short-specific entry, stop, gap-through-stop, and trailing logic

### 4.3.4 `episodic_pivot`
- Depends on event datasets plus price/volume confirmation
- Must support:
  - event detection
  - event freshness window
  - pivot confirmation logic
  - optional gap filter
  - optional liquidity and relative-strength ranking

## 4.4 Strategy registry

Create a runtime registry so CLI, API, and UI resolve strategies dynamically. The registry must support:

- listing all installed strategies
- loading by name
- loading by name + version
- surfacing parameter schema and defaults
- surfacing required feature sets and datasets

---

## 5. Backtest Engine

## 5.1 Separate orchestration from strategy logic

Refactor the backtest runtime into explicit stages:

- `UniverseSelector`
- `CandidateGenerator`
- `EntryResolver`
- `ExecutionModel`
- `ExitPolicy`
- `ResultAggregator`
- `ArtifactPublisher`

The runner remains responsible for:

- loading strategy definition
- validating datasets/features
- scheduling yearly or partition-based runs
- checkpointing and resume
- persistence and lineage
- progress heartbeat reporting

The runner should no longer own embedded 2LYNCH-specific SQL or hardcoded strategy names.

## 5.2 Generic signal model

Replace current signal assumptions with a strategy-neutral signal object containing:

- `signal_id`
- `strategy_name`
- `strategy_version`
- `signal_date`
- `symbol`
- `direction`
- `candidate_rank`
- `entry_trigger_type`
- `entry_price`
- `entry_time`
- `initial_stop`
- `target_price` optional
- `signal_metadata_json`

## 5.3 Generic trade model

Trade records must support both long and short:

- `trade_id`
- `exp_id`
- `strategy_name`
- `direction`
- `entry_date`
- `entry_time`
- `entry_price`
- `exit_date`
- `exit_time`
- `exit_price`
- `qty`
- `gross_pnl`
- `net_pnl`
- `pnl_pct`
- `pnl_r`
- `mae_r`
- `mfe_r`
- `holding_days`
- `exit_reason`
- `trade_metadata_json`

## 5.4 Exit policy framework

Generalize exits into pluggable policies.

Required exit policy components:

- initial stop policy
- breakeven policy
- trailing policy
- time-stop policy
- profit-taking policy
- event invalidation policy
- gap-through-stop handling
- delisting or missing-data handling

Exit reasons should become generic across strategies:

- `STOP`
- `TRAIL_STOP`
- `BREAKEVEN_STOP`
- `TIME_EXIT`
- `TARGET_EXIT`
- `GAP_STOP`
- `EVENT_INVALIDATION`
- `RULE_EXIT`
- `DELISTING`
- `DATA_INVALIDATION`

Current 2LYNCH reasons can be mapped into this generic set plus optional detailed reason metadata.

## 5.5 Long/short support

Long/short readiness must be built into the core engine, not patched later.

Requirements:

- correct PnL sign handling
- short-side stop behavior
- short-side trail behavior
- short-side gap risk
- short borrow or borrow-cost field in engine config even if initially defaulted to zero
- long/short attribution in results

---

## 6. Research Protocol Layer

## 6.1 Unify current research modules

Current optimizer, sensitivity, walk-forward, and batch-run logic should be unified under one protocol framework.

Introduce protocol types:

- `single_run`
- `grid_search`
- `random_search`
- `walk_forward_anchored`
- `walk_forward_rolling`
- `sensitivity_oat`

## 6.2 Shared protocol contract

Each protocol run must persist:

- `protocol_name`
- `protocol_version`
- `strategy_name`
- `strategy_version`
- parameter search space or tested params
- engine config
- dataset hash
- feature-set hash
- code hash
- checkpoint state
- per-fold or per-batch metrics

## 6.3 Walk-forward requirements

Support both:

- **anchored walk-forward**
- **rolling walk-forward**

Required config:

- train window
- test window
- roll interval
- warmup policy
- objective metric
- constraints
- minimum trade count per fold
- optional re-optimization frequency

Each fold must persist:

- train range
- test range
- selected parameters
- in-sample metrics
- out-of-sample metrics
- trade counts
- stability flags

## 6.4 Optimization requirements

Support:

- deterministic grid search
- deterministic random search with seed
- max run limit
- resumable parameter batches
- multi-metric ranking

Ranking metrics should include:

- annualized return
- max drawdown
- Calmar
- Sharpe
- Sortino
- profit factor
- expectancy
- win rate
- fold stability

The first version can rank by one objective plus hard constraints, but the architecture must allow multi-objective scoring later.

## 6.5 Sensitivity analysis

Support one-at-a-time sweeps across:

- strategy params
- engine params
- universe filters
- slippage assumptions
- stop logic variants

Sensitivity outputs must include:

- base config
- changed parameter
- changed value
- metric deltas
- stability interpretation

---

## 7. Persistence, Lineage, and Artifacts

## 7.1 Postgres metadata model

Postgres should remain the system of record for:

- dataset manifests
- partition manifests
- materialization jobs
- strategy registry metadata
- backtest runs
- research runs
- fold results
- parameter test results
- artifact references

The schema should explicitly distinguish:

- dataset lineage
- feature lineage
- strategy lineage
- execution lineage

## 7.2 MinIO artifacts

Artifacts should include:

- run-level snapshots
- fold-level metrics
- trade exports
- equity curves
- parameter surfaces
- event diagnostics
- log bundles
- optional DuckDB snapshots

Artifact prefixes should be organized by:

- strategy
- protocol
- dataset hash
- experiment id
- timestamp

## 7.3 DuckDB responsibilities

DuckDB remains the compute/query layer for:

- direct Parquet querying
- core materializations
- strategy candidate SQL
- fast experiment summary reads

DuckDB should not be the only place where lineage lives. The system must remain reconstructable from Postgres + MinIO.

---

## 8. CLI, API, and UI

## 8.1 CLI

Introduce or refactor commands to support the generalized model:

- `nseml-data sync`
- `nseml-data validate`
- `nseml-features materialize`
- `nseml-strategy list`
- `nseml-backtest --strategy <name>`
- `nseml-research optimize --strategy <name>`
- `nseml-research walkforward --strategy <name>`
- `nseml-research sensitivity --strategy <name>`

CLI behavior requirements:

- strategy is explicit
- dataset version can be pinned or default to latest
- checkpoint/resume is supported
- progress heartbeats are emitted consistently

## 8.2 API

Expose generic endpoints for:

- dataset freshness
- manifests
- materialization status
- strategy catalog
- backtest runs
- research runs
- fold details
- artifact listing

The API must no longer assume one strategy family in endpoint naming or payload shape.

## 8.3 UI

The dashboard should evolve to show:

- strategy catalog and available parameter schemas
- dataset freshness and latest available dates
- feature materialization state
- experiment results across strategies
- walk-forward fold analysis
- optimization/sensitivity results
- artifact links and dataset lineage

The Home page should prominently show:

- latest daily date
- latest 5-minute date
- latest event date
- stale partitions
- pending feature rebuilds
- runs using outdated datasets

---

## 9. Implementation Phases

## Phase 1. Generalize the domain model
Deliverables:
- generic strategy registry
- generic signal/trade/result models
- removal of hardcoded strategy names from runner, scan worker, CLI, and UI labels
- 2LYNCH re-implemented as a strategy plugin

Acceptance:
- current 2LYNCH run still works through the new registry
- no user-facing regression for current backtest flows

## Phase 2. Build the real incremental data pipeline ✓ COMPLETE (2026-03-06)
Deliverables:
- real ingestion worker ✓ - `IngestionPipeline` in `services/ingest/pipeline.py`
- bronze/silver/gold contract ✓ - `DataLayer` enum, partition structure defined
- partition manifests ✓ - `PartitionManifest` model in `db/models.py`, `PartitionManifestManager` service
- incremental refresh planner ✓ - `IncrementalRefreshPlanner` with lookback windows, dependency cascading
- MinIO-first dataset publishing ✓ - `MinIOPublisher` with atomic uploads, ETag verification

New modules:
- `src/nse_momentum_lab/services/data_lake/` - Data lake services package
  - `partition_manager.py` - Partition discovery, manifest registration, stale marking
  - `refresh_planner.py` - Incremental refresh planning with feature set configs
  - `minio_publisher.py` - MinIO publishing, mirroring to local cache
- `src/nse_momentum_lab/services/ingest/pipeline.py` - Real ingestion pipeline
- `src/nse_momentum_lab/cli/data_pipeline.py` - CLI for data pipeline operations

New Postgres models:
- `PartitionManifest` - Tracks individual partitions with checksums, row counts, date ranges
- `MaterializationJob` - Tracks feature materialization with incremental state
- `IncrementalRefreshState` - Links upstream partitions to downstream feature dependencies

Acceptance:
- adding new data for 2026 does not force a full historical rebuild ✓
- manifests correctly report new partitions and freshness ✓
- CLI commands for partition management and refresh planning ✓

## Phase 3. Refactor the feature store ✓ COMPLETE (2026-03-06)
Deliverables:
- `feat_daily_core` ✓ - Core daily features: returns, volatility, trend, liquidity, gaps, candle structure
- `feat_intraday_core` ✓ - Intraday features: opening ranges, breakout times, FEE windows
- `feat_event_core` ✓ - Event features: earnings, corporate actions (placeholder structure)
- strategy-derived feature registry ✓ - `feat_2lynch_derived` with filter flags
- incremental materializer ✓ - `IncrementalFeatureMaterializer` with dependency resolution

Acceptance:
- 2LYNCH still works ✓ - Legacy `feat_daily` view maintained
- threshold breakout/breakdown can use the same core feature families without new monolithic tables ✓

## Phase 4. Generalize the execution engine
Deliverables:
- pluggable candidate generator
- pluggable entry resolver
- pluggable exit policy
- long/short-ready trade handling
- generic exit reasons

Acceptance:
- current 2LYNCH exits remain reproducible within tolerance
- short-side threshold breakdown strategy can execute end-to-end

## Phase 5. Add the first new strategy families
Deliverables:
- configurable threshold breakout
- configurable threshold breakdown
- episodic pivot
- shared UI/API/CLI support for all three

Acceptance:
- all strategies run through the same orchestration and persistence path

## Phase 6. Unify research protocols
Deliverables:
- unified protocol framework
- anchored walk-forward
- rolling walk-forward
- grid search
- random search
- sensitivity analysis
- fold-level checkpointing and persistence

Acceptance:
- protocol results are resumable, lineage-complete, and strategy-agnostic

## Phase 7. Harden for production research ✓ COMPLETE (2026-03-06)
Deliverables:
- performance benchmarks ✓ - `BacktestBenchmark` with runtime/memory tracking, `@benchmarked` decorator, DuckDB persistence, regression detection
- stale-run detection ✓ - `DatasetVersionTracker`, `is_run_stale()`, `list_stale_runs()`, dependency cascade via `FeatureDependencyGraph`
- research validation quality gates ✓ - `QualityThresholds` per strategy, `validate_backtest_result()` with 9 sanity checks, `validate_research_run()` for protocols
- documentation for data append and strategy onboarding ✓ - `docs/operations/DATA_APPEND_GUIDE.md`, `docs/development/STRATEGY_ONBOARDING.md`

Acceptance:
- the platform is usable for ongoing multi-strategy research without ad hoc scripts ✓

---

## Implementation Status (2026-03-07)

### Phase 1 - Generalize the domain model: 100% ✓ (2026-03-07)
- **What exists**: Strategy registry, CLI flags, DuckDB runner generalized, ScanWorker `strategy_name` wired into scan definition name/version lookup via `resolve_strategy()`
- **Architecture note**: Live scan routing (FeatureEngine/ScanRuleEngine) remains Indian2LYNCH-specific; DuckDB-SQL-based live scan for non-2LYNCH strategies is a Phase 1 enhancement tracked as backlog. Scan definition metadata now correctly reflects the active strategy.

### Phase 2 - Build the real incremental data pipeline: 100% ✓ (2026-03-06)
- **What exists**:
  - `PartitionManifest` Postgres model with checksum, row counts, date ranges, status tracking
  - `MaterializationJob` model for feature materialization with incremental state
  - `IncrementalRefreshState` model for upstream-downstream dependency links
  - `PartitionManifestManager` service for partition discovery, registration, stale marking
  - `IncrementalRefreshPlanner` with lookback windows and feature set configs
  - `MinIOPublisher` for atomic partition uploads and ETag verification
  - `IngestionPipeline` replacing no-op worker with real data ingestion
  - CLI `data_pipeline.py` for partition management and refresh operations
- **New modules**: `src/nse_momentum_lab/services/data_lake/` package with partition_manager.py, refresh_planner.py, minio_publisher.py
- **Partitioning**: symbol/year for daily, symbol/year/month for 5min, type/year/month for events
- **Feature set configs**: `feat_daily_core` (252-day lookback), `feat_intraday_core`, `feat_event_core`, `feat_2lynch_derived`

### Phase 3 - Refactor the feature store: 100% ✓ (2026-03-06)
- **What exists**: feat_daily_core, feat_intraday_core, feat_event_core, feat_2lynch_derived, feature registry, incremental materializer
- **New modules**: `src/nse_momentum_lab/features/` with registry.py, daily_core.py, intraday_core.py, event_core.py, strategy_derived.py, materializer.py
- **CLI**: `nseml-build-features` supports modular feature building (`--feature-set`, `--status`, `--list`)
- **Backward compatibility**: Legacy `feat_daily` view maintained over `feat_daily_core`

### Phase 4 - Generalize the execution engine: 100% ✓ (2026-03-07)
- **What exists**:
  - VectorBT engine with direction support (LONG/SHORT)
  - Pluggable exit policy framework (ExitPolicy base class, DefaultBreakoutExitPolicy)
  - `VectorBTEngine.__init__` accepts optional `exit_policy: ExitPolicy` — wired to `compute_initial_stop` for fallback stop calculation (zero behavior change: `atr=None` path returns same `0.96/1.04` fallbacks)
  - PositionSide enum (LONG/SHORT)
  - Generic exit reasons (STOP, TRAIL_STOP, TIME_EXIT, GAP_STOP, STOP_POST_DAY3, etc.)
  - Direction passed from strategy to VectorBT config
  - Exit logic handles both long and short positions (inverted stop/profit logic, post-day-3 tightening)
  - VectorBT uses short_entries/short_exits for SHORT (not direction= param, which VBT 0.28.x rejects)
  - Direction-aware pnl_pct: `(entry - exit)/entry` for SHORT, `(exit - entry)/entry` for LONG (2026-03-07)

### Phase 5 - Add the first new strategy families: 100% ✓ (2026-03-07)
- **What exists**:
  - `2LYNCHBreakout` (previously ThresholdBreakout) — LONG, configurable threshold, full 2LYNCH filter stack
  - `2LYNCHBreakdown` (previously ThresholdBreakdown) — SHORT, configurable threshold, mirrored 2LYNCH filters
  - `EpisodicPivot` — gap-based, LONG
  - filter_2 fixed to use `ret_1d_lag1/lag2` (matching 2LYNCH) — was incorrectly using `ret_5d` (fixed 2026-03-07)
  - All strategies end-to-end tested: 4% and 2% breakout/breakdown across 2015–2025 complete
  - CLI `--breakout-threshold`, `--min-value-traded`, `--min-volume` parameters
  - All strategies show direction in CLI
- **EpisodicPivot 10-year result** (exp `8f7387ee18478ecb`, 2026-03-07): 6 trades, 66.7% win rate, 3.0% total return, Calmar 0.67. Very low signal count — the 5% gap threshold combined with 60-min FEE window rarely aligns on NSE. Strategy needs lower `min_gap` (2–3%) or relaxed FEE window for research viability. Infrastructure confirmed working; this is a parameter finding.

### Phase 6 - Unify research protocols: 100% ✓ (2026-03-06)
- **What exists**:
  - Unified protocol framework: `services/backtest/protocols.py` with `ProtocolType` enum
  - Protocol types: `single_run`, `grid_search`, `random_search`, `walk_forward_anchored`, `walk_forward_rolling`, `sensitivity_oat`
  - `ProtocolConfig` - shared configuration with strategy resolution
  - `ProtocolResult` - lineage-complete results with fold-level checkpointing
  - `FoldResult` - individual fold metrics and params persistence
  - Strategy-agnostic `optimizer.py` with `--strategy` flag and protocol modes
  - Strategy-agnostic `sensitivity.py` with `--strategy` flag
  - Strategy-agnostic `walkforward.py` with anchored and rolling modes
  - Database persistence via `store_protocol_result()` method
- **CLI modes**: `grid`, `walkforward`, `walkforward-anchored`, `walkforward-rolling`, `random`, `sensitivity`
- **Acceptance**: Protocol results are resumable, lineage-complete, and strategy-agnostic ✓

### Phase 7 - Harden for production research: 100% ✓ (2026-03-06)
- **New modules**: `services/research/benchmarks.py`, `services/research/stale_detection.py`, `services/research/validation.py`
- **Performance benchmarking**: `BacktestBenchmark` class with timing, memory, throughput tracking; regression detection; baseline expectations per strategy
- **Stale-run detection**: `DatasetVersionTracker` for hash-based staleness; `is_run_stale()` for single-run checks; `list_stale_runs()` for batch detection; cascade detection via `FeatureDependencyGraph`
- **Quality gates**: `QualityThresholds` per strategy; `validate_backtest_result()` with 9+ sanity checks; `validate_research_run()` for protocol validation; `validate_performance_regressions()` for comparison
- **Documentation**: `docs/operations/DATA_APPEND_GUIDE.md` with incremental rebuild workflows; `docs/development/STRATEGY_ONBOARDING.md` with implementation checklist
- **Acceptance**: Platform ready for ongoing multi-strategy research without ad hoc scripts

---

## 10. Test and Validation Plan

Required test categories:

- regression tests for current 2LYNCH results
- strategy contract tests
- long/short execution tests
- incremental data refresh tests
- feature rebuild equivalence tests
- walk-forward fold determinism tests
- optimizer checkpoint/resume tests
- lineage completeness tests
- UI/API strategy registry tests

Performance tests must verify:

- 10-year, 500-symbol runs remain in single-digit minutes on the current machine
- adding one new year and refreshing affected features is substantially faster than full rebuild
- fold reruns are limited to impacted date windows when dataset changes are appended

Correctness tests must verify:

- identical overlapping outputs between full rebuild and incremental rebuild
- short-side PnL and stops are correct
- strategy-specific derived features do not contaminate core feature tables
- every experiment/fold records dataset hash, feature hash, strategy version, and code hash

---

## 11. Assumptions and Defaults

- DuckDB remains the analytical engine in this phase
- Postgres remains the authoritative metadata store
- MinIO remains the canonical data/artifact store
- Local `data/` remains a cache or developer mirror only
- Strategy authoring remains hybrid Python + DSL/spec
- Long and short support are first-class from the beginning
- Existing ad hoc scripts are migration aids and validation helpers, not long-term production interfaces
- `docs/architecture/` does not exist today and should be created during implementation together with this file
