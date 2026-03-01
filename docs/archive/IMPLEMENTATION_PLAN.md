# NSE Momentum Lab - Enterprise Implementation Plan (v2)

## 1. Goal

Ship a production-grade platform where:

- Market data is canonical in MinIO Parquet.
- DuckDB is the analytics/compute engine and local cache.
- Postgres is the operational metadata and governance source of truth.
- Backtests and scans are idempotent and reproducible.
- Dashboard UX is high-confidence, fast, and decision-ready.

## 2. Architecture Decisions (Locked)

1. Store large snapshots/artifacts in MinIO, not Postgres.
2. Store metadata, lineage, idempotency keys, and artifact URIs/checksums in Postgres.
3. Use deterministic run keys based on `(dataset_hash, params_hash, code_hash)`.
4. Use incremental recomputation only; avoid full reruns unless contract version changes.

## 3. Current Gaps Summary

1. Ingestion worker is effectively a stub and does not persist operational data.
2. Dataset manifest table exists but is not yet integrated into runtime flow.
3. DuckDB backtest flow writes results to DuckDB only; MinIO artifact export is missing.
4. CI does not validate end-to-end idempotency + artifact persistence.
5. Dashboard UX has limited filtering, inconsistent information hierarchy, and weak load/error states.
6. Migrations/schema alignment is partial and needs hardening.

## 4. Program Phases

## Phase P0 - Data Contracts + Idempotency Backbone

### P0.1 Data Contract Registry

- [ ] `D-001` Add `dataset_contract_version` and `query_version` tracking conventions.
- [ ] `D-002` Define canonical S3 prefixes:
  - `market-data/raw/...`
  - `market-data/curated/...`
  - `market-data/features/...`
  - `artifacts/experiments/{exp_hash}/...`
  - `artifacts/datasets/{dataset_hash}/...`
- [ ] `D-003` Document filename/partition standard for daily and 5min parquet.

Acceptance Criteria:

- Contract doc committed and referenced by ingestion, scan, and backtest code.
- All code paths use constants (no scattered hardcoded bucket/prefix strings).

### P0.2 Postgres Manifest Wiring

- [x] `D-010` Implement manifest repository/service for `dataset_manifest`.
- [x] `D-011` Upsert manifest during dataset discovery/materialization.
- [x] `D-012` Persist source URI, row count, min/max trading date, metadata JSON, code/params hash.
- [x] `D-013` Add lookup APIs for latest dataset by kind/hash.

Acceptance Criteria:

- Running pipeline/backtest writes manifest rows in Postgres every time dataset hash changes.
- Duplicate hash+params+code does not create duplicate records.

### P0.3 Idempotent Run Keys Everywhere

- [ ] `D-020` Standardize run key format in scan worker and backtest runner.
- [ ] `D-021` Ensure `run_daily_pipeline`, scans, and backtests use same dataset fingerprint semantics.
- [ ] `D-022` Add skip behavior telemetry in `job_run` and experiment records.

Acceptance Criteria:

- Re-running same dataset+params+code skips compute and returns existing run IDs.
- Changing dataset OR params OR code triggers new run.

## Phase P1 - Ingestion + Lake Reliability

### P1.1 Replace Stub Ingestion Path

- [ ] `I-001` Implement real ingestion repository/service (CSV/parquet -> normalized staging -> persisted outputs).
- [ ] `I-002` Integrate `DataQualityValidator` into ingestion acceptance gate.
- [ ] `I-003` Persist ingestion summary into `job_run.metrics_json`.
- [ ] `I-004` Support quarantine flow for bad rows with reproducible error reasons.

Acceptance Criteria:

- Pipeline stage 1 actually processes files and produces measurable outputs.
- Data quality failures are visible and auditable.

### P1.2 Canonical Data Publish

- [ ] `I-010` Publish curated Parquet partitions to MinIO.
- [ ] `I-011` Register published dataset hash into Postgres manifest.
- [ ] `I-012` Optionally generate DuckDB checkpoint snapshot for milestone builds and upload to MinIO.

Acceptance Criteria:

- New dataset appears in MinIO with deterministic path and checksum.
- Manifest contains URI+hash linkage.

## Phase P2 - DuckDB + MinIO Artifacts

### P2.1 Backtest Artifacts

- [x] `B-001` Export trades, yearly metrics, equity curve as Parquet/CSV artifacts to MinIO.
- [x] `B-002` Compute SHA256 for artifacts and store in `exp_artifact`.
- [x] `B-003` Save artifact URIs in experiment metadata.
- [ ] `B-004` Add retention/lifecycle class tagging for artifacts.

Acceptance Criteria:

- Every completed backtest has artifact rows in Postgres and files in MinIO.
- Dashboard can load from artifact URI without local file dependency.

### P2.2 DuckDB Snapshot Policy

- [x] `B-010` Add optional `--snapshot` mode to push DuckDB snapshot to MinIO on demand.
- [ ] `B-011` Register snapshot in dataset manifest metadata and artifact table.
- [ ] `B-012` Add restore helper command to materialize local DuckDB from latest snapshot.

Acceptance Criteria:

- Snapshots are optional and milestone-based, not per-run.
- Restore command rehydrates a working local environment.

## Phase P3 - Dashboard UX/IA Overhaul

### P3.1 Global UX Baseline

- [ ] `U-001` Establish dashboard design tokens and layout conventions in shared utils.
- [ ] `U-002` Ensure sidebar navigation consistency and remove mixed main-canvas navigation.
- [ ] `U-003` Define page structure pattern: Summary -> Filters -> Visuals -> Tables -> Actions.

Acceptance Criteria:

- All pages follow a consistent layout and spacing rhythm.
- Navigation behavior is predictable across all pages.

### P3.2 Backtest Results UX Upgrade (`apps/dashboard/pages/15_Backtest_Results.py`)

- [x] `U-010` Add explicit experiment context panel (strategy, date range, params, status).
- [x] `U-011` Add filter bar:
  - date range
  - symbol multi-select
  - exit reason
  - min/max pnl
- [x] `U-012` Ensure all charts and metrics react to active filters.
- [x] `U-013` Add paginated/limited table rendering for large result sets.
- [x] `U-014` Add download buttons for filtered and full datasets.
- [x] `U-015` Add robust load/error/empty states with retry actions.

Acceptance Criteria:

- Page stays responsive on large experiments.
- User can isolate subsets and get consistent chart/table numbers.
- Failure states are actionable, not silent.

### P3.3 Remaining Page UX Hardening

- [ ] `U-020` Scans page: better filter controls, trend visualization, status clarity.
- [ ] `U-021` Experiments page: search, compare flow, quick drill-through to artifacts.
- [ ] `U-022` Pipeline page: real execution state, progress timeline, failures with remediation hints.
- [ ] `U-023` Home page: clear IA and quick links by workflow.

Acceptance Criteria:

- Core user journeys are 2-3 clicks max.
- No page requires terminal context to interpret status.

### P3.4 Accessibility + Responsiveness

- [ ] `U-030` Keyboard navigation pass on all interactive controls.
- [ ] `U-031` Color contrast and non-color status indicators.
- [ ] `U-032` Mobile/tablet layout checks and overflow handling for wide tables.

Acceptance Criteria:

- Manual accessibility checklist passes.
- Mobile/tablet views remain usable for key workflows.

## Phase P4 - API + Orchestration

### P4.1 API Contracts

- [x] `A-001` Add dataset manifest endpoints (list/get latest by kind/hash).
- [x] `A-002` Add experiment artifact endpoints for dashboard retrieval.
- [ ] `A-003` Add filtered/paginated backtest trade endpoint.
- [ ] `A-004` Add compare experiments endpoint with aligned metrics.

Acceptance Criteria:

- Dashboard pages can use API for heavy reads where direct DuckDB access is not ideal.

### P4.2 Orchestration Reliability

- [ ] `A-010` Align `job_run` schema/code and enforce idempotency key behavior.
- [ ] `A-011` Add explicit retry policy for ingestion/scan/backtest tasks.
- [ ] `A-012` Add run cancellation/interrupt safety for long jobs.

Acceptance Criteria:

- Recoverable failures do not require manual DB cleanup.

## Phase P5 - Test and CI Gates

### P5.1 Unit + Integration Coverage

- [x] `T-001` Unit tests for manifest repository and idempotency key computation.
- [ ] `T-002` Integration test: dataset publish -> manifest write -> scan/backtest skip on rerun.
- [ ] `T-003` Integration test: backtest artifact upload + `exp_artifact` linkage.
- [ ] `T-004` UI smoke tests (Home + Backtest Results critical interactions).

Acceptance Criteria:

- Tests prove no duplicate compute for unchanged inputs.
- Artifact URIs are always resolvable post-run.

### P5.2 CI Workflow Expansion

- [ ] `T-010` Add integration lane with Postgres + MinIO services in CI.
- [ ] `T-011` Gate merges on idempotency and artifact persistence tests.
- [ ] `T-012` Add dashboard smoke test in CI.

Acceptance Criteria:

- CI fails on broken lineage/idempotency/UX regressions.

## Phase P6 - Ops, Security, Governance

### P6.1 Backup + Retention

- [ ] `O-001` Scheduled Postgres logical backups.
- [ ] `O-002` MinIO bucket lifecycle policy (retention/archival).
- [ ] `O-003` Optional snapshot replication for DuckDB checkpoint artifacts.

Acceptance Criteria:

- Restore drill documented and verified.

### P6.2 Security Baseline

- [ ] `O-010` Enforce API auth defaults for non-local environments.
- [ ] `O-011` Audit logging for pipeline triggers and destructive operations.
- [ ] `O-012` Ensure secrets only via Doppler; no local secret fallbacks.

Acceptance Criteria:

- Production mode cannot start with insecure defaults.

### P6.3 Observability

- [ ] `O-020` Add metrics for ingest rows, quality failures, scan throughput, backtest duration.
- [ ] `O-021` Add health endpoints and alerts for stale pipeline, failed jobs, missing datasets.

Acceptance Criteria:

- Operators can detect and diagnose failures quickly without SSH/log digging.

## 5. Execution Order (No Rework Path)

1. `P0` must complete before all other phases.
2. `P1` and `P2` can run in parallel after `P0`.
3. `P3` starts once `P2` APIs/artifacts are stable.
4. `P4` and `P5` run together after `P1/P2`.
5. `P6` begins once workload patterns are stable from prior phases.

## 6. Definition of Done

- [ ] No duplicate compute for unchanged dataset+params+code.
- [ ] Every production run has manifest lineage and artifact URIs.
- [ ] Dashboard provides fast filterable analysis with robust load/error UX.
- [ ] CI validates idempotency, artifacts, and dashboard smoke behavior.
- [ ] Backup/restore and security baselines are operational.

## 7. Immediate Next Sprint (Recommended)

1. `D-010` to `D-013` (manifest repository + runtime writes).
2. `B-001` to `B-003` (backtest artifact upload + `exp_artifact` linkage).
3. `U-010` to `U-015` (Backtest Results UX core uplift).
4. `T-002` + `T-003` (integration proofs for rerun prevention and artifacts).
