# Migration Strategy

## Phase 1 Approach: SQL Init Files + Volume Resets

For Phase 1 (early development), we use a lightweight approach:

- **SQL init scripts**: `db/init/*.sql` runs on first container start
- **Volume resets**: During iteration, drop volumes and restart:
  ```powershell
  docker compose down -v
  docker compose up -d
  ```

## When to Migrate to Alembic

Switch to Alembic migrations when:
- Schema stabilizes (after Phase 1)
- Multiple developers need independent schema changes
- Production deployment requires zero-downtime migrations

## Current Schema

Schema is defined in:
- **SQL**: `db/init/001_init.sql` (runs in Docker)
- **ORM**: `src/nse_momentum_lab/db/models.py` (SQLAlchemy models)

Both must be kept in sync manually during Phase 1.

## Adding New Tables/Columns

1. Edit `db/init/001_init.sql` to add DDL
2. Edit `src/nse_momentum_lab/db/models.py` to add corresponding ORM model
3. Test by resetting volumes: `docker compose down -v; docker compose up -d`

## Partition Management

Time-series tables (`md_ohlcv_raw`, `md_ohlcv_adj`, `feat_daily`) use monthly partitions.
During Phase 1, partitions are created via the DEFAULT partition - PostgreSQL auto-creates
child partitions as needed based on the date range.

For production, consider adding a partition management job or explicit partition creation.
