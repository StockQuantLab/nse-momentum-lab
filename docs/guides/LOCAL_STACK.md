# Local stack: Postgres + MinIO (Docker Compose)

This repo runs locally and uses Doppler for secrets injection.

## Prereqs

- Docker Desktop
- Doppler CLI (see [docs/guides/DOPPLER_SETUP.md](docs/guides/DOPPLER_SETUP.md))

## Required Doppler variables

Set these in Doppler (names match `docker-compose.yml`):

Postgres:
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- Optional: `POSTGRES_PORT` (default 5434)
- Optional: `POSTGRES_IMAGE` (default `postgres:18-alpine`)

MinIO:
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- Optional: `MINIO_PORT` (default 9003) - Changed from 9000 to avoid conflicts
- Optional: `MINIO_CONSOLE_PORT` (default 9004) - Changed from 9001 to avoid conflicts
- Optional: `MINIO_IMAGE` (default `minio/minio:latest`)
- Optional: `MINIO_MC_IMAGE` (default `minio/mc:latest`)

## Start services

From repo root:

- `doppler run -- docker compose up -d`

This will:
- Start Postgres and initialize schema from `db/init/001_init.sql`
- Start MinIO and create buckets `market-data` and `artifacts`

## Check health

- Postgres:
  - `docker ps` should show `nseml-postgres` healthy
- MinIO:
  - API: http://127.0.0.1:9003
  - Console: http://127.0.0.1:9004

## Notes

- The DDL in `db/init/001_init.sql` is a starter bootstrap so coding can begin.
- Once services are implemented, move schema management to Alembic migrations.
- Partitioned time-series tables are created with DEFAULT partitions so inserts always work; a later admin job can create monthly partitions.
