# Port Configuration Guide

## Overview

This document describes the port configuration for nse-momentum-lab and how to avoid conflicts with other projects running on the same machine.

## Current Port Usage Across All Projects

As of the latest check, these ports are in use by various projects:

| Project | Service | Host Port | Container Port | Exposure |
|---------|---------|-----------|----------------|----------|
| card-fraud | postgres | 5432 | 5432 | 0.0.0.0 (all interfaces) |
| strategy | postgres | 5433 | 5432 | 127.0.0.1 |
| **nse-momentum-lab** | **postgres** | **5434** | **5432** | **127.0.0.1** |
| card-fraud | redis | 6379 | 6379 | 0.0.0.0 |
| card-fraud | rule-management | 8000 | 8000 | 0.0.0.0 |
| card-fraud | transaction-management | 8002 | 8002 | 0.0.0.0 |
| card-fraud | rule-engine | 8081 | 8081 | 0.0.0.0 |
| card-fraud | redpanda-console | 8083 | 8080 | 0.0.0.0 |
| card-fraud | minio | 9000-9001 | 9000-9001 | 0.0.0.0 |
| card-fraud | intelligence-portal | 5173 | 5173 | 0.0.0.0 |
| card-fraud | redpanda | 9092, 9644 | 9092, 9644 | 0.0.0.0 |

## nse-momentum-lab Port Configuration

To avoid conflicts with the card-fraud project, we use the following ports:

| Service | Host Port | Container/Internal Port | URL |
|---------|-----------|-------------------------|-----|
| **Postgres** | **5434** | 5432 | `postgresql://user:pass@127.0.0.1:5434/db` |
| **MinIO API** | **9003** | 9000 | http://127.0.0.1:9003 |
| **MinIO Console** | **9004** | 9001 | http://127.0.0.1:9004 |
| **FastAPI** | **8004** | N/A (Python) | http://127.0.0.1:8004 |
| **NiceGUI Dashboard** | **8501** | N/A (Python) | http://localhost:8501 |

## Configuration Files

The following files were updated to use these ports:

1. **src/nse_momentum_lab/config.py** - Default port values
2. **src/nse_momentum_lab/cli/api.py** - FastAPI port (8004)
3. **docker-compose.yml** - MinIO port mappings (9003, 9004)
4. **apps/nicegui/** - NiceGUI dashboard (port 8501)

## Doppler Configuration

Set these environment variables in Doppler:

```bash
# Postgres
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=nseml
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5434

# MinIO
MINIO_ROOT_USER=your_minio_user
MINIO_ROOT_PASSWORD=your_minio_password
MINIO_HOST=127.0.0.1
MINIO_PORT=9003
MINIO_CONSOLE_PORT=9004
```

## Starting the Services

1. **Docker services** (Postgres + MinIO):
   ```bash
   doppler run -- docker compose up -d
   ```

2. **FastAPI**:
   ```bash
   doppler run -- uv run nseml-api
   ```
   Access at: http://127.0.0.1:8004
   API docs: http://127.0.0.1:8004/docs

3. **NiceGUI Dashboard**:
   ```bash
   doppler run -- uv run nseml-dashboard
   ```
   Access at: http://localhost:8501

## Port Conflicts Resolved

The following changes were made to avoid conflicts:

- **8000 → 8004**: FastAPI moved from 8000 to 8004 (card-fraud uses 8000)
- **9000 → 9003**: MinIO API moved from 9000 to 9003 (card-fraud uses 9000)
- **9001 → 9004**: MinIO Console moved from 9001 to 9004 (card-fraud uses 9001)

## Next Steps

To apply the docker-compose changes:

1. Stop the existing containers:
   ```bash
   docker compose down
   ```

2. Start with new port mappings:
   ```bash
   doppler run -- docker compose up -d
   ```

3. Verify MinIO is accessible:
   - API: http://127.0.0.1:9003
   - Console: http://127.0.0.1:9004

## Security Note

All nse-momentum-lab services are bound to `127.0.0.1` (localhost only) except where explicitly noted. This ensures they are not accessible from external networks.
