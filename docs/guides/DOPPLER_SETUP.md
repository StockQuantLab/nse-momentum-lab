# Doppler setup (local-only, no secrets in repo)

This project is local-first, but secrets must never be committed to Git.

Principle:
- No `.env` files.
- Inject secrets at runtime using Doppler.

## Prereqs

- Install Doppler CLI (Windows): https://docs.doppler.com/docs/install-cli
- Login: `doppler login`

## One-time project bootstrap

From the repo root:

1) Create/select a Doppler project + config (e.g. `dev`).
2) Link the folder:

- `doppler setup`

This links the folder to a Doppler project/config.

Note: current Doppler CLI versions typically store this linkage in your user config (e.g. under `C:\Users\<you>\.doppler\...`) with the repo path as the scope, and may not create a repo-local `doppler.yaml` file.

## Required secrets (Phase 1)

You can seed secrets in **either** of these styles:

### Style A (preferred for the Python app)

- `DATABASE_URL`
  - Example (this repo default): `postgresql://<user>:<pass>@127.0.0.1:5434/<db>`
- `MINIO_ENDPOINT`
  - Example: `http://127.0.0.1:9003`  (Note: 9003 to avoid conflicts with other projects)
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`

### Style B (docker-compose style)

These are used by `docker-compose.yml` and are often already what you set first:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `POSTGRES_PORT` (default `5434`)

- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_PORT` (default `9003`)  # Changed from 9000 to avoid conflicts
- `MINIO_CONSOLE_PORT` (default `9004`)  # Changed from 9001 to avoid conflicts

The Python app can derive `DATABASE_URL` and MinIO credentials from these variables (see `src/nse_momentum_lab/config.py`).

Optional (only if/when you enable LLM routing):
- `GLM_API_KEY` (or whatever your LiteLLM/provider expects)

## Running locally

Inject secrets into processes without writing them to disk:

- Docker compose:
  - `doppler run -- docker compose up -d`
- Running a Python command:
  - `doppler run -- uv run python -m some_module`

## Notes

- Doppler is the single source of truth for secrets in this repo.
- Experiment registry should record *non-secret* config (dataset hash, params, code SHA, runtime versions) but never store raw secrets.
