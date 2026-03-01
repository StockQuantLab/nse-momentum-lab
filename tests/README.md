# Tests

This repo keeps all tests under `tests/` (pytest).

## Layout

- `tests/test_*.py`: high-signal smoke tests
- `tests/unit/`: pure unit tests (no Postgres/MinIO; deterministic; fast)
- `tests/integration/`: tests that need external services (Postgres/MinIO via Docker Compose)

## Conventions

- Prefer testing importable code from `src/nse_momentum_lab/` (never import from `apps.*`).
- Unit tests should avoid network and wall-clock time.
- Integration tests should be explicitly marked (optional future: `@pytest.mark.integration`).
