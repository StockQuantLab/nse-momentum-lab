#!/bin/sh
set -eu

HOST="http://minio:9000"

# Wait for MinIO to be reachable (avoid relying on healthcheck tools in MinIO image)
attempt=1
max_attempts=60
until mc alias set nseml "${HOST}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
	if [ "$attempt" -ge "$max_attempts" ]; then
		echo "MinIO not reachable after ${max_attempts} attempts: ${HOST}" >&2
		exit 1
	fi
	attempt=$((attempt + 1))
	sleep 1
done

# Buckets (idempotent)
mc mb -p nseml/market-data || true
mc mb -p nseml/artifacts || true

# Optional: basic lifecycle/policy can be added later

echo "MinIO buckets ensured: market-data, artifacts"
