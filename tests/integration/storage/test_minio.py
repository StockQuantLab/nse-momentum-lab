"""Integration tests for MinIO storage.

Run with: doppler run -- uv run pytest tests/integration/storage -v
"""

from __future__ import annotations

import pytest

from nse_momentum_lab.services.ingest.minio import MinioArtifactStore

pytestmark = pytest.mark.integration


class TestMinIOConnection:
    def test_minio_connection(self, minio_store):
        """Test that MinIO connection works."""
        assert minio_store._client is not None

    def test_buckets_exist(self, minio_store):
        """Test that required buckets exist."""
        raw_exists = minio_store._client.bucket_exists(MinioArtifactStore.BUCKET_RAW)
        artifacts_exists = minio_store._client.bucket_exists(MinioArtifactStore.BUCKET_ARTIFACTS)

        assert raw_exists, f"Bucket {MinioArtifactStore.BUCKET_RAW} should exist"
        assert artifacts_exists, f"Bucket {MinioArtifactStore.BUCKET_ARTIFACTS} should exist"


class TestMinIOOperations:
    def test_uri_generation(self, minio_store):
        """Test URI generation."""
        uri = minio_store.get_uri("test-bucket", "test/path/file.txt")
        assert uri == "s3://test-bucket/test/path/file.txt"


class TestMinIOBucketCreation:
    def test_bucket_creation_on_init(self):
        """Test that buckets are created on MinioArtifactStore init."""
        store = MinioArtifactStore()

        raw_exists = store._client.bucket_exists(MinioArtifactStore.BUCKET_RAW)
        artifacts_exists = store._client.bucket_exists(MinioArtifactStore.BUCKET_ARTIFACTS)

        assert raw_exists
        assert artifacts_exists
