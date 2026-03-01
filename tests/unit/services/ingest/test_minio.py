from unittest.mock import MagicMock

from nse_momentum_lab.services.ingest.minio import MinioArtifactStore


class TestMinioArtifactStore:
    def test_get_uri(self) -> None:
        store = MinioArtifactStore(client=MagicMock())
        uri = store.get_uri("test-bucket", "path/to/file.txt")
        assert uri == "s3://test-bucket/path/to/file.txt"

    def test_bucket_constants(self) -> None:
        assert MinioArtifactStore.BUCKET_RAW == "market-data"
        assert MinioArtifactStore.BUCKET_ARTIFACTS == "artifacts"

    def test_put_bytes(self) -> None:
        client = MagicMock()
        store = MinioArtifactStore(client=client)

        uri = store.put_bytes(
            MinioArtifactStore.BUCKET_ARTIFACTS,
            "experiments/exp123/summary.json",
            b'{"k":"v"}',
            content_type="application/json",
        )

        client.put_object.assert_called_once()
        assert uri == "s3://artifacts/experiments/exp123/summary.json"

    def test_put_file(self, tmp_path) -> None:
        client = MagicMock()
        store = MinioArtifactStore(client=client)
        file_path = tmp_path / "payload.csv"
        file_path.write_text("a,b\n1,2\n", encoding="utf-8")

        uri = store.put_file(
            MinioArtifactStore.BUCKET_ARTIFACTS,
            "experiments/exp123/payload.csv",
            file_path,
        )

        client.fput_object.assert_called_once()
        assert uri == "s3://artifacts/experiments/exp123/payload.csv"
