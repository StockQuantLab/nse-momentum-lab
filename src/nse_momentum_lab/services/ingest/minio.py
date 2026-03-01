from __future__ import annotations

import logging
import mimetypes
from io import BytesIO
from pathlib import Path

from minio import Minio

from nse_momentum_lab.config import get_settings

logger = logging.getLogger(__name__)


class MinioArtifactStore:
    BUCKET_RAW = "market-data"
    BUCKET_ARTIFACTS = "artifacts"

    def __init__(self, client: Minio | None = None) -> None:
        if client is None:
            settings = get_settings()
            self._client = Minio(
                endpoint=str(settings.minio_endpoint)
                .replace("http://", "")
                .replace("https://", ""),
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
        else:
            self._client = client
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        for bucket in [self.BUCKET_RAW, self.BUCKET_ARTIFACTS]:
            found = self._client.bucket_exists(bucket)
            if not found:
                logger.info(f"Creating bucket: {bucket}")
                self._client.make_bucket(bucket)

    def get_uri(self, bucket: str, object_name: str) -> str:
        return f"s3://{bucket}/{object_name}"

    def put_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> str:
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return self.get_uri(bucket, object_name)

    def put_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str | Path,
        *,
        content_type: str | None = None,
    ) -> str:
        path = Path(file_path)
        resolved_content_type = (
            content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        )
        self._client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=str(path),
            content_type=resolved_content_type,
        )
        return self.get_uri(bucket, object_name)
