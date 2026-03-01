"""
Ingestion services.

Market data is loaded from Zerodha Parquet files via DuckDB.
See: nse_momentum_lab.db.market_db
"""

from nse_momentum_lab.services.ingest.minio import MinioArtifactStore

__all__ = ["MinioArtifactStore"]
