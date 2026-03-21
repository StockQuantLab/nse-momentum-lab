from __future__ import annotations

import logging

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine.url import URL

logger = logging.getLogger(__name__)


def _mask_password(url: str) -> str:
    """Mask password in database URL for logging."""
    if "@" not in url:
        return url
    # Mask password portion of URL
    parts = url.split("@")
    if len(parts) == 2:
        front, back = parts
        if "://" in front and ":" in front.split("://")[1]:
            # Replace password in user:password@host
            protocol_user_pass = front.split("://")
            user_pass = protocol_user_pass[1]
            if ":" in user_pass:
                user = user_pass.split(":")[0]
                front = f"{protocol_user_pass[0]}://{user}:****"
            return f"{front}@{back}"
    return url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    # Preferred (app-facing) settings.
    # These can be set directly OR derived from docker-compose style vars below.
    database_url: str | None = None

    minio_endpoint: str | None = None
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    minio_secure: bool = False

    # Optional (LLM routing). Keep unused in Phase 1 if desired.
    glm_api_key: str | None = None

    # docker-compose style variables (commonly already present in Doppler)
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_db: str | None = None
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5434

    minio_root_user: str | None = None
    minio_root_password: str | None = None
    minio_host: str = "127.0.0.1"
    minio_port: int = 9003  # Changed from 9000 to avoid conflicts
    minio_console_port: int = 9004  # Changed from 9001 to avoid conflicts

    # Kite Connect v4 settings.
    # Keep secrets in Doppler or the process environment; never persist them in .env files.
    kite_api_key: str | None = None
    kite_api_secret: str | None = None
    kite_access_token: str | None = None
    kite_redirect_url: str | None = None
    kite_ws_max_tokens: int = 3000
    kite_quote_batch_size: int = 500
    kite_login_url: str = "https://kite.zerodha.com/connect/login?v=3"
    kite_api_root: str = "https://api.kite.trade"

    # Data lake contract (DuckDB + Parquet)
    data_lake_mode: str = "local"  # local|minio
    data_lake_local_dir: str = "data/parquet"
    data_lake_bucket: str = "market-data"
    data_lake_daily_prefix: str = "parquet/daily"
    data_lake_5min_prefix: str = "parquet/5min"
    duckdb_path: str = "data/market.duckdb"

    def model_post_init(self, __context: object) -> None:
        # Construct database URL using SQLAlchemy URL for safe password handling
        if self.database_url is None:
            if self.postgres_user and self.postgres_password and self.postgres_db:
                # Use SQLAlchemy URL object for safe construction
                # Note: render_as_string(hide_password=False) is required because str() masks passwords
                url_obj = URL.create(
                    drivername="postgresql",
                    username=self.postgres_user,
                    password=self.postgres_password,
                    host=self.postgres_host,
                    port=self.postgres_port,
                    database=self.postgres_db,
                )
                self.database_url = url_obj.render_as_string(hide_password=False)

        if self.minio_endpoint is None:
            scheme = "https" if self.minio_secure else "http"
            self.minio_endpoint = f"{scheme}://{self.minio_host}:{self.minio_port}"

        if self.minio_access_key is None and self.minio_root_user:
            self.minio_access_key = self.minio_root_user
        if self.minio_secret_key is None and self.minio_root_password:
            self.minio_secret_key = self.minio_root_password

        self.data_lake_mode = self.data_lake_mode.lower().strip()
        if self.data_lake_mode not in {"local", "minio"}:
            raise ValueError("DATA_LAKE_MODE must be either 'local' or 'minio'")

        missing: list[str] = []
        if self.database_url is None:
            missing.append(
                "DATABASE_URL (or POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB [+ POSTGRES_HOST/POSTGRES_PORT])"
            )
        if self.data_lake_mode == "minio":
            if self.minio_endpoint is None:
                missing.append("MINIO_ENDPOINT (or MINIO_HOST/MINIO_PORT [+ MINIO_SECURE])")
            if self.minio_access_key is None:
                missing.append("MINIO_ACCESS_KEY (or MINIO_ROOT_USER)")
            if self.minio_secret_key is None:
                missing.append("MINIO_SECRET_KEY (or MINIO_ROOT_PASSWORD)")
            if not self.data_lake_bucket.strip():
                missing.append("DATA_LAKE_BUCKET")
        if missing:
            raise ValueError("Missing required settings: " + "; ".join(missing))

    def get_masked_database_url(self) -> str:
        if self.database_url:
            return _mask_password(self.database_url)
        return "Not configured"

    def has_kite_credentials(self) -> bool:
        return bool(self.kite_api_key and self.kite_access_token)


def get_settings() -> Settings:
    return Settings()  # reads from environment (Doppler injected)
