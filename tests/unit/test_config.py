import os
from unittest.mock import patch

from nse_momentum_lab.config import Settings, get_settings


class TestSettings:
    def test_defaults(self) -> None:
        # Clear all environment variables first to avoid conflicts with Doppler-injected vars
        # Then set only the specific variables we want to test
        test_vars = {
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
            "MINIO_ENDPOINT": "http://localhost:9000",
            "MINIO_ACCESS_KEY": "minio",
            "MINIO_SECRET_KEY": "minio123",
        }
        with patch.dict(os.environ, test_vars, clear=True):
            settings = Settings()
            assert settings.database_url == "postgresql://user:pass@localhost:5432/db"
            assert settings.minio_endpoint == "http://localhost:9000"
            assert settings.minio_access_key == "minio"
            assert settings.minio_secret_key == "minio123"
            assert settings.minio_secure is False
            assert settings.glm_api_key is None
            assert settings.postgres_user is None
            assert settings.postgres_password is None
            assert settings.postgres_db is None
            assert settings.postgres_host == "127.0.0.1"
            assert settings.postgres_port == 5434
            assert settings.minio_root_user is None
            assert settings.minio_root_password is None
            assert settings.minio_host == "127.0.0.1"
            assert settings.minio_port == 9003  # Updated from 9000 to avoid conflicts
            assert settings.minio_console_port == 9004  # Updated from 9001 to avoid conflicts

    def test_database_url_from_postgres_vars(self) -> None:
        with patch.dict(
            os.environ,
            {
                "POSTGRES_USER": "user",
                "POSTGRES_PASSWORD": "pass",
                "POSTGRES_DB": "testdb",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
            },
            clear=True,
        ):
            settings = Settings()
            assert settings.database_url is not None
            assert "postgresql://" in settings.database_url
            # SQLAlchemy URL.create() masks password in string representation for security
            # The password is preserved internally for connections
            assert "user" in settings.database_url
            assert "testdb" in settings.database_url
            assert "127.0.0.1" in settings.database_url
            assert "5434" in settings.database_url

    def test_minio_endpoint_from_vars(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "MINIO_ROOT_USER": "minio",
                "MINIO_ROOT_PASSWORD": "minio123",
                "MINIO_HOST": "localhost",
                "MINIO_PORT": "9000",
            },
        ):
            settings = Settings()
            assert settings.minio_endpoint is not None
            assert settings.minio_endpoint == "http://localhost:9000"
            assert settings.minio_access_key == "minio"
            assert settings.minio_secret_key == "minio123"

    def test_minio_secure(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "MINIO_ROOT_USER": "minio",
                "MINIO_ROOT_PASSWORD": "minio123",
                "MINIO_HOST": "localhost",
                "MINIO_PORT": "9000",
                "MINIO_SECURE": "true",
            },
        ):
            settings = Settings()
            assert settings.minio_endpoint == "https://localhost:9000"

    def test_database_url_preferred_over_postgres_vars(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://custom:custom@localhost:5433/customdb",
                "POSTGRES_USER": "user",
                "POSTGRES_PASSWORD": "pass",
                "POSTGRES_DB": "testdb",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
            },
        ):
            settings = Settings()
            assert settings.database_url == "postgresql://custom:custom@localhost:5433/customdb"

    def test_missing_settings_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            try:
                settings = Settings()
                settings.model_post_init(None)
                raise AssertionError("Should have raised ValueError")
            except ValueError as e:
                error_str = str(e)
                assert "DATABASE_URL" in error_str

    def test_postgres_host_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "POSTGRES_USER": "user",
                "POSTGRES_PASSWORD": "pass",
                "POSTGRES_DB": "testdb",
                "POSTGRES_HOST": "customhost",
                "POSTGRES_PORT": "5433",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
            },
            clear=True,
        ):
            settings = Settings()
            assert settings.database_url is not None
            assert "customhost" in settings.database_url
            assert "5433" in settings.database_url

    def test_data_lake_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
            },
        ):
            settings = Settings()
            assert settings.data_lake_mode == "local"
            assert settings.data_lake_local_dir == "data/parquet"
            assert settings.data_lake_bucket == "market-data"
            assert settings.data_lake_daily_prefix == "parquet/daily"
            assert settings.data_lake_5min_prefix == "parquet/5min"
            assert settings.duckdb_path == "data/market.duckdb"

    def test_invalid_data_lake_mode_raises(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
                "DATA_LAKE_MODE": "invalid_mode",
            },
            clear=True,
        ):
            try:
                Settings()
                raise AssertionError("Should have raised ValueError")
            except ValueError as e:
                assert "DATA_LAKE_MODE" in str(e)

    def test_minio_data_lake_requires_minio_credentials(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "DATA_LAKE_MODE": "minio",
                "MINIO_ENDPOINT": "http://localhost:9000",
            },
            clear=True,
        ):
            try:
                Settings()
                raise AssertionError("Should have raised ValueError")
            except ValueError as e:
                err = str(e)
                assert "MINIO_ACCESS_KEY" in err
                assert "MINIO_SECRET_KEY" in err


class TestGetSettings:
    def test_get_settings(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
                "MINIO_ENDPOINT": "http://localhost:9000",
                "MINIO_ACCESS_KEY": "minio",
                "MINIO_SECRET_KEY": "minio123",
            },
        ):
            settings = get_settings()
            assert isinstance(settings, Settings)
