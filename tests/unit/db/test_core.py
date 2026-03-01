from unittest.mock import MagicMock, patch

from nse_momentum_lab.db.core import (
    create_engine,
    get_engine,
    get_sessionmaker,
)


class TestDBCore:
    @patch("nse_momentum_lab.db.core.get_settings")
    def test_create_engine(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.database_url = "postgresql://user:pass@localhost:5432/db"
        engine = create_engine()
        assert engine is not None

    @patch("nse_momentum_lab.db.core.create_engine")
    @patch("nse_momentum_lab.db.core.get_settings")
    def test_get_engine_singleton(self, mock_settings: MagicMock, mock_create: MagicMock) -> None:
        mock_settings.return_value.database_url = "postgresql://user:pass@localhost:5432/db"
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        engine1 = get_engine()
        engine2 = get_engine()
        assert engine1 is engine2

    @patch("nse_momentum_lab.db.core.get_engine")
    @patch("nse_momentum_lab.db.core.get_settings")
    def test_get_sessionmaker(self, mock_settings: MagicMock, mock_get_engine: MagicMock) -> None:
        mock_settings.return_value.database_url = "postgresql://user:pass@localhost:5432/db"
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        sessionmaker = get_sessionmaker()
        assert sessionmaker is not None

    @patch("nse_momentum_lab.db.core.get_engine")
    @patch("nse_momentum_lab.db.core.get_settings")
    def test_get_sessionmaker_singleton(
        self, mock_settings: MagicMock, mock_get_engine: MagicMock
    ) -> None:
        mock_settings.return_value.database_url = "postgresql://user:pass@localhost:5432/db"
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        sm1 = get_sessionmaker()
        sm2 = get_sessionmaker()
        assert sm1 is sm2
