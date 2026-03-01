import os
from unittest.mock import patch

from nse_momentum_lab.cli.api import main


class TestCLI:
    @patch("nse_momentum_lab.cli.api.uvicorn")
    def test_api_main(self, mock_uvicorn: patch) -> None:
        with patch.dict(os.environ, {"API_PORT": "8004"}):
            main()
            mock_uvicorn.run.assert_called_once()
