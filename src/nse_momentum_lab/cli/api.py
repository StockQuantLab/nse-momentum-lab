from __future__ import annotations

import os

import uvicorn


def main() -> None:
    port = int(os.environ.get("API_PORT", "8004"))
    uvicorn.run(
        "nse_momentum_lab.api.app:app",
        host="127.0.0.1",
        port=port,
        reload=True,
        log_level="info",
    )
