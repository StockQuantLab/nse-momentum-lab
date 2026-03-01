from fastapi.testclient import TestClient

from nse_momentum_lab.api.app import create_app


def test_health() -> None:
    app = create_app()
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
