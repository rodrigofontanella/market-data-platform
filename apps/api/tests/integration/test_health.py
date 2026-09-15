import pytest
from app.database import get_session
from app.main import app
from sqlalchemy.exc import OperationalError

pytestmark = pytest.mark.integration


def test_ready_reports_ready_when_the_database_is_up(client):
    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_ready_reports_503_when_the_database_is_down(client):
    class DeadSession:
        def execute(self, *args, **kwargs):
            raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    app.dependency_overrides[get_session] = lambda: DeadSession()

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"]["postgresql"]["status"] == "down"
