from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(db_session):
    from app.database import get_session
    from app.main import app

    app.dependency_overrides[get_session] = lambda: db_session
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()