"""Level 2a: the request middleware. No database, no container.

The middleware's metric labels have nothing to do with storage, so this uses a
stub session rather than the integration fixtures. A test that needed a real
PostgreSQL to check a label would be lying about what it depends on.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.database import get_session
from app.main import app


class EmptySession:
    """Enough Session surface for the trade routes, and nothing more.

    The routes call scalars(...).all() and scalar(...). Returning nothing makes
    /trades/{symbol} answer 404, which is fine -- the metric is recorded either
    way, and the status code is not what this test is about.
    """

    def scalars(self, statement):
        return self

    def all(self):
        return []

    def scalar(self, statement):
        return None


def test_metric_labels_use_the_route_template_not_the_raw_path() -> None:
    """Two symbols must produce ONE time series, not two.

    Labelling with request.url.path would create a new series per symbol. With
    three simulated symbols that is invisible; with a real feed it is thousands
    of series for one endpoint, and Prometheus falls over long before the API
    does.

    The mechanism is worth knowing: request.scope["route"] does not exist on
    the way in -- Starlette sets it while routing, inside call_next -- which is
    why the middleware reads the template AFTER awaiting the response. An
    unmatched URL has no route at all and falls back to the constant
    "unmatched", so random 404 probes cannot inflate cardinality either.
    """
    app.dependency_overrides[get_session] = lambda: EmptySession()

    try:
        with TestClient(app) as client:
            client.get("/trades/AAPL")
            client.get("/trades/MSFT")

            body = client.get("/metrics").text
    finally:
        app.dependency_overrides.clear()

    assert 'path="/trades/{symbol}"' in body
    assert 'path="/trades/AAPL"' not in body
    assert 'path="/trades/MSFT"' not in body

