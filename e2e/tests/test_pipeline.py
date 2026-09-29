"""Level 3: the whole pipeline, through its interfaces only.

One test. Everything else is tested closer to the code; this exists to prove
the wiring -- that a trade produced to Kafka reaches PostgreSQL and comes back
out of the HTTP API.

THIS SUITE ASSUMES A RUNNING STACK. It does not start one, and it does not
skip when one is absent -- it fails, in about a second, naming what is down:

    ./scripts/compose.sh up -d          # bring the stack up first
    cd e2e && ../.venv/bin/pytest -q    # then run this, deliberately

The alternative was a fixture that owns the stack. Assuming one won because
this suite is excluded from ./scripts/check.sh entirely, so running it is
already a deliberate act against a stack someone deliberately started. Owning
the stack costs ~90 seconds a run and needs an answer for .env in CI that does
not exist yet; that is Lesson 4's problem, and building for it now is the same
speculative work as a cursor API with no caller. A skip-when-absent version was
rejected outright: a skip nobody reads reports as success.

Its own rootdir, not an `e2e` marker. Nothing in check.sh names this directory,
so the exclusion is a fact about the layout instead of a `-m` filter somebody
has to remember in two branches of a shell script.

No application imports. All three services expose a package called `app`, so a
test importing two of them could not run in one process -- and a black-box test
should not want to. It produces with confluent_kafka and asserts over HTTP,
exactly as an outside client would. market_core is imported for TradeEvent
alone, so the payload is the one the producer actually sends, schema rules
included.

DELIVERY SEMANTICS. The pipeline is at-least-once: the consumer commits the
offset after the database write, so a crash between the two replays the
message. save_trade() is idempotent on event_id (ON CONFLICT DO NOTHING), so a
replay costs a duplicate delivery, not a duplicate row. Hence the assertion is
`len(trades) == 1` rather than `>= 1` -- this is the wiring test and the
idempotency test in one.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from confluent_kafka import KafkaException, Producer
from market_core import TradeEvent

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "market.trades.raw")
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

# One hop through a local stack: the consumer is already polling, so this is
# generous. Short enough that a broken pipeline is a ten-second answer.
VISIBILITY_TIMEOUT_SECONDS = float(
    os.environ.get("E2E_VISIBILITY_TIMEOUT_SECONDS", "10")
)
POLL_INTERVAL_SECONDS = 0.25
DELIVERY_TIMEOUT_SECONDS = 10.0
REQUEST_TIMEOUT_SECONDS = 5.0


@pytest.fixture(scope="session")
def api_base_url() -> str:
    """The API's base URL, once it has answered /health/ready.

    Checked before anything is produced. Without it, a stack nobody started
    costs the full visibility timeout and then reports "the trade never
    arrived" -- blaming the consumer for a message that was never sent.
    """
    url = f"{API_BASE_URL}/health/ready"

    try:
        status, body = _get(url)
    except urllib.error.URLError as exc:
        pytest.fail(
            f"no API at {API_BASE_URL} ({exc.reason}). This suite assumes a "
            "running stack: ./scripts/compose.sh up -d",
            pytrace=False,
        )

    if status != 200:
        pytest.fail(
            f"{url} returned {status} {body!r}. The API is up but not ready, "
            "which means it cannot reach PostgreSQL.",
            pytrace=False,
        )

    return API_BASE_URL


@pytest.fixture(scope="session")
def producer() -> Producer:
    """A producer whose broker and topic have both been confirmed present.

    Broker auto-creation is disabled on purpose, so a missing topic is a real
    condition rather than something the first produce() papers over.

    The failures are raised outside the except block on purpose: pytest.fail()
    from inside a handler drags the original traceback along, and this message
    is the one somebody reads at 11pm.
    """
    instance = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "market-data-e2e",
            "acks": "all",
        }
    )

    metadata = None
    broker_error = None

    try:
        metadata = instance.list_topics(topic=TOPIC, timeout=5.0)
    except KafkaException as exc:
        broker_error = str(exc)

    if broker_error is not None:
        pytest.fail(
            f"no broker at {BOOTSTRAP_SERVERS} ({broker_error}). This suite "
            "assumes a running stack: ./scripts/compose.sh up -d",
            pytrace=False,
        )

    topic_metadata = metadata.topics.get(TOPIC)
    topic_error = topic_metadata.error if topic_metadata else "missing"

    if topic_error is not None:
        pytest.fail(
            f"topic {TOPIC!r} unavailable on {BOOTSTRAP_SERVERS} "
            f"({topic_error}). The `topics` compose service provisions it; "
            "auto-create is off.",
            pytrace=False,
        )

    return instance


def _publish(producer: Producer, trade: TradeEvent) -> None:
    """Produce one trade and confirm the broker acknowledged it.

    flush() returns the number of messages still queued, so 0 means the queue
    drained -- delivered OR failed. The callback is the only place the verdict
    appears. Skip it and a rejected produce becomes a visibility timeout that
    blames the consumer for a message it never received.
    """
    failures: list[str] = []

    def record_delivery(error, message) -> None:
        if error is not None:
            failures.append(str(error))

    producer.produce(
        topic=TOPIC,
        key=trade.symbol,
        value=trade.model_dump_json(),
        callback=record_delivery,
    )

    remaining = producer.flush(DELIVERY_TIMEOUT_SECONDS)

    assert remaining == 0, (
        f"{remaining} message(s) still queued after "
        f"{DELIVERY_TIMEOUT_SECONDS}s -- the broker never acknowledged"
    )
    assert not failures, f"delivery failed: {failures}"


def _await_visible(base_url: str, symbol: str) -> list[dict]:
    """Poll /trades/{symbol} until it answers 200, or the deadline expires.

    404 is the expected intermediate state, not an error -- the route raises it
    while the symbol has no rows. Any OTHER status ends the test at once rather
    than spending the remaining deadline: a 500 is a different problem and must
    not be reported as "the pipeline is slow". So is the API disappearing
    mid-poll.

    Never sleep-then-assert. The length of the sleep would be a guess about
    another process's scheduling, and the failure it produces cannot tell you
    whether the trade was late or absent.
    """
    deadline = time.monotonic() + VISIBILITY_TIMEOUT_SECONDS
    polls = 0

    while True:
        try:
            status, body = _get(f"{base_url}/trades/{symbol}")
        except urllib.error.URLError as exc:
            pytest.fail(
                f"the API stopped answering after {polls} polls "
                f"({exc.reason}) -- it was reachable at the start of this test",
                pytrace=False,
            )

        polls += 1

        if status == 200:
            return json.loads(body)

        if status != 404:
            pytest.fail(
                f"GET /trades/{symbol} returned {status} {body!r}",
                pytrace=False,
            )

        if time.monotonic() >= deadline:
            pytest.fail(
                f"{symbol} never reached the API: {polls} polls over "
                f"{VISIBILITY_TIMEOUT_SECONDS}s, last status 404. The produce "
                "was acknowledged, so the gap is downstream of the broker -- "
                "check the consumer's logs and the DLQ topic.",
                pytrace=False,
            )

        time.sleep(POLL_INTERVAL_SECONDS)



def test_a_produced_trade_becomes_visible_through_the_api(
    api_base_url: str,
    producer: Producer,
) -> None:
    """Produce one trade, poll until the API serves it, assert the fields.

    The symbol is unique per run, which is what stops this passing on a row the
    last run left behind -- the `violations = 0` lesson. It also isolates the
    assertion from the producer service, which is publishing a trade a second
    into this same topic while the test runs.

    Built uppercase deliberately. TradeEvent's validator would uppercase it
    anyway, but that is a different decision, pinned by its own Level 1 test;
    relying on it here would make this test fail for two unrelated reasons.
    """
    trade = TradeEvent(
        symbol=f"E2E{uuid4().hex[:12].upper()}",
        price=Decimal("215.00"),
        volume=100,
        timestamp=datetime.now(tz=UTC),
        source="e2e",
    )

    _publish(producer, trade)

    trades = _await_visible(api_base_url, trade.symbol)

    assert len(trades) == 1

    stored = trades[0]

    assert stored["symbol"] == trade.symbol
    assert stored["event_id"] == str(trade.event_id)
    assert stored["volume"] == trade.volume
    assert stored["source"] == "e2e"

    # Decimal(str(...)) rather than asserting the JSON form. Whether FastAPI
    # serialises a Decimal as a number or a string is pinned by the api suite's
    # own round-trip test; this one is about the value surviving the trip.
    assert Decimal(str(stored["price"])) == trade.price


def _get(url: str) -> tuple[int, str]:
    """One GET, returning (status, body) for any status rather than raising.

    urllib treats 4xx and 5xx as exceptions. The poll below needs 404 as data,
    not as an error, so both paths come back the same shape here and the
    decision about what a status MEANS is made in one place.
    """
    try:
        with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")