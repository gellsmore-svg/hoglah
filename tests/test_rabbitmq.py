"""RabbitMQ bridge tests (ADR-019).

The broker-neutral crash-safety logic is already proven by the shared
FakeTransport suite in test_kafka_bridge.py. Here we cover:
  - the bridge's broker *selection* wiring (non-gated, no broker), and
  - a real end-to-end round-trip + poison→dead-letter, gated behind
    RUN_RABBITMQ_TESTS=1 (needs pika + a broker at localhost:5672).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from types import SimpleNamespace

import pytest

from hoglah.kafka_bridge import MessageBridge

requires_rabbitmq = pytest.mark.skipif(
    os.environ.get("RUN_RABBITMQ_TESTS") != "1",
    reason="RabbitMQ tests require RUN_RABBITMQ_TESTS=1, pika, and a broker at localhost:5672.",
)

RABBITMQ_URL = os.environ.get("HOGLAH_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


# --------------------------------------------------------------------------- #
# Broker selection (non-gated — no connection made)
# --------------------------------------------------------------------------- #


def test_bridge_selects_rabbitmq_when_only_rabbitmq_enabled():
    cfg = SimpleNamespace(
        rabbitmq_enabled=True,
        kafka_enabled=False,
        rabbitmq_input_queue="in-q",
        rabbitmq_results_queue="out-q",
    )
    bridge = MessageBridge(store=None, config=cfg, transport=None)
    assert bridge._broker == "rabbitmq"
    assert bridge._input_name == "in-q"
    assert bridge._results_dest == "out-q"


def test_bridge_prefers_kafka_when_both_enabled():
    cfg = SimpleNamespace(
        rabbitmq_enabled=True,
        kafka_enabled=True,
        kafka_input_topic="k-in",
        kafka_results_topic="k-out",
        rabbitmq_input_queue="r-in",
        rabbitmq_results_queue="r-out",
    )
    bridge = MessageBridge(store=None, config=cfg, transport=None)
    assert bridge._broker == "kafka"
    assert bridge._input_name == "k-in"
    assert bridge._results_dest == "k-out"


# --------------------------------------------------------------------------- #
# Gated real-broker tests
# --------------------------------------------------------------------------- #


@requires_rabbitmq
def test_real_rabbitmq_round_trip(tmp_path):
    """Publish an input message; a rabbitmq_enabled Hoglah (StubAdapter) consumes
    → processes → publishes a result; consume it and match by correlation_id."""
    import pika

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_q = f"hoglah-jobs-{run}"
    out_q = f"hoglah-results-{run}"
    dlx = f"hoglah-dlx-{run}"
    dlq = f"hoglah-jobs-dlq-{run}"
    correlation_id = f"corr-{run}"

    h = Hoglah(
        config={
            "rabbitmq_enabled": True,
            "rabbitmq_url": RABBITMQ_URL,
            "rabbitmq_input_queue": in_q,
            "rabbitmq_results_queue": out_q,
            "rabbitmq_dlx": dlx,
            "rabbitmq_dlq": dlq,
            "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    try:
        # The bridge declared in_q/out_q on startup; publish the request.
        ch.basic_publish(
            exchange="",
            routing_key=in_q,
            body=json.dumps({"correlation_id": correlation_id, "model": "stub", "prompt": "hi"}).encode(),
            properties=pika.BasicProperties(correlation_id=correlation_id),
        )
        deadline = time.time() + 30
        received = None
        while time.time() < deadline and received is None:
            method, _props, body = ch.basic_get(out_q, auto_ack=True)
            if body is None:
                time.sleep(0.2)
                continue
            payload = json.loads(body)
            if payload.get("correlation_id") == correlation_id:
                received = payload
        assert received is not None, "no result message received from the RabbitMQ bridge"
        assert received["status"] == "completed"
    finally:
        h.close()
        for q in (in_q, out_q, dlq):
            try:
                ch.queue_delete(q)
            except Exception:
                pass
        try:
            ch.exchange_delete(dlx)
        except Exception:
            pass
        conn.close()


@requires_rabbitmq
def test_real_rabbitmq_property_only_correlation_id(tmp_path):
    """A message whose body omits correlation_id but sets the AMQP correlation_id
    property still round-trips (ADR-019 property fallback)."""
    import pika

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_q, out_q, dlx, dlq = (f"hoglah-jobs-{run}", f"hoglah-results-{run}", f"hoglah-dlx-{run}", f"hoglah-dlq-{run}")
    correlation_id = f"prop-{run}"

    h = Hoglah(
        config={
            "rabbitmq_enabled": True, "rabbitmq_url": RABBITMQ_URL,
            "rabbitmq_input_queue": in_q, "rabbitmq_results_queue": out_q,
            "rabbitmq_dlx": dlx, "rabbitmq_dlq": dlq, "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    try:
        # Body has NO correlation_id; it's only in the AMQP property.
        ch.basic_publish(
            exchange="", routing_key=in_q,
            body=json.dumps({"model": "stub", "prompt": "hi"}).encode(),
            properties=pika.BasicProperties(correlation_id=correlation_id),
        )
        deadline = time.time() + 30
        received = None
        while time.time() < deadline and received is None:
            _m, _p, body = ch.basic_get(out_q, auto_ack=True)
            if body is None:
                time.sleep(0.2)
                continue
            payload = json.loads(body)
            if payload.get("correlation_id") == correlation_id:
                received = payload
        assert received is not None, "property-only correlation_id message did not round-trip"
        assert received["status"] == "completed"
    finally:
        h.close()
        for q in (in_q, out_q, dlq):
            try:
                ch.queue_delete(q)
            except Exception:
                pass
        try:
            ch.exchange_delete(dlx)
        except Exception:
            pass
        conn.close()


@requires_rabbitmq
def test_real_rabbitmq_unroutable_reply_to_keeps_outbox_hot(tmp_path):
    """A result addressed to a non-existent reply_to queue is unroutable
    (mandatory + confirms) → the job is NOT marked published, so the outbox
    retains it for re-emit. No silent 'delivered'."""
    import pika

    from hoglah import Hoglah, JobStatus

    run = uuid.uuid4().hex[:8]
    in_q, out_q, dlx, dlq = (f"hoglah-jobs-{run}", f"hoglah-results-{run}", f"hoglah-dlx-{run}", f"hoglah-dlq-{run}")
    correlation_id = f"unroutable-{run}"

    h = Hoglah(
        config={
            "rabbitmq_enabled": True, "rabbitmq_url": RABBITMQ_URL,
            "rabbitmq_input_queue": in_q, "rabbitmq_results_queue": out_q,
            "rabbitmq_dlx": dlx, "rabbitmq_dlq": dlq, "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    try:
        ch.basic_publish(
            exchange="", routing_key=in_q,
            body=json.dumps(
                {"correlation_id": correlation_id, "model": "stub", "prompt": "hi",
                 "reply_to": f"no-such-queue-{run}"}
            ).encode(),
        )
        # Wait for the job to reach a terminal state, then for the egress attempt
        # (to the bogus reply_to) to fail and leave it in the outbox.
        deadline = time.time() + 30
        unpublished = []
        while time.time() < deadline:
            terminal = [
                r for r in h._store.list(limit=50)
                if r["status"] in (JobStatus.COMPLETED.value, JobStatus.FAILED.value)
            ]
            if terminal:
                time.sleep(1.0)  # let the egress thread attempt + fail
                unpublished = h._store.list_unpublished_terminal(limit=50)
                break
            time.sleep(0.2)
        assert any(
            (r.get("request") or {}).get("metadata", {}).get("_kafka", {}).get("correlation_id") == correlation_id
            for r in unpublished
        ), "unroutable result should remain unpublished in the outbox"
    finally:
        h.close()
        for q in (in_q, out_q, dlq):
            try:
                ch.queue_delete(q)
            except Exception:
                pass
        try:
            ch.exchange_delete(dlx)
        except Exception:
            pass
        conn.close()


@requires_rabbitmq
def test_real_rabbitmq_poison_to_dlq(tmp_path):
    """A poison (un-parseable) message is nacked → routed to the dead-letter
    queue, and no job is created."""
    import pika

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_q = f"hoglah-jobs-{run}"
    out_q = f"hoglah-results-{run}"
    dlx = f"hoglah-dlx-{run}"
    dlq = f"hoglah-jobs-dlq-{run}"

    h = Hoglah(
        config={
            "rabbitmq_enabled": True,
            "rabbitmq_url": RABBITMQ_URL,
            "rabbitmq_input_queue": in_q,
            "rabbitmq_results_queue": out_q,
            "rabbitmq_dlx": dlx,
            "rabbitmq_dlq": dlq,
            "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    try:
        ch.basic_publish(exchange="", routing_key=in_q, body=b"{not valid json")
        deadline = time.time() + 30
        dead = None
        while time.time() < deadline and dead is None:
            method, _props, body = ch.basic_get(dlq, auto_ack=True)
            if body is None:
                time.sleep(0.2)
                continue
            dead = body
        assert dead is not None, "poison message never reached the dead-letter queue"
        assert h.stats()["counts"] == {} or "queued" not in h.stats()["counts"]
    finally:
        h.close()
        for q in (in_q, out_q, dlq):
            try:
                ch.queue_delete(q)
            except Exception:
                pass
        try:
            ch.exchange_delete(dlx)
        except Exception:
            pass
        conn.close()
