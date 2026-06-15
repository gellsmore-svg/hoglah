"""Kafka bridge tests (ADR-018).

The crash-safety behaviour is proven deterministically with an in-memory
`FakeKafkaTransport` (no broker needed): idempotent ingress, enqueue-then-commit
ordering, redelivery after a simulated crash, poison→dead-letter, and the
transactional-outbox egress (mark-published only after ack, re-emit on restart).

A real end-to-end round-trip against a live broker is gated behind
RUN_KAFKA_TESTS=1 (needs confluent-kafka + a Kafka at localhost:9092).
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import deque
from types import SimpleNamespace

import pytest

from hoglah.kafka_bridge import (
    KafkaBridge,
    KafkaMessage,
    KafkaPublishError,
    parse_input_message,
)
from hoglah.models import JobRequest, JobResult, JobStatus
from hoglah.store import create_sqlite_store

requires_mongo = pytest.mark.skipif(
    os.environ.get("RUN_MONGO_TESTS") != "1",
    reason="Mongo bridge tests require RUN_MONGO_TESTS=1 and a MongoDB at mongodb://localhost:27017.",
)


# --------------------------------------------------------------------------- #
# Fake transport + fixtures
# --------------------------------------------------------------------------- #


class FakeKafkaTransport:
    """Deterministic in-memory MessageTransport. Lets tests inject input
    messages, inspect produced (egress) messages, dead-lettered (poison)
    messages, and acked offsets, and simulate broker failures and crashes.

    `nack` models a generic transport: a successful dead-letter both records the
    message and acks it (the message won't be redelivered); a broker failure
    raises so the bridge leaves it un-acked for retry."""

    def __init__(self) -> None:
        self._inbox: deque[KafkaMessage] = deque()
        self.produced: list[tuple[str, str | None, bytes]] = []  # egress results
        self.dead_lettered: list[tuple[KafkaMessage, str]] = []  # poison
        self.acked: list[tuple[str, int, int]] = []
        self.fail_publish = False
        self._next_offset = 0

    def add_input(self, value: bytes, *, key: str | None = None, topic: str = "hoglah-jobs") -> KafkaMessage:
        msg = KafkaMessage(
            source=topic, partition=0, offset=self._next_offset, key=key, value=value, raw=("raw", self._next_offset)
        )
        self._next_offset += 1
        self._inbox.append(msg)
        return msg

    # MessageTransport protocol
    def poll(self, timeout: float) -> KafkaMessage | None:
        return self._inbox.popleft() if self._inbox else None

    def ack(self, message: KafkaMessage) -> None:
        self.acked.append((message.source, message.partition, message.offset))

    def nack(self, message: KafkaMessage, reason: str) -> None:
        if self.fail_publish:
            raise KafkaPublishError("simulated broker failure")
        self.dead_lettered.append((message, reason))
        self.acked.append((message.source, message.partition, message.offset))

    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None:
        if self.fail_publish:
            raise KafkaPublishError("simulated broker failure")
        self.produced.append((dest, key, value))

    def close(self) -> None:
        pass


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        kafka_input_topic="hoglah-jobs",
        kafka_results_topic="hoglah-results",
        kafka_dlt_topic="hoglah-jobs-dlt",
        kafka_group_id="hoglah",
        kafka_bootstrap_servers="localhost:9092",
    )


@pytest.fixture
def store(tmp_path):
    s = create_sqlite_store(tmp_path / "kafka.db")
    yield s
    s.close()


def _bridge(store) -> tuple[KafkaBridge, FakeKafkaTransport]:
    transport = FakeKafkaTransport()
    return KafkaBridge(store=store, config=_config(), transport=transport), transport


def _input(correlation_id: str, **extra) -> bytes:
    body = {"correlation_id": correlation_id, "model": "m", "prompt": "hi", **extra}
    return json.dumps(body).encode("utf-8")


# --------------------------------------------------------------------------- #
# Ingress
# --------------------------------------------------------------------------- #


def test_ingress_enqueues_then_acks(store):
    bridge, t = _bridge(store)
    msg = t.add_input(_input("c1"))
    bridge._handle_message(msg)

    assert store.get_status_counts().get("queued") == 1
    jobs = store.list()
    assert len(jobs) == 1
    # correlation_id + reply routing stashed in request metadata
    assert jobs[0]["request"]["metadata"]["_kafka"]["correlation_id"] == "c1"
    # acked exactly once, AFTER the enqueue
    assert t.acked == [("hoglah-jobs", 0, msg.offset)]


def test_ingress_idempotent_on_duplicate(store):
    bridge, t = _bridge(store)
    bridge._handle_message(t.add_input(_input("dup")))
    bridge._handle_message(t.add_input(_input("dup")))  # redelivery of same correlation_id
    assert store.get_status_counts().get("queued") == 1
    assert len(store.list()) == 1


def test_crash_between_enqueue_and_ack_is_safe(store):
    """The load-bearing crash scenario: enqueue succeeds, the process dies before
    acking, the broker redelivers. Result: exactly one job."""
    bridge, t = _bridge(store)
    payload = _input("x")

    # First delivery: enqueue, then "crash" before ack (ack raises; the bridge
    # logs and swallows — the message is NOT recorded as acked).
    m1 = t.add_input(payload)
    real_ack = t.ack
    t.ack = lambda msg: (_ for _ in ()).throw(RuntimeError("crash before ack"))  # type: ignore[assignment]
    bridge._handle_message(m1)
    t.ack = real_ack  # type: ignore[assignment]
    assert t.acked == []  # never acked → broker will redeliver

    # Redelivery after restart: idempotent enqueue is a no-op; ack now lands.
    m2 = t.add_input(payload)
    bridge._handle_message(m2)
    assert len(store.list()) == 1
    assert t.acked == [("hoglah-jobs", 0, m2.offset)]


@pytest.mark.parametrize(
    "bad",
    [b"{not valid json", json.dumps({"model": "m", "prompt": "x"}).encode(), json.dumps({"correlation_id": "c"}).encode()],
)
def test_poison_message_is_dead_lettered_and_acked(store, bad):
    bridge, t = _bridge(store)
    msg = t.add_input(bad)
    bridge._handle_message(msg)

    assert store.get_status_counts() == {}  # no job created
    assert len(t.dead_lettered) == 1  # parked in the dead-letter
    assert t.produced == []  # no egress
    assert t.acked == [("hoglah-jobs", 0, msg.offset)]  # acked past the poison


# --------------------------------------------------------------------------- #
# Egress (transactional outbox)
# --------------------------------------------------------------------------- #


def _complete_kafka_job(store, correlation_id: str, reply_to: str | None, output: str = "done") -> str:
    req = JobRequest(prompt="hi", model="m", metadata={"_kafka": {"correlation_id": correlation_id, "reply_to": reply_to}})
    jid = store.enqueue(req, correlation_id=correlation_id)
    store.set_result(jid, JobResult(job_id=jid, status=JobStatus.COMPLETED, output=output, model="m"))
    return jid


def test_egress_publishes_marks_and_honors_reply_to(store):
    bridge, t = _bridge(store)
    _complete_kafka_job(store, "c9", reply_to="team-x", output="hello")

    assert bridge.republish_unpublished() == 1
    topic, key, value = t.produced[-1]
    assert topic == "team-x"  # reply_to overrides the default results topic
    assert key == "c9"
    out = json.loads(value)
    assert out["correlation_id"] == "c9" and out["status"] == "completed" and out["output"] == "hello"

    # marked published → not re-emitted
    assert bridge.republish_unpublished() == 0


def test_egress_unacked_result_is_retried_on_restart(store):
    """Crash scenario: result computed, but the broker never acked the produce.
    It must stay in the outbox and be re-emitted, not silently lost."""
    bridge, t = _bridge(store)
    _complete_kafka_job(store, "c", reply_to=None)

    t.fail_publish = True
    assert bridge.republish_unpublished() == 0  # broker rejected → nothing acked
    assert store.list_unpublished_terminal()  # still pending in the outbox

    t.fail_publish = False
    assert bridge.republish_unpublished() == 1  # re-emitted on the next pass
    assert t.produced[-1][0] == "hoglah-results"  # default topic when reply_to is None
    assert bridge.republish_unpublished() == 0  # now marked, no rescan


def test_non_kafka_jobs_never_enter_the_outbox(store):
    jid = store.enqueue(JobRequest(prompt="hi", model="m"))  # no correlation_id
    store.set_result(jid, JobResult(job_id=jid, status=JobStatus.COMPLETED, output="x", model="m"))
    assert store.list_unpublished_terminal() == []


def test_publish_result_noop_for_non_kafka_job(store):
    bridge, t = _bridge(store)
    result = JobResult(job_id="j1", status=JobStatus.COMPLETED, output="x", model="m")
    bridge.publish_result(result, JobRequest(prompt="hi", model="m"))  # no _kafka metadata
    time.sleep(0.1)
    assert t.produced == []


# --------------------------------------------------------------------------- #
# Message parsing
# --------------------------------------------------------------------------- #


def test_parse_input_maps_fields_and_options():
    parsed = parse_input_message(
        json.dumps(
            {
                "correlation_id": "c",
                "model": "m",
                "prompt": "hi",
                "tags": ["a", "b"],
                "reply_to": "r",
                "kind": "generate",
                "options": {"temperature": 0.5},
            }
        ).encode()
    )
    assert parsed.correlation_id == "c"
    assert parsed.reply_to == "r"
    assert parsed.request.model == "m"
    assert parsed.request.tags == ["a", "b"]
    assert parsed.request.metadata["_kafka"]["reply_to"] == "r"


def test_reply_to_must_be_a_string():
    from hoglah.kafka_bridge import InvalidMessageError

    with pytest.raises(InvalidMessageError):
        parse_input_message(json.dumps({"correlation_id": "c", "model": "m", "prompt": "x", "reply_to": 123}).encode())


# --------------------------------------------------------------------------- #
# Regression tests for the hardening findings (v0.5.1)
# --------------------------------------------------------------------------- #


def test_poison_not_committed_when_dlt_write_fails(store):
    """Critical: a poison message must NOT be committed if the dead-letter write
    fails — otherwise it is silently lost. It must be retried instead."""
    bridge, t = _bridge(store)
    t.fail_publish = True  # dead-letter write will fail
    m = t.add_input(b"{bad json")
    bridge._handle_message(m)
    assert t.dead_lettered == []  # nothing dead-lettered
    assert t.acked == []  # and NOT acked → broker will redeliver

    # Broker recovers; redelivery now dead-letters and acks.
    t.fail_publish = False
    m2 = t.add_input(b"{bad json")
    bridge._handle_message(m2)
    assert len(t.dead_lettered) == 1
    assert t.acked == [("hoglah-jobs", 0, m2.offset)]


def test_egress_crash_after_ack_before_mark_is_reemitted(store):
    """The (b) crash window: produce is acked but the post-ack mark fails. The
    result must stay in the outbox and be re-emitted (at-least-once egress)."""
    bridge, t = _bridge(store)
    _complete_kafka_job(store, "ackmark", reply_to=None)

    real_mark = store.mark_result_published
    store.mark_result_published = lambda jid: (_ for _ in ()).throw(RuntimeError("crash after ack"))  # type: ignore[method-assign]
    assert bridge.republish_unpublished() == 0  # produced, but mark failed → not counted done
    assert len(t.produced) == 1  # it WAS published once
    assert store.list_unpublished_terminal()  # still pending in the outbox

    store.mark_result_published = real_mark  # type: ignore[method-assign]
    assert bridge.republish_unpublished() == 1  # re-emitted (Kafka duplicate, deduped downstream)
    assert len(t.produced) == 2
    assert bridge.republish_unpublished() == 0


def test_publish_result_live_path_produces_on_thread(store):
    bridge, t = _bridge(store)
    req = JobRequest(prompt="hi", model="m", metadata={"_kafka": {"correlation_id": "live", "reply_to": "out"}})
    result = JobResult(job_id="jlive", status=JobStatus.COMPLETED, output="z", model="m")
    bridge.publish_result(result, req)  # spawns a daemon egress thread

    deadline = time.time() + 3
    while not t.produced and time.time() < deadline:
        time.sleep(0.02)
    assert t.produced, "live egress thread did not produce"
    topic, key, value = t.produced[-1]
    assert topic == "out" and key == "live"
    assert json.loads(value)["correlation_id"] == "live"


def test_concurrent_enqueue_same_correlation_id_yields_one_job(store):
    """The idempotent-enqueue race: many threads enqueue the same correlation_id
    at once; exactly one job exists and all callers see the same id."""
    n = 12
    barrier = threading.Barrier(n)
    ids: list[str] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        jid = store.enqueue(JobRequest(prompt="x", model="m"), correlation_id="race")
        with lock:
            ids.append(jid)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len(set(ids)) == 1
    assert len(store.list()) == 1


@requires_mongo
def test_mongo_bridge_ingress_is_idempotent():
    from hoglah.mongo_store import create_mongo_store

    store = create_mongo_store("mongodb://localhost:27017", "hoglah_test", "hoglah_kafka_" + uuid.uuid4().hex[:8])
    transport = FakeKafkaTransport()
    bridge = KafkaBridge(store=store, config=_config(), transport=transport)
    try:
        payload = _input("mdup")
        bridge._handle_message(transport.add_input(payload))
        bridge._handle_message(transport.add_input(payload))  # redelivery
        assert store.get_status_counts().get("queued") == 1
        assert len(store.list()) == 1
    finally:
        store._col.drop()
        store.close()


# --------------------------------------------------------------------------- #
# Gated real-broker round-trip
# --------------------------------------------------------------------------- #

requires_kafka = pytest.mark.skipif(
    os.environ.get("RUN_KAFKA_TESTS") != "1",
    reason="Kafka tests require RUN_KAFKA_TESTS=1, confluent-kafka, and a broker at localhost:9092.",
)

KAFKA_BOOTSTRAP = os.environ.get("HOGLAH_KAFKA_BOOTSTRAP", "localhost:9092")


@requires_kafka
def test_real_kafka_round_trip(tmp_path):
    """End to end against a real broker: produce an input message, let a
    kafka_enabled Hoglah (StubAdapter) consume → process → produce a result,
    then consume the result and match it by correlation_id."""
    from confluent_kafka import Consumer, Producer

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_topic = f"hoglah-jobs-{run}"
    out_topic = f"hoglah-results-{run}"
    correlation_id = f"corr-{run}"

    # Produce one input message.
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(
        in_topic,
        key=correlation_id,
        value=json.dumps({"correlation_id": correlation_id, "model": "stub", "prompt": "hi"}).encode(),
    )
    producer.flush(10)

    # Result consumer (subscribe before the bridge produces).
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"verify-{run}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([out_topic])

    h = Hoglah(
        config={
            "kafka_enabled": True,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP,
            "kafka_input_topic": in_topic,
            "kafka_results_topic": out_topic,
            "kafka_group_id": f"hoglah-{run}",
            "db_path": str(tmp_path / "k.db"),
        },
        start_worker=True,
    )
    try:
        deadline = time.time() + 30
        received = None
        while time.time() < deadline and received is None:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value())
            if payload.get("correlation_id") == correlation_id:
                received = payload
        assert received is not None, "no result message received from the bridge"
        assert received["status"] == "completed"
    finally:
        h.close()
        consumer.close()
