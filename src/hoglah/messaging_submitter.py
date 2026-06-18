"""Submitter-side messaging client (the mirror of the worker bridge).

The bridges in `kafka_bridge` / `rabbitmq` / `redis_streams` are the *worker*
side: they consume job-request messages from an input topic/queue/stream,
execute them, and produce a result message to a `reply_to` destination keyed by
`correlation_id`.

This module is the *submitter* side a client (e.g. Tirzah) uses to dispatch a job
over a broker instead of writing to the shared SQLite store: publish a request
message, then block until the matching result message comes back. The request
body schema is exactly what `parse_input_message` accepts, and the result body is
exactly what `build_result_message` produces — so a running
`hoglah {kafka,rabbitmq,redis}-bridge` on the same topics serves these requests.

Transport-specific request/reply mechanics (one per broker) sit behind a tiny
`SubmitterTransport` protocol so the orchestration is broker-neutral and unit
testable with an in-memory fake. Real broker libraries are imported lazily inside
each transport, so importing this module never requires them.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Protocol

# Reuse the wire formats the worker bridge already defines so the two sides can
# never drift.
from .kafka_bridge import build_result_message  # noqa: F401  (re-exported for symmetry)


class MessagingSubmitError(RuntimeError):
    """A messaging submission failed (publish error, timeout, or transport error)."""


def build_request_message(
    *,
    kind: str,
    prompt: str,
    model: str,
    correlation_id: str,
    reply_to: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    fmt: str | None = None,
    timeout_seconds: int | None = None,
    **extra: Any,
) -> bytes:
    """Build a job-request message body accepted by `parse_input_message`."""
    body: dict[str, Any] = {
        "kind": kind,
        "prompt": prompt,
        "model": model,
        "correlation_id": correlation_id,
        "tags": tags or [],
        "metadata": metadata or {},
    }
    if reply_to is not None:
        body["reply_to"] = reply_to
    if fmt is not None:
        body["format"] = fmt
    if timeout_seconds is not None:
        body["timeout_seconds"] = int(timeout_seconds)
    for key, value in extra.items():
        if value is not None:
            body[key] = value
    return json.dumps(body, default=str).encode("utf-8")


def parse_result_message(value: bytes) -> dict[str, Any] | None:
    """Decode a result message body (as produced by `build_result_message`)."""
    try:
        data = json.loads(value)
    except Exception:  # noqa: BLE001 - a malformed result is simply skipped
        return None
    return data if isinstance(data, dict) else None


class SubmitterTransport(Protocol):
    """Broker-neutral request/reply seam for the submitter side."""

    def publish_request(self, body: bytes, *, correlation_id: str) -> None:
        """Publish a request message to the bridge's input destination."""
        ...

    def await_result(self, correlation_id: str, timeout: float) -> dict[str, Any] | None:
        """Block up to `timeout` for the result whose correlation_id matches."""
        ...

    def reply_destination(self) -> str | None:
        """The reply destination to stamp on the request (None = shared results)."""
        ...

    def close(self) -> None: ...


class MessagingSubmitter:
    """Publish a job request over a transport and await its result.

    One instance owns one transport (producer + reply consumer). Thread-unsafe by
    design: use one submitter per in-flight request, or serialise calls.
    """

    def __init__(self, transport: SubmitterTransport) -> None:
        self._transport = transport

    def submit(
        self,
        *,
        kind: str,
        prompt: str,
        model: str,
        timeout: float,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        fmt: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:  # noqa: D401
        correlation_id = uuid.uuid4().hex
        body = build_request_message(
            kind=kind,
            prompt=prompt,
            model=model,
            correlation_id=correlation_id,
            reply_to=self._transport.reply_destination(),
            tags=tags,
            metadata=metadata,
            fmt=fmt,
            timeout_seconds=int(timeout),
            **extra,
        )
        deadline = time.monotonic() + timeout
        self._transport.publish_request(body, correlation_id=correlation_id)
        remaining = max(0.0, deadline - time.monotonic())
        result = self._transport.await_result(correlation_id, remaining)
        if result is None:
            raise MessagingSubmitError(
                f"no result for correlation_id {correlation_id} within {timeout:.0f}s"
            )
        return result

    def close(self) -> None:
        self._transport.close()


# --------------------------------------------------------------------------- #
# Transport implementations (one per broker). Broker libraries are imported
# lazily so this module imports without confluent-kafka / pika / redis present.
# --------------------------------------------------------------------------- #


class KafkaSubmitterTransport:
    """Kafka submitter: produce to the input topic, consume the results topic.

    A throwaway consumer group (unique per instance) reading from `earliest`
    scans the results topic for the matching `correlation_id` — race-free without
    coordinating the subscribe against the produce, at the cost of skipping over
    other submitters' results.
    """

    def __init__(self, *, bootstrap_servers: str, input_topic: str, results_topic: str) -> None:
        from confluent_kafka import Consumer, Producer

        self._input_topic = input_topic
        self._results_topic = results_topic
        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all"}
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"hoglah-submitter-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([results_topic])

    def reply_destination(self) -> str | None:
        return self._results_topic

    def publish_request(self, body: bytes, *, correlation_id: str) -> None:
        errors: list[Any] = []
        self._producer.produce(
            self._input_topic,
            key=correlation_id,
            value=body,
            on_delivery=lambda err, _msg: err is not None and errors.append(err),
        )
        remaining = self._producer.flush(10.0)
        if remaining:
            raise MessagingSubmitError("Kafka request not acknowledged within 10s")
        if errors:
            raise MessagingSubmitError(str(errors[0]))

    def await_result(self, correlation_id: str, timeout: float) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self._consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
            if msg is None or msg.error():
                continue
            data = parse_result_message(msg.value() or b"")
            if data and data.get("correlation_id") == correlation_id:
                return data
        return None

    def close(self) -> None:
        try:
            self._consumer.close()
        finally:
            self._producer.flush(5.0)


class RabbitMQSubmitterTransport:
    """RabbitMQ RPC submitter: publish to the input queue with a private,
    exclusive reply queue; the bridge replies there keyed by correlation_id."""

    def __init__(self, *, url: str, input_queue: str) -> None:
        import pika

        self._pika = pika
        self._input_queue = input_queue
        self._conn = pika.BlockingConnection(pika.URLParameters(url))
        self._ch = self._conn.channel()
        # A private, auto-deleted reply queue tied to this connection.
        declared = self._ch.queue_declare(queue="", exclusive=True, auto_delete=True)
        self._reply_queue = declared.method.queue

    def reply_destination(self) -> str | None:
        return self._reply_queue

    def publish_request(self, body: bytes, *, correlation_id: str) -> None:
        props = self._pika.BasicProperties(
            correlation_id=correlation_id, reply_to=self._reply_queue, delivery_mode=2
        )
        self._ch.basic_publish(
            exchange="", routing_key=self._input_queue, body=body, properties=props
        )

    def await_result(self, correlation_id: str, timeout: float) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            method, _props, body = self._ch.basic_get(self._reply_queue, auto_ack=True)
            if method is None:
                self._conn.sleep(0.2)
                continue
            data = parse_result_message(body or b"")
            if data and data.get("correlation_id") == correlation_id:
                return data
        return None

    def close(self) -> None:
        try:
            self._ch.close()
        finally:
            try:
                self._conn.close()
            except Exception:
                pass


class RedisSubmitterTransport:
    """Redis Streams submitter: XADD to the input stream, then XREAD the results
    stream from the position captured *before* the add (race-free), matching the
    correlation_id."""

    def __init__(self, *, url: str, input_stream: str, results_stream: str) -> None:
        import redis

        self._r = redis.Redis.from_url(url)
        self._input_stream = input_stream
        self._results_stream = results_stream
        self._since_id = "0"

    def reply_destination(self) -> str | None:
        return self._results_stream

    def publish_request(self, body: bytes, *, correlation_id: str) -> None:
        # Capture the results-stream tail BEFORE adding the request so await never
        # misses a result produced between publish and read.
        last = self._r.xrevrange(self._results_stream, count=1)
        self._since_id = last[0][0] if last else "0"
        try:
            self._r.xadd(
                self._input_stream,
                {"data": body, "correlation_id": correlation_id, "reply_to": self._results_stream},
            )
        except Exception as exc:  # noqa: BLE001
            raise MessagingSubmitError(str(exc)) from exc

    def await_result(self, correlation_id: str, timeout: float) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        since = self._since_id
        while time.monotonic() < deadline:
            block_ms = int(max(1, min(1000, (deadline - time.monotonic()) * 1000)))
            resp = self._r.xread({self._results_stream: since}, count=10, block=block_ms)
            if not resp:
                continue
            _stream, entries = resp[0]
            for entry_id, fields in entries:
                since = entry_id
                data = parse_result_message(fields.get(b"data", b""))
                if data and data.get("correlation_id") == correlation_id:
                    return data
        return None

    def close(self) -> None:
        try:
            self._r.close()
        except Exception:
            pass


def make_submitter_transport(
    transport: str,
    *,
    kafka_bootstrap_servers: str | None = None,
    kafka_input_topic: str | None = None,
    kafka_results_topic: str | None = None,
    rabbitmq_url: str | None = None,
    rabbitmq_input_queue: str | None = None,
    redis_url: str | None = None,
    redis_input_stream: str | None = None,
    redis_results_stream: str | None = None,
) -> SubmitterTransport:
    """Construct a submitter transport by name. Connection params are explicit so
    callers (e.g. Tirzah) need not depend on Hoglah's config object."""
    if transport == "kafka":
        return KafkaSubmitterTransport(
            bootstrap_servers=kafka_bootstrap_servers or "localhost:9092",
            input_topic=kafka_input_topic or "hoglah-jobs",
            results_topic=kafka_results_topic or "hoglah-results",
        )
    if transport == "rabbitmq":
        return RabbitMQSubmitterTransport(
            url=rabbitmq_url or "amqp://guest:guest@localhost:5672/",
            input_queue=rabbitmq_input_queue or "hoglah-jobs",
        )
    if transport == "redis":
        return RedisSubmitterTransport(
            url=redis_url or "redis://localhost:6379/0",
            input_stream=redis_input_stream or "hoglah-jobs",
            results_stream=redis_results_stream or "hoglah-results",
        )
    raise ValueError(f"unknown messaging transport {transport!r}; expected kafka|rabbitmq|redis")

