"""Kafka bridge — a transport adapter, NOT a storage backend (ADR-018).

When enabled, Hoglah:
  - **consumes** job-request messages from an input topic, enqueues them into
    the existing JobStore (SQLite/Mongo), and lets the normal serial worker
    process them; and
  - **produces** a result message back to Kafka on terminal status.

Crash safety is built in, not best-effort (see docs/kafka-bridge-design.md §6):

  - **Ingress** uses the *idempotent consumer* pattern: the offset is committed
    only AFTER the job is durably enqueued, and enqueue is idempotent on the
    message's `correlation_id`. So a redelivery after a crash in the
    enqueue→commit window re-enqueues harmlessly (a no-op) — never lost, never
    duplicated.
  - **Egress** uses a *transactional outbox*: a result is marked published in
    the store only AFTER the broker acks it. On startup, terminal jobs that
    were computed but not yet published are re-emitted (`republish_unpublished`).
    With an idempotent producer + consumer-side correlation_id de-dup this gives
    exactly-once *effect* end to end.

`confluent-kafka` is an optional dependency (`pip install "hoglah[kafka]"`),
imported lazily so non-Kafka users never need it. The broker is abstracted
behind the `KafkaTransport` protocol so the crash scenarios are unit-testable
with a deterministic in-memory fake (no broker required).
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from .models import JobRequest, JobResult

logger = logging.getLogger("hoglah")

# JobRequest fields we accept verbatim from an input message (top-level or under
# "options"). Reserved keys are mapped explicitly below.
_REQ_FIELDS = {f.name for f in dataclasses.fields(JobRequest)}
_RESERVED = {"kind", "prompt", "messages", "model", "tags", "metadata", "correlation_id", "reply_to"}


class InvalidMessageError(ValueError):
    """A consumed message cannot be turned into a job (poison → dead-letter)."""


class KafkaPublishError(RuntimeError):
    """A result message was not acknowledged by the broker."""


@dataclass
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    error: str | None = None
    raw: Any = None  # underlying client message, used for offset commit


@dataclass
class ParsedInput:
    request: JobRequest
    correlation_id: str
    reply_to: str | None


def _safe_text(value: bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def parse_input_message(value: bytes) -> ParsedInput:
    """Deserialize + validate an input message into a JobRequest. Raises
    InvalidMessageError for anything un-processable (→ dead-letter topic)."""
    try:
        data = json.loads(value)
    except Exception as exc:  # noqa: BLE001 - any decode failure is poison
        raise InvalidMessageError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidMessageError("message is not a JSON object")

    correlation_id = data.get("correlation_id")
    if not correlation_id or not isinstance(correlation_id, str):
        raise InvalidMessageError("missing or non-string 'correlation_id'")

    model = data.get("model")
    if not model or not isinstance(model, str):
        raise InvalidMessageError("missing or non-string 'model'")

    prompt = data.get("prompt")
    messages = data.get("messages")
    if prompt is None and not messages:
        raise InvalidMessageError("must supply 'prompt' or 'messages'")

    reply_to = data.get("reply_to")
    user_meta = data.get("metadata") or {}
    if not isinstance(user_meta, dict):
        raise InvalidMessageError("'metadata' must be an object")
    options = data.get("options") or {}

    # Accept known JobRequest fields from the top level or from `options`.
    extra: dict[str, Any] = {
        k: v for k, v in {**options, **data}.items() if k in _REQ_FIELDS and k not in _RESERVED
    }
    if options and "options" in _REQ_FIELDS:
        extra.setdefault("options", options)

    try:
        request = JobRequest(
            kind=data.get("kind", "generate"),
            prompt=prompt,
            messages=messages,
            model=model,
            tags=data.get("tags") or [],
            # Stash the Kafka routing info so egress (live or restart-replay) can
            # address the reply and echo the correlation_id.
            metadata={**user_meta, "_kafka": {"correlation_id": correlation_id, "reply_to": reply_to}},
            **extra,
        )
    except TypeError as exc:  # a bad/unknown field value
        raise InvalidMessageError(f"could not build job request: {exc}") from exc

    return ParsedInput(request=request, correlation_id=correlation_id, reply_to=reply_to)


def build_result_message(result_dict: dict[str, Any], correlation_id: str) -> bytes:
    """Build the JSON output message from a (JSON-safe) JobResult dict."""
    meta = result_dict.get("metadata") or {}
    if isinstance(meta, dict):
        meta = {k: v for k, v in meta.items() if k != "_kafka"}
    out = {
        "correlation_id": correlation_id,
        "job_id": result_dict.get("job_id"),
        "status": result_dict.get("status"),
        "model": result_dict.get("model"),
        "output": result_dict.get("output"),
        "embedding": result_dict.get("embedding"),
        "embedding_dim": result_dict.get("embedding_dim"),
        "error": result_dict.get("error"),
        "truncated": result_dict.get("truncated"),
        "timings": result_dict.get("timings"),
        "metadata": meta,
    }
    return json.dumps(out, default=str).encode("utf-8")


# --------------------------------------------------------------------------- #
# Transport abstraction (real broker vs. in-memory fake)
# --------------------------------------------------------------------------- #


class KafkaTransport(Protocol):
    def poll(self, timeout: float) -> KafkaMessage | None: ...
    def commit(self, message: KafkaMessage) -> None: ...
    def produce_and_flush(self, topic: str, key: str | None, value: bytes, timeout: float = 10.0) -> None: ...
    def close(self) -> None: ...


class ConfluentKafkaTransport:
    """Real transport over `confluent-kafka` (librdkafka).

    Consumer: manual offset commit (`enable.auto.commit=False`) so we only ever
    commit after a durable enqueue. Producer: idempotent + `acks=all` so the
    broker de-dups producer retries and a result isn't lost on a transient hiccup.
    """

    def __init__(self, *, bootstrap_servers: str, group_id: str, input_topic: str):
        try:
            from confluent_kafka import Consumer, Producer
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "The Kafka bridge requires confluent-kafka. Install with: pip install 'hoglah[kafka]'"
            ) from exc

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([input_topic])
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def poll(self, timeout: float) -> KafkaMessage | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            return KafkaMessage(
                topic=msg.topic() or "",
                partition=msg.partition() if msg.partition() is not None else -1,
                offset=msg.offset() if msg.offset() is not None else -1,
                key=None,
                value=b"",
                error=str(msg.error()),
                raw=msg,
            )
        key = msg.key()
        return KafkaMessage(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=key.decode("utf-8", "replace") if isinstance(key, bytes) else key,
            value=msg.value() or b"",
            raw=msg,
        )

    def commit(self, message: KafkaMessage) -> None:
        self._consumer.commit(message=message.raw, asynchronous=False)

    def produce_and_flush(self, topic: str, key: str | None, value: bytes, timeout: float = 10.0) -> None:
        errors: list[Any] = []

        def _on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)

        self._producer.produce(topic, key=key, value=value, on_delivery=_on_delivery)
        remaining = self._producer.flush(timeout)
        if remaining and remaining > 0:
            raise KafkaPublishError(f"{remaining} message(s) not acknowledged within {timeout}s")
        if errors:
            raise KafkaPublishError(str(errors[0]))

    def close(self) -> None:
        try:
            self._consumer.close()
        finally:
            self._producer.flush(5.0)


# --------------------------------------------------------------------------- #
# Bridge orchestration
# --------------------------------------------------------------------------- #


class KafkaBridge:
    """Wires a KafkaTransport to a JobStore. Owns the consumer thread and the
    egress (result-producing) path."""

    def __init__(self, *, store: Any, config: Any, transport: KafkaTransport | None = None):
        self._store = store
        self._config = config
        self._transport = transport
        self._running = False
        self._thread: threading.Thread | None = None

    # -- lifecycle --------------------------------------------------------- #

    def _ensure_transport(self) -> KafkaTransport:
        if self._transport is None:
            self._transport = ConfluentKafkaTransport(
                bootstrap_servers=self._config.kafka_bootstrap_servers,
                group_id=self._config.kafka_group_id,
                input_topic=self._config.kafka_input_topic,
            )
        return self._transport

    def start(self) -> None:
        self._ensure_transport()
        # Transactional-outbox recovery: re-emit results computed but not yet
        # published before a previous crash, BEFORE consuming anything new.
        try:
            self.republish_unpublished()
        except Exception:
            logger.exception("Kafka outbox re-publish failed at startup")
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="hoglah-kafka-consumer"
        )
        self._thread.start()
        logger.info(
            "Kafka bridge started (in='%s' results='%s' group='%s')",
            self._config.kafka_input_topic,
            self._config.kafka_results_topic,
            self._config.kafka_group_id,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                logger.exception("Error closing Kafka transport")

    # -- ingress ----------------------------------------------------------- #

    def _consume_loop(self) -> None:
        assert self._transport is not None
        while self._running:
            try:
                msg = self._transport.poll(1.0)
            except Exception:
                logger.exception("Kafka poll failed; backing off")
                time.sleep(1.0)
                continue
            if msg is None:
                continue
            if msg.error is not None:
                logger.warning("Kafka consume error: %s", msg.error)
                continue
            self._handle_message(msg)

    def _handle_message(self, msg: KafkaMessage) -> None:
        """Process exactly one input message. Order is load-bearing for crash
        safety: durable idempotent enqueue FIRST, commit the offset only after."""
        try:
            parsed = parse_input_message(msg.value)
        except InvalidMessageError as exc:
            logger.warning(
                "Poison message at %s[%d]@%d → dead-letter: %s",
                msg.topic, msg.partition, msg.offset, exc,
            )
            self._to_dead_letter(msg, str(exc))
            self._safe_commit(msg)  # commit so the bad message can't block the partition
            return
        # Durable, idempotent enqueue. A crash here (before commit) means the
        # message is redelivered and re-enqueued as a no-op — never duplicated.
        self._store.enqueue(parsed.request, correlation_id=parsed.correlation_id)
        self._safe_commit(msg)

    def _to_dead_letter(self, msg: KafkaMessage, reason: str) -> None:
        assert self._transport is not None
        payload = json.dumps(
            {
                "reason": reason,
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "raw": _safe_text(msg.value),
            }
        ).encode("utf-8")
        try:
            self._transport.produce_and_flush(self._config.kafka_dlt_topic, msg.key, payload)
        except Exception:
            logger.exception("Failed to write poison message to dead-letter topic")

    def _safe_commit(self, msg: KafkaMessage) -> None:
        assert self._transport is not None
        try:
            self._transport.commit(msg)
        except Exception:
            logger.exception("Kafka offset commit failed at %s[%d]@%d", msg.topic, msg.partition, msg.offset)

    # -- egress ------------------------------------------------------------ #

    def publish_result(self, result: JobResult, request: JobRequest | None = None) -> None:
        """Live egress hook (called from Hoglah._deliver on terminal status).

        Only publishes jobs that originated from Kafka (carry a `_kafka`
        correlation_id). Runs on a daemon thread so a slow broker never blocks
        the worker loop — mirroring the ADR-015 HTTP-callback pattern."""
        meta = (request.metadata if request is not None else None) or {}
        kafka_meta = meta.get("_kafka") if isinstance(meta, dict) else None
        if not kafka_meta or not kafka_meta.get("correlation_id"):
            return
        correlation_id = kafka_meta["correlation_id"]
        reply_to = kafka_meta.get("reply_to")
        result_dict = json.loads(json.dumps(asdict(result), default=str))
        value = build_result_message(result_dict, correlation_id)
        threading.Thread(
            target=self._publish_now,
            args=(result.job_id, correlation_id, reply_to, value),
            daemon=True,
            name=f"hoglah-kafka-pub-{result.job_id[:8]}",
        ).start()

    def _publish_now(self, job_id: str, correlation_id: str, reply_to: str | None, value: bytes) -> bool:
        """Produce one result message; mark published in the store ONLY on a
        confirmed broker ack (the outbox flip). On failure, leave it unpublished
        so startup recovery re-emits it."""
        assert self._transport is not None
        topic = reply_to or self._config.kafka_results_topic
        try:
            self._transport.produce_and_flush(topic, correlation_id, value)
        except Exception:
            logger.warning(
                "Kafka result publish failed for job %s (left for restart re-emit)", job_id
            )
            return False
        self._store.mark_result_published(job_id)
        return True

    def republish_unpublished(self) -> int:
        """Outbox recovery: re-emit terminal results not yet acknowledged by the
        broker. Returns the number re-emitted."""
        if not hasattr(self._store, "list_unpublished_terminal"):
            return 0
        count = 0
        for row in self._store.list_unpublished_terminal(limit=500):
            req_meta = (row.get("request") or {}).get("metadata") or {}
            kafka_meta = req_meta.get("_kafka") or {}
            correlation_id = kafka_meta.get("correlation_id")
            if not correlation_id:
                # Not a Kafka-originated job; mark it so we don't rescan forever.
                self._store.mark_result_published(row["id"])
                continue
            reply_to = kafka_meta.get("reply_to")
            value = build_result_message(row.get("result") or {}, correlation_id)
            if self._publish_now(row["id"], correlation_id, reply_to, value):
                count += 1
        if count:
            logger.info("Kafka outbox re-emitted %d unpublished result(s) on startup", count)
        return count
