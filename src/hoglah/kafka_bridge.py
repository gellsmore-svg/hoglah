"""Messaging bridge — a transport adapter, NOT a storage backend (ADR-018).

When enabled, Hoglah:
  - **consumes** job-request messages from an input source, enqueues them into
    the existing JobStore (SQLite/Mongo), and lets the normal serial worker
    process them; and
  - **produces** a result message back on terminal status.

Crash safety is built in, not best-effort (see docs/kafka-bridge-design.md §6):

  - **Ingress** uses the *idempotent consumer* pattern: the message is acked only
    AFTER the job is durably enqueued, and enqueue is idempotent on the message's
    `correlation_id`. So a redelivery after a crash in the enqueue→ack window
    re-enqueues harmlessly (a no-op) — never lost, never duplicated.
  - **Egress** uses a *transactional outbox*: a result is marked published in the
    store only AFTER the broker acks it. On startup, terminal jobs that were
    computed but not yet published are re-emitted (`republish_unpublished`),
    giving exactly-once *effect* (with consumer-side correlation_id de-dup).
  - **Poison** messages are dead-lettered via the transport's `nack`, never lost.

The broker is abstracted behind the `MessageTransport` protocol (poll / ack /
nack / produce_and_flush / close), so the crash scenarios are unit-testable with
a deterministic in-memory fake (no broker required), and additional brokers
(RabbitMQ, …) plug in as new adapters. The first adapter is Kafka
(`ConfluentKafkaTransport`), behind the optional `confluent-kafka` dependency
(`pip install "hoglah[kafka]"`), imported lazily. The names `KafkaTransport` /
`KafkaBridge` / `KafkaMessage` remain as back-compat aliases (ADR-018).
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


class MessagePublishError(RuntimeError):
    """A message was not acknowledged by the broker."""


@dataclass
class Message:
    source: str  # topic / queue the message came from (informational)
    partition: int  # Kafka partition; -1 for brokers without partitions
    offset: int  # Kafka offset; -1 for brokers without offsets
    key: str | None  # message key; also carries a broker-native correlation_id
    value: bytes
    reply_to: str | None = None  # broker-native reply destination (e.g. AMQP reply_to)
    error: str | None = None
    raw: Any = None  # underlying client message, used for ack / nack


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


def dead_letter_envelope(msg: Message, reason: str) -> bytes:
    """The JSON we wrap a poison message in when dead-lettering it."""
    return json.dumps(
        {
            "reason": reason,
            "source": msg.source,
            "partition": msg.partition,
            "offset": msg.offset,
            "raw": _safe_text(msg.value),
        }
    ).encode("utf-8")


def parse_input_message(
    value: bytes, *, correlation_id: str | None = None, reply_to: str | None = None
) -> ParsedInput:
    """Deserialize + validate an input message into a JobRequest. Raises
    InvalidMessageError for anything un-processable (→ dead-letter).

    The JSON body is authoritative. `correlation_id` / `reply_to` passed in (e.g.
    from a broker's native message properties, like AMQP's) are used only as a
    fallback when the body omits them — so a property-only message is not
    poisoned (ADR-019)."""
    try:
        data = json.loads(value)
    except Exception as exc:  # noqa: BLE001 - any decode failure is poison
        raise InvalidMessageError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidMessageError("message is not a JSON object")

    body_corr = data.get("correlation_id")
    corr = body_corr if (isinstance(body_corr, str) and body_corr) else correlation_id
    if not corr or not isinstance(corr, str):
        raise InvalidMessageError("missing or non-string 'correlation_id' (body or message property)")

    model = data.get("model")
    if not model or not isinstance(model, str):
        raise InvalidMessageError("missing or non-string 'model'")

    prompt = data.get("prompt")
    messages = data.get("messages")
    if prompt is None and not messages:
        raise InvalidMessageError("must supply 'prompt' or 'messages'")

    body_reply = data.get("reply_to")
    if body_reply is not None and not isinstance(body_reply, str):
        raise InvalidMessageError("'reply_to' must be a string topic/queue name")
    rt = body_reply if body_reply is not None else reply_to
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
            # Stash the messaging routing info so egress (live or restart-replay)
            # can address the reply and echo the correlation_id. The key is
            # "_kafka" for backward compatibility with jobs enqueued by v0.5.x.
            metadata={**user_meta, "_kafka": {"correlation_id": corr, "reply_to": rt}},
            **extra,
        )
    except TypeError as exc:  # a bad/unknown field value
        raise InvalidMessageError(f"could not build job request: {exc}") from exc

    return ParsedInput(request=request, correlation_id=corr, reply_to=rt)


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
        "truncation_reason": result_dict.get("truncation_reason"),
        "tags": result_dict.get("tags"),
        "parent_job_id": result_dict.get("parent_job_id"),
        "timings": result_dict.get("timings"),
        "metadata": meta,
    }
    return json.dumps(out, default=str).encode("utf-8")


# --------------------------------------------------------------------------- #
# Transport abstraction (real broker vs. in-memory fake)
# --------------------------------------------------------------------------- #


class MessageTransport(Protocol):
    """A broker adapter. `ack`/`nack` are per-message: ack = "handled, don't
    redeliver"; nack = "dead-letter this poison message". Both must be durable —
    raise if the broker did not confirm, so the bridge leaves the message for
    redelivery rather than losing it."""

    def poll(self, timeout: float) -> Message | None: ...
    def ack(self, message: Message) -> None: ...
    def nack(self, message: Message, reason: str) -> None: ...
    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None: ...
    def close(self) -> None: ...


class ConfluentKafkaTransport:
    """Kafka adapter over `confluent-kafka` (librdkafka).

    Consumer: manual offset commit (`enable.auto.commit=False`) so we only ever
    ack (commit) after a durable enqueue. Producer: idempotent + `acks=all` so the
    broker de-dups producer retries and a result isn't lost on a transient hiccup.
    Kafka has no native dead-letter, so `nack` produces the poison message to a
    dead-letter topic and then commits the offset (and only commits if that
    produce was acked — otherwise it raises and the offset stays uncommitted).
    """

    def __init__(self, *, bootstrap_servers: str, group_id: str, input_topic: str, dlt_topic: str):
        try:
            from confluent_kafka import Consumer, Producer
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "The Kafka bridge requires confluent-kafka. Install with: pip install 'hoglah[kafka]'"
            ) from exc

        self._dlt_topic = dlt_topic
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

    def poll(self, timeout: float) -> Message | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            return Message(
                source=msg.topic() or "",
                partition=msg.partition() if msg.partition() is not None else -1,
                offset=msg.offset() if msg.offset() is not None else -1,
                key=None,
                value=b"",
                error=str(msg.error()),
                raw=msg,
            )
        key = msg.key()
        return Message(
            source=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=key.decode("utf-8", "replace") if isinstance(key, bytes) else key,
            value=msg.value() or b"",
            raw=msg,
        )

    def ack(self, message: Message) -> None:
        self._consumer.commit(message=message.raw, asynchronous=False)

    def nack(self, message: Message, reason: str) -> None:
        # Dead-letter then commit. produce_and_flush raises if the DLT write was
        # not acked — in which case we do NOT commit, so the message is retried.
        self.produce_and_flush(self._dlt_topic, message.key, dead_letter_envelope(message, reason))
        self._consumer.commit(message=message.raw, asynchronous=False)

    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None:
        errors: list[Any] = []

        def _on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)

        self._producer.produce(dest, key=key, value=value, on_delivery=_on_delivery)
        remaining = self._producer.flush(timeout)
        if remaining and remaining > 0:
            raise MessagePublishError(f"{remaining} message(s) not acknowledged within {timeout}s")
        if errors:
            raise MessagePublishError(str(errors[0]))

    def close(self) -> None:
        try:
            self._consumer.close()
        finally:
            self._producer.flush(5.0)


# --------------------------------------------------------------------------- #
# Bridge orchestration (broker-neutral)
# --------------------------------------------------------------------------- #


class MessageBridge:
    """Wires a MessageTransport to a JobStore. Owns the consumer thread and the
    egress (result-producing) path. Broker-neutral: all broker specifics live in
    the transport."""

    def __init__(self, *, store: Any, config: Any, transport: MessageTransport | None = None):
        self._store = store
        self._config = config
        self._transport = transport
        self._running = False
        self._thread: threading.Thread | None = None
        # In-flight live-egress publishes, so stop() can drain them before the
        # producer is closed.
        self._egress_lock = threading.Lock()
        self._egress_inflight = 0
        # Which broker is active, and the names used for egress + logging. Decided
        # from config now (not lazily) so the egress path works even when a
        # transport is injected (tests). Checked in precedence order; if several
        # flags are set the first wins (the client warns). The final else makes
        # Kafka the default for configs with no *_enabled flag (injected-transport
        # tests).
        if getattr(config, "rabbitmq_enabled", False) and not getattr(config, "kafka_enabled", False):
            self._broker = "rabbitmq"
            self._input_name = getattr(config, "rabbitmq_input_queue", None)
            self._results_dest = getattr(config, "rabbitmq_results_queue", None)
        elif (
            getattr(config, "redis_enabled", False)
            and not getattr(config, "kafka_enabled", False)
            and not getattr(config, "rabbitmq_enabled", False)
        ):
            self._broker = "redis"
            self._input_name = getattr(config, "redis_input_stream", None)
            self._results_dest = getattr(config, "redis_results_stream", None)
        else:
            self._broker = "kafka"
            self._input_name = getattr(config, "kafka_input_topic", None)
            self._results_dest = getattr(config, "kafka_results_topic", None)

    # -- lifecycle --------------------------------------------------------- #

    def _ensure_transport(self) -> MessageTransport:
        if self._transport is None:
            if self._broker == "rabbitmq":
                from .rabbitmq import create_pika_transport

                self._transport = create_pika_transport(self._config)
            elif self._broker == "redis":
                from .redis_streams import create_redis_streams_transport

                self._transport = create_redis_streams_transport(self._config)
            else:
                self._transport = ConfluentKafkaTransport(
                    bootstrap_servers=self._config.kafka_bootstrap_servers,
                    group_id=self._config.kafka_group_id,
                    input_topic=self._config.kafka_input_topic,
                    dlt_topic=self._config.kafka_dlt_topic,
                )
        return self._transport

    def prime(self) -> None:
        """Ensure the transport exists and drain the egress outbox. Call this
        BEFORE the worker starts: re-emitting computed-but-unpublished results
        must complete before the worker can produce new completions, otherwise a
        pre-existing terminal job could be published twice (once here, once by
        the worker's live _deliver)."""
        self._ensure_transport()
        try:
            self.republish_unpublished()
        except Exception:
            logger.exception("Messaging outbox re-publish failed at startup")

    def start(self, *, skip_republish: bool = False) -> None:
        self._ensure_transport()
        # Transactional-outbox recovery before consuming anything new (unless the
        # caller already primed it — the client does so before the worker starts).
        if not skip_republish:
            try:
                self.republish_unpublished()
            except Exception:
                logger.exception("Messaging outbox re-publish failed at startup")
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="hoglah-msg-consumer"
        )
        self._thread.start()
        logger.info(
            "Messaging bridge started (broker=%s in='%s' results='%s')",
            self._broker, self._input_name, self._results_dest,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        # Drain in-flight live-egress publishes before closing the producer, so a
        # produce_and_flush isn't racing transport.close(). (Post-ack crashes are
        # already covered by the outbox; this just makes shutdown graceful.)
        deadline = time.time() + 3.0
        while time.time() < deadline:
            with self._egress_lock:
                if self._egress_inflight == 0:
                    break
            time.sleep(0.05)
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                logger.exception("Error closing messaging transport")

    # -- ingress ----------------------------------------------------------- #

    def _consume_loop(self) -> None:
        assert self._transport is not None
        while self._running:
            try:
                msg = self._transport.poll(1.0)
            except Exception:
                logger.exception("Messaging poll failed; backing off")
                time.sleep(1.0)
                continue
            if msg is None:
                continue
            if msg.error is not None:
                logger.warning("Messaging consume error: %s", msg.error)
                continue
            try:
                self._handle_message(msg)
            except Exception:
                # A store/transport failure mid-handle must not kill the consumer
                # thread (which would silently stop all consumption). Leave the
                # message un-acked so it is retried, back off, continue.
                logger.exception(
                    "Failed handling message from %s[%d]@%d; not acking (will retry)",
                    msg.source, msg.partition, msg.offset,
                )
                time.sleep(0.5)

    def _handle_message(self, msg: Message) -> None:
        """Process exactly one input message. Order is load-bearing for crash
        safety: durable idempotent enqueue FIRST, ack only after."""
        try:
            # Body is authoritative; the broker's native key/reply_to (e.g. AMQP
            # properties) are fallbacks when the body omits them.
            parsed = parse_input_message(msg.value, correlation_id=msg.key, reply_to=msg.reply_to)
        except InvalidMessageError as exc:
            logger.warning(
                "Poison message from %s[%d]@%d → dead-letter: %s",
                msg.source, msg.partition, msg.offset, exc,
            )
            # nack dead-letters the message. If the transport cannot durably
            # dead-letter it (raises), we do NOT ack — leaving it for redelivery
            # rather than losing it. (For Kafka, nack = produce-to-DLT + commit.)
            self._safe_nack(msg, str(exc))
            return
        # Durable, idempotent enqueue. A crash here (before ack) means the message
        # is redelivered and re-enqueued as a no-op — never duplicated.
        self._store.enqueue(parsed.request, correlation_id=parsed.correlation_id)
        self._safe_ack(msg)

    def _safe_ack(self, msg: Message) -> None:
        assert self._transport is not None
        try:
            self._transport.ack(msg)
        except Exception:
            logger.exception("Message ack failed at %s[%d]@%d", msg.source, msg.partition, msg.offset)

    def _safe_nack(self, msg: Message, reason: str) -> None:
        assert self._transport is not None
        try:
            self._transport.nack(msg, reason)
        except Exception:
            logger.exception(
                "Dead-letter failed at %s[%d]@%d; not acking (will retry)",
                msg.source, msg.partition, msg.offset,
            )

    # -- egress ------------------------------------------------------------ #

    def publish_result(self, result: JobResult, request: JobRequest | None = None) -> None:
        """Live egress hook (called from Hoglah._deliver on terminal status).

        Only publishes jobs that originated from the bridge (carry a `_kafka`
        correlation_id). Runs on a daemon thread so a slow broker never blocks the
        worker loop — mirroring the ADR-015 HTTP-callback pattern."""
        meta = (request.metadata if request is not None else None) or {}
        kafka_meta = meta.get("_kafka") if isinstance(meta, dict) else None
        if not kafka_meta or not kafka_meta.get("correlation_id"):
            return
        correlation_id = kafka_meta["correlation_id"]
        reply_to = kafka_meta.get("reply_to")
        result_dict = json.loads(json.dumps(asdict(result), default=str))
        value = build_result_message(result_dict, correlation_id)

        def _run() -> None:
            with self._egress_lock:
                self._egress_inflight += 1
            try:
                self._publish_now(result.job_id, correlation_id, reply_to, value)
            finally:
                with self._egress_lock:
                    self._egress_inflight -= 1

        threading.Thread(
            target=_run, daemon=True, name=f"hoglah-msg-pub-{result.job_id[:8]}"
        ).start()

    def _publish_now(self, job_id: str, correlation_id: str, reply_to: str | None, value: bytes) -> bool:
        """Produce one result message; mark published in the store ONLY on a
        confirmed broker ack (the outbox flip). If the produce fails — or the
        post-ack mark fails — leave it unpublished so startup recovery re-emits
        it (a re-emit is a duplicate, de-duped downstream by correlation_id)."""
        assert self._transport is not None
        dest = reply_to or self._results_dest
        try:
            self._transport.produce_and_flush(dest, correlation_id, value)
        except Exception:
            logger.warning(
                "Result publish failed for job %s (left for restart re-emit)", job_id
            )
            return False
        try:
            self._store.mark_result_published(job_id)
        except Exception:
            logger.warning(
                "Published job %s but failed to mark it published (will re-emit on restart)", job_id
            )
            return False
        return True

    def republish_unpublished(self, *, batch: int = 500) -> int:
        """Outbox recovery: re-emit terminal results not yet acknowledged by the
        broker. Drains the full backlog (in batches), not just the first `batch`.
        Returns the number re-emitted."""
        if not hasattr(self._store, "list_unpublished_terminal"):
            return 0
        total = 0
        while True:
            rows = self._store.list_unpublished_terminal(limit=batch)
            if not rows:
                break
            removed = 0  # rows that left the unpublished set this pass (emitted or marked)
            for row in rows:
                req_meta = (row.get("request") or {}).get("metadata") or {}
                kafka_meta = req_meta.get("_kafka") or {}
                correlation_id = kafka_meta.get("correlation_id")
                if not correlation_id:
                    # Not a bridge-originated job; mark it so we don't rescan forever.
                    self._store.mark_result_published(row["id"])
                    removed += 1
                    continue
                reply_to = kafka_meta.get("reply_to")
                value = build_result_message(row.get("result") or {}, correlation_id)
                if self._publish_now(row["id"], correlation_id, reply_to, value):
                    total += 1
                    removed += 1
            # No progress (e.g. broker unreachable) → stop instead of spinning;
            # a short batch means the backlog is drained.
            if removed == 0 or len(rows) < batch:
                break
        if total:
            logger.info("Messaging outbox re-emitted %d unpublished result(s) on startup", total)
        return total


# --------------------------------------------------------------------------- #
# Back-compat aliases — Kafka was the first transport (ADR-018). External code
# and the v0.5.x test/CLI surface refer to the Kafka* names.
# --------------------------------------------------------------------------- #
KafkaMessage = Message
KafkaTransport = MessageTransport
KafkaBridge = MessageBridge
KafkaPublishError = MessagePublishError
