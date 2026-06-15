"""RabbitMQ (AMQP 0-9-1) transport for the messaging bridge (ADR-019).

A `MessageTransport` adapter over `pika`, so the broker-neutral `MessageBridge`
(see kafka_bridge.py) can consume job requests from / produce results to
RabbitMQ with the same crash-safety guarantees as Kafka. The bridge logic is
unchanged; only this adapter is RabbitMQ-specific.

Why RabbitMQ maps cleanly (see docs/rabbitmq-bridge-design.md §3):
  - `ack`  = `basic_ack` after a durable enqueue — per message, so a slow/bad
    message never head-of-line-blocks its neighbours.
  - `nack` = `basic_nack(requeue=False)` → the broker routes the message to the
    dead-letter exchange. One broker-side op; no separate "produce to a DLT
    topic" to confirm, so the "dead-lettered but not committed" failure class
    Kafka has does not arise here.
  - egress = publish with **publisher confirms** + `mandatory`; an
    unconfirmed/unroutable publish raises, so the outbox flips only after a real
    ack.

`pika` is an optional dependency (`pip install "hoglah[rabbitmq]"`), imported
lazily. `pika` channels are NOT thread-safe, so this adapter uses a **separate
publisher connection + channel guarded by a lock** for egress (the consumer
thread and the per-job egress daemon threads must not share a channel), and
**reconnects the publisher on failure** (an idle publisher connection can be
dropped by the broker between bursts / across a broker restart).
"""

from __future__ import annotations

import threading
from typing import Any

from .kafka_bridge import Message, MessagePublishError


class PikaTransport:
    """RabbitMQ MessageTransport over pika's BlockingConnection."""

    def __init__(
        self,
        *,
        url: str,
        input_queue: str,
        results_queue: str,
        dlx: str,
        dlq: str,
        prefetch: int = 1,
        declare_topology: bool = True,
    ):
        try:
            import pika
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "The RabbitMQ bridge requires pika. Install with: pip install 'hoglah[rabbitmq]'"
            ) from exc

        self._pika = pika
        self._input_queue = input_queue
        self._results_queue = results_queue
        self._dlx = dlx
        self._dlq = dlq
        self._params = self._build_params(url)
        self._pub_lock = threading.Lock()

        # Consumer connection/channel (used only from the consumer thread).
        self._conn = pika.BlockingConnection(self._params)
        try:
            self._ch = self._conn.channel()
            self._ch.basic_qos(prefetch_count=prefetch)
            if declare_topology:
                self._declare_topology()
            # Dedicated publisher connection/channel (pika is not thread-safe),
            # with publisher confirms so produce_and_flush raises unless acked.
            self._open_publisher()
        except Exception:
            # Don't leak a half-open connection if setup fails partway.
            for conn_attr in ("_pub_conn", "_conn"):
                conn = getattr(self, conn_attr, None)
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            raise

        # A consume generator with a poll-sized inactivity timeout: each next()
        # blocks up to ~1s and yields (None, None, None) on timeout.
        self._consume_gen = self._ch.consume(self._input_queue, inactivity_timeout=1.0)

    def _build_params(self, url: str) -> Any:
        params = self._pika.URLParameters(url)
        # Bound a blocked publish (broker resource alarm / half-open TCP) so
        # produce_and_flush and shutdown can't hang indefinitely.
        if params.blocked_connection_timeout is None:
            params.blocked_connection_timeout = 30
        return params

    def _open_publisher(self) -> None:
        self._pub_conn = self._pika.BlockingConnection(self._params)
        self._pub_ch = self._pub_conn.channel()
        self._pub_ch.confirm_delivery()

    def _reopen_publisher(self) -> None:
        try:
            self._pub_conn.close()
        except Exception:
            pass
        self._open_publisher()

    def _declare_topology(self) -> None:
        try:
            # Dead-letter exchange + queue.
            self._ch.exchange_declare(self._dlx, exchange_type="fanout", durable=True)
            self._ch.queue_declare(self._dlq, durable=True)
            self._ch.queue_bind(self._dlq, self._dlx)
            # Input queue routes rejected messages to the DLX; results queue.
            self._ch.queue_declare(
                self._input_queue, durable=True, arguments={"x-dead-letter-exchange": self._dlx}
            )
            self._ch.queue_declare(self._results_queue, durable=True)
        except self._pika.exceptions.ChannelClosedByBroker as exc:
            raise RuntimeError(
                "RabbitMQ topology declaration failed — a queue/exchange likely already "
                f"exists with different settings: {exc}. Either align the settings, or "
                "pre-provision them and set rabbitmq_declare_topology=False."
            ) from exc

    def poll(self, timeout: float) -> Message | None:
        method, properties, body = next(self._consume_gen)
        if method is None:  # inactivity timeout — no message this tick
            return None
        return Message(
            source=self._input_queue,
            partition=-1,
            offset=method.delivery_tag,
            key=getattr(properties, "correlation_id", None),
            value=body or b"",
            reply_to=getattr(properties, "reply_to", None),
            raw=method,
        )

    def ack(self, message: Message) -> None:
        self._ch.basic_ack(delivery_tag=message.raw.delivery_tag)

    def nack(self, message: Message, reason: str) -> None:
        # requeue=False + the input queue's x-dead-letter-exchange routes the
        # poison message to the DLX. The `reason` is not carried in the AMQP
        # body (the broker adds an x-death header); kept in the signature for
        # parity with the transport protocol.
        self._ch.basic_nack(delivery_tag=message.raw.delivery_tag, requeue=False)

    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None:
        pika = self._pika
        props = pika.BasicProperties(correlation_id=key, delivery_mode=2)  # persistent
        with self._pub_lock:
            try:
                self._publish(dest, value, props)
            except pika.exceptions.UnroutableError as exc:
                # The destination queue doesn't exist (e.g. a reply_to that was
                # never declared). Reconnecting won't help — surface it so the
                # outbox re-emits later (or the caller logs).
                raise MessagePublishError(f"result unroutable to '{dest}' (no such queue?): {exc}") from exc
            except Exception:
                # The publisher connection may have been dropped (idle heartbeat,
                # broker restart). Rebuild it once and retry.
                try:
                    self._reopen_publisher()
                    self._publish(dest, value, props)
                except pika.exceptions.UnroutableError as exc:
                    raise MessagePublishError(f"result unroutable to '{dest}' (no such queue?): {exc}") from exc
                except Exception as exc:
                    raise MessagePublishError(str(exc)) from exc

    def _publish(self, dest: str, value: bytes, props: Any) -> None:
        # Default exchange routes by routing_key = queue name. mandatory=True +
        # publisher confirms → raises if unroutable/unconfirmed, so we never mark
        # a result published unless a queue actually got it.
        self._pub_ch.basic_publish(
            exchange="", routing_key=dest, body=value, properties=props, mandatory=True
        )

    def close(self) -> None:
        # Best-effort teardown of both connections.
        for closer in (
            self._consume_gen.close,
            self._ch.cancel,
            self._conn.close,
            self._pub_conn.close,
        ):
            try:
                closer()
            except Exception:
                pass


def create_pika_transport(config: Any) -> PikaTransport:
    """Build a PikaTransport from a Hoglah config (rabbitmq_* fields)."""
    return PikaTransport(
        url=config.rabbitmq_url,
        input_queue=config.rabbitmq_input_queue,
        results_queue=config.rabbitmq_results_queue,
        dlx=config.rabbitmq_dlx,
        dlq=config.rabbitmq_dlq,
        prefetch=config.rabbitmq_prefetch,
        declare_topology=config.rabbitmq_declare_topology,
    )
