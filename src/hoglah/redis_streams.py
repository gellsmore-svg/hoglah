"""Redis Streams transport for the messaging bridge (ADR-020).

A `MessageTransport` adapter over `redis` (redis-py), so the broker-neutral
`MessageBridge` (see kafka_bridge.py) can consume job requests from / produce
results to Redis Streams with the same crash-safety guarantees as Kafka and
RabbitMQ. The bridge logic is unchanged; only this adapter is Redis-specific.

How it maps to the crash-safety contract:
  - `ack`  = `XACK` (+ best-effort `XDEL`) after a durable enqueue. A consumed-
    but-unacked entry stays in this consumer's Pending Entries List (PEL).
  - **Crash recovery** — a stable `consumer` name + reading the PEL (`XREADGROUP`
    id `0`) on startup *before* new messages (`>`): an entry read but not acked
    before a crash is re-read and re-processed (idempotent enqueue → no dup).
  - `nack` = `XADD` the poison entry to a dead-letter stream, then `XACK` the
    original (Redis Streams have no broker-side dead-letter; mirrors the Kafka
    adapter). If the `XADD` fails it raises → no ack → the entry stays in the
    PEL → recovered later.
  - egress = `XADD` to the results stream (or the message's reply_to stream). A
    failed `XADD` raises, so the outbox flips only after a durable write.

`redis` is an optional dependency (`pip install "hoglah[redis]"`), imported
lazily. The redis-py client is thread-safe (connection-pool backed), so — unlike
the pika adapter — egress needs no separate connection or lock.
"""

from __future__ import annotations

from typing import Any

from .kafka_bridge import Message, MessagePublishError, dead_letter_envelope


class RedisStreamsTransport:
    """Redis Streams MessageTransport over redis-py."""

    def __init__(
        self,
        *,
        url: str,
        input_stream: str,
        results_stream: str,
        dlq_stream: str,
        group: str,
        consumer_name: str,
        delete_acked: bool = True,
    ):
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "The Redis Streams bridge requires redis. Install with: pip install 'hoglah[redis]'"
            ) from exc

        self._redis = redis
        self._input_stream = input_stream
        self._results_stream = results_stream
        self._dlq_stream = dlq_stream
        self._group = group
        self._consumer = consumer_name
        self._delete_acked = delete_acked
        self._recovered = False  # have we drained our PEL since startup?

        self._r = redis.Redis.from_url(url)  # bytes responses (decode_responses=False)
        # Create the consumer group at id "0" so messages already in the stream
        # at group-creation time are not missed (MKSTREAM creates the stream).
        # The group is server-side and persists across restarts.
        try:
            self._r.xgroup_create(self._input_stream, self._group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # already exists is fine
                raise

    def _to_message(self, entry: tuple[Any, dict[Any, Any]]) -> Message:
        entry_id, fields = entry
        data = fields.get(b"data", b"")
        corr = fields.get(b"correlation_id")
        reply_to = fields.get(b"reply_to")
        return Message(
            source=self._input_stream,
            partition=-1,
            offset=-1,  # Redis ids are strings; carried in `raw`
            key=corr.decode("utf-8", "replace") if corr else None,
            value=data if isinstance(data, bytes) else str(data).encode("utf-8"),
            reply_to=reply_to.decode("utf-8", "replace") if reply_to else None,
            raw=entry_id,
        )

    def _read(self, stream_id: str, block_ms: int) -> tuple[Any, dict[Any, Any]] | None:
        resp = self._r.xreadgroup(
            self._group, self._consumer, {self._input_stream: stream_id}, count=1, block=block_ms
        )
        if not resp:
            return None
        _stream, entries = resp[0]
        return entries[0] if entries else None

    def poll(self, timeout: float) -> Message | None:
        # First drain our own Pending Entries List (entries delivered to this
        # consumer before a crash but never acked), then read new messages.
        if not self._recovered:
            pending = self._read("0", block_ms=0)
            if pending is not None:
                return self._to_message(pending)
            self._recovered = True
        entry = self._read(">", block_ms=int(timeout * 1000))
        return self._to_message(entry) if entry is not None else None

    def ack(self, message: Message) -> None:
        self._r.xack(self._input_stream, self._group, message.raw)
        if self._delete_acked:
            try:  # keep the input stream bounded; best-effort
                self._r.xdel(self._input_stream, message.raw)
            except Exception:
                pass

    def nack(self, message: Message, reason: str) -> None:
        # No broker-side dead-letter in Redis Streams: XADD to the DLQ stream,
        # then XACK. If the XADD raises we do NOT ack → the entry stays in the
        # PEL → recovered on the next startup. The envelope (reason + source +
        # raw body) matches the Kafka adapter's dead-letter shape.
        self._r.xadd(self._dlq_stream, {"data": dead_letter_envelope(message, reason)})
        self._r.xack(self._input_stream, self._group, message.raw)
        if self._delete_acked:
            try:
                self._r.xdel(self._input_stream, message.raw)
            except Exception:
                pass

    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None:
        try:
            self._r.xadd(dest, {"data": value, "correlation_id": key or ""})
        except Exception as exc:
            raise MessagePublishError(str(exc)) from exc

    def close(self) -> None:
        try:
            self._r.close()
        except Exception:
            pass


def create_redis_streams_transport(config: Any) -> RedisStreamsTransport:
    """Build a RedisStreamsTransport from a Hoglah config (redis_* fields)."""
    return RedisStreamsTransport(
        url=config.redis_url,
        input_stream=config.redis_input_stream,
        results_stream=config.redis_results_stream,
        dlq_stream=config.redis_dlq_stream,
        group=config.redis_group,
        consumer_name=config.redis_consumer_name,
        delete_acked=config.redis_delete_acked,
    )
