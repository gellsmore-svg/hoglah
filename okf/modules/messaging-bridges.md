---
type: Module
title: Messaging bridges (worker side)
description: The worker-side MessageBridge over a broker-neutral MessageTransport — consumes job-request messages from Kafka/RabbitMQ/Redis, enqueues them durably, and publishes results back, all crash-safe.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/kafka_bridge.py
tags: [hoglah, messaging, kafka, rabbitmq, redis, adr-018, adr-019, adr-020]
timestamp: 2026-06-19T00:00:00Z
---

# Messaging bridges (`kafka_bridge.py`, `rabbitmq.py`, `redis_streams.py`)

The **worker side** of broker integration. A broker-neutral `MessageBridge`
**consumes** job-request messages from an input topic/queue/stream, enqueues them
into the [store](storage.md) (the source of truth — brokers are *transport, not
storage*, ADR-018), executes them, and **produces** a result message to the
request's `reply_to` (or the results destination), keyed by `correlation_id`.

The broker is abstracted behind a `MessageTransport` protocol (`poll` / `ack` /
`nack` / `produce_and_flush`), with three implementations:

- **`ConfluentKafkaTransport`** (`kafka_bridge.py`, ADR-018) — `confluent-kafka`;
  `pip install "hoglah[kafka]"`.
- **`PikaTransport`** (`rabbitmq.py`, ADR-019) — AMQP via `pika`; per-message
  `basic_ack` + dead-letter-exchange; `pip install "hoglah[rabbitmq]"`.
- **`RedisStreamsTransport`** (`redis_streams.py`, ADR-020) — Redis Streams;
  consumer-group + Pending-Entries-List recovery; `pip install "hoglah[redis]"`.

All three get [crash safety](../concepts/crash-safety.md) for free from the shared
bridge (idempotent consumer + transactional outbox). At most one broker per
instance (precedence kafka > rabbitmq > redis). Run one with the
[`*-bridge` CLI](../cli/daemons.md). Messages are parsed/built by shared helpers
(`parse_input_message` / `build_result_message`) so the submitter side agrees on
the wire format — see the [messaging submitter](messaging-submitter.md).
