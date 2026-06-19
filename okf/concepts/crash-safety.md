---
type: Concept
title: Crash safety (idempotent consumer + transactional outbox)
description: The messaging bridges are crash-safe by construction — ingress acks only after a durable idempotent enqueue keyed on correlation_id, and egress marks a result published only after a broker ack, giving exactly-once effect end to end.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/docs/kafka-bridge-design.md
tags: [hoglah, crash-safety, exactly-once, outbox, adr-018]
timestamp: 2026-06-19T00:00:00Z
---

# Crash safety

The [messaging bridges](../modules/messaging-bridges.md) (Kafka/RabbitMQ/Redis)
are crash-safe **by construction**, not best-effort. Two patterns combine:

- **Ingress — idempotent consumer.** A consumed job-request message is acked
  (Kafka offset commit / AMQP `basic_ack` / Redis `XACK`) only **after** a durable
  enqueue into the [store](../modules/storage.md), keyed on the message's
  `correlation_id` (a UNIQUE index). A redelivery in the enqueue→ack window is a
  harmless no-op: at-least-once delivery + idempotent enqueue = no loss, no dup.
- **Egress — transactional outbox.** A terminal result is marked published
  (`result_published`) only **after** the broker acks the produce. On startup,
  `republish_unpublished()` re-emits any computed-but-unpublished results. With an
  idempotent producer + consumer-side `correlation_id` de-dup, this is
  **exactly-once *effect*** end to end.

**Poison messages** go to a dead-letter destination and are then acked, so a bad
message never blocks a partition/queue/stream. Each broker maps the contract
slightly differently (RabbitMQ's per-message ack + DLX is the cleanest; Kafka and
Redis emulate dead-lettering with a dead-letter topic/stream) — see the
[messaging bridges](../modules/messaging-bridges.md) module and the design docs
[`kafka-bridge-design.md`](https://github.com/gellsmore-svg/hoglah/blob/main/docs/kafka-bridge-design.md)
/ [`rabbitmq-bridge-design.md`](https://github.com/gellsmore-svg/hoglah/blob/main/docs/rabbitmq-bridge-design.md).

The crash scenarios are unit-tested with a broker-neutral in-memory `FakeTransport`,
plus gated real-broker round-trips (`RUN_{KAFKA,RABBITMQ,REDIS}_TESTS=1`).
