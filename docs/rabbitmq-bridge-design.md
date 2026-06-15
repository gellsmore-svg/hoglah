# Design Proposal: RabbitMQ Bridge (+ transport generalization)

**Status:** **Implemented (ADR-019).** Phase 1 = v0.5.2 (transport generalization);
Phases 2–4 = v0.6.0 (the RabbitMQ adapter + config/CLI + gated tests).
**Date:** 2026-06-15
**Author:** drafted by Claude from the "next messaging service" discussion.
**Builds on:** ADR-018 + `docs/kafka-bridge-design.md` (the Kafka bridge, shipped v0.5.0/0.5.1).

> **Done.** Phase 1 (v0.5.2): the §4 generalization —
> `KafkaTransport`→`MessageTransport` (`commit`→`ack` + new `nack`),
> `KafkaBridge`→`MessageBridge`, `KafkaMessage`→`Message`; poison path moved into
> the transport. Phases 2–4 (v0.6.0): `PikaTransport` (`src/hoglah/rabbitmq.py`),
> `rabbitmq_*` config + `hoglah rabbitmq-bridge` CLI + `hoglah[rabbitmq]` extra,
> and gated (`RUN_RABBITMQ_TESTS=1`) real-broker round-trip + poison→DLQ tests
> (`tests/test_rabbitmq.py`) — run green against RabbitMQ 3.
>
> §13 decisions as built: **pika** (BlockingConnection); publisher thread-safety
> = **dedicated publisher connection + lock**; `correlation_id`/`reply_to` read
> from AMQP properties on the verify side, body fields on the contract side;
> **at most one bridge per instance** (Kafka wins if both enabled, with a
> warning) — per-broker `*_enabled` bools kept (no `messaging_backend` selector
> yet); topology declared on startup unless `rabbitmq_declare_topology=False`;
> prefetch default **1**.

---

## 1. Summary

Add RabbitMQ (AMQP 0-9-1) as a second messaging bridge, alongside Kafka. As with
Kafka, Hoglah acts as a **lightweight bridge** — it consumes job-request messages
from an input queue, enqueues them into the existing `JobStore`, processes them
with the normal serial worker, and publishes result messages back — **without
owning or managing the RabbitMQ cluster**.

The crash-safety machinery built for Kafka (idempotent ingress, transactional
outbox, dead-letter routing) and the `JobStore` primitives it relies on
(`correlation_id`-keyed idempotent `enqueue`, `result_published`,
`list_unpublished_terminal`) are **already broker-agnostic**. The only
Kafka-specific code is the `KafkaTransport` adapter. So this proposal is really
two things:

1. **A small generalization** of the transport seam so any broker can plug in.
2. **A RabbitMQ adapter** on that seam.

RabbitMQ is the recommended next broker because its **per-message acknowledgement**
model maps to our crash-safety contract *more cleanly than Kafka's offset model*,
and because it reaches the large population of teams who run a message broker but
not a Kafka cluster.

---

## 2. Glossary (AMQP / RabbitMQ terms)

Defined here because they recur; skip if familiar.

- **Exchange** — the routing component. Producers publish to an *exchange*, never
  directly to a queue.
- **Queue** — holds messages; consumers read from queues.
- **Binding / routing key** — a binding links an exchange to a queue; the
  *routing key* on a message is what the exchange matches to route it.
- **Ack (`basic.ack`)** — a consumer's confirmation that it has finished a
  message; the broker then drops it. The AMQP analogue of a Kafka offset commit,
  but **per message**.
- **Nack / reject (`basic.nack`)** — a consumer declining a message. With
  `requeue=false` and a dead-letter exchange configured, the broker routes the
  message to the DLX. No separate "produce to a DLT topic" step.
- **Dead-letter exchange (DLX)** — an exchange that a queue forwards
  rejected/expired messages to; bound to a dead-letter queue.
- **Prefetch / QoS (`basic.qos`)** — the max number of unacknowledged messages a
  consumer may hold at once. Backpressure: set it small so a slow worker doesn't
  hoard the queue.
- **Publisher confirms** — the broker's acknowledgement that it durably accepted
  a *published* message. This is the egress ack our transactional outbox needs.
- **Competing consumers** — multiple consumers on one queue share the work; the
  broker pushes each message to one of them. The analogue of a Kafka consumer
  group.
- **Durable queue + persistent message** — survive a broker restart.
- **`correlation_id` / `reply_to`** — *native AMQP message properties* (the RPC
  pattern uses exactly these), which line up perfectly with our message contract.

---

## 3. Why RabbitMQ fits our crash-safety contract better than Kafka

Our guarantees need four things from a broker (see kafka design §6): (1) ack a
message only *after* a durable enqueue, (2) redelivery of un-acked messages on
crash, (3) a producer confirm so the outbox flips only after the result lands,
(4) dead-letter support. RabbitMQ provides all four, and two of them are *nicer*
than Kafka's:

| Concern | Kafka (shipped) | RabbitMQ (proposed) |
|---|---|---|
| Ingress ack | Commit a **positional offset**; a stuck message can head-of-line-block its partition | **Per-message `basic.ack`** — one bad/slow message never blocks its neighbours |
| Poison handling | Manually produce to a DLT topic, *then* commit — and if that produce fails we must not commit (the v0.5.1 Critical fix) | **`basic.nack(requeue=false)` → DLX**, a single broker-side routing op; no separate produce to confirm, so that whole failure class largely disappears |
| Egress confirm | Idempotent producer + `acks=all` | **Publisher confirms** |
| Redelivery | Consumer-group rebalance redelivers uncommitted offsets | Un-acked messages auto-redeliver to another competing consumer |
| Scaling | Consumer group over partitions | Competing consumers over one queue |
| `correlation_id` / `reply_to` | Carried in the JSON payload | Available as **native AMQP properties** (payload still works as fallback) |

Net: the RabbitMQ adapter is *simpler* than the Kafka one on the two paths that
caused the most subtlety (poison handling and head-of-line blocking), while
preserving the same exactly-once-*effect* guarantee.

---

## 4. The enabling refactor: `MessageTransport` / `MessageBridge`

Today: `KafkaTransport` (protocol), `ConfluentKafkaTransport`, `FakeKafkaTransport`,
`KafkaBridge`, `KafkaMessage`. Generalize to broker-neutral names and one new
operation:

```python
class MessageTransport(Protocol):
    def poll(self, timeout: float) -> Message | None: ...
    def ack(self, message: Message) -> None: ...       # was commit()
    def nack(self, message: Message) -> None: ...       # NEW — poison/dead-letter route
    def produce_and_flush(self, dest: str, key: str | None, value: bytes, timeout: float = 10.0) -> None: ...
    def close(self) -> None: ...
```

Key change — **the poison path moves into the transport**:

- The bridge stops doing "produce to a DLT *topic* + commit." Instead, on a
  poison message it calls `transport.nack(msg)` and the adapter does the
  broker-appropriate thing:
  - **Kafka adapter:** `nack` = produce the message to the DLT topic, then commit
    the offset (exactly today's behaviour, including the v0.5.1 "only commit on a
    confirmed DLT write" rule — failure to dead-letter leaves the offset
    uncommitted).
  - **RabbitMQ adapter:** `nack` = `basic.nack(delivery_tag, requeue=false)` → the
    broker routes to the DLX. One call, broker-side.
- `commit(message)` → `ack(message)`. The Kafka adapter implements `ack` as the
  positional offset commit it does now.

`KafkaBridge` becomes `MessageBridge` (broker-neutral orchestration:
`prime`/`start`/`stop`, `_handle_message`, `_publish_now`, `republish_unpublished`,
the in-flight-egress drain). `KafkaMessage` → `Message`. The store primitives and
the bridge logic are **unchanged in behaviour** — only renamed and with the
poison path delegated to `nack`.

**Backward compatibility:** the public surface stays. `kafka_enabled`, the
`kafka_*` config, the `hoglah kafka-bridge` CLI, and ADR-018 semantics are all
preserved; `KafkaBridge`/`KafkaTransport` can remain as thin aliases if anything
imports them. The generic types are internal.

**Testing payoff:** the existing `FakeKafkaTransport` becomes `FakeTransport`, and
the whole crash-scenario suite (idempotent ingress, enqueue-then-ack ordering,
crash/redeliver, poison→dead-letter, outbox re-emit, ack-then-mark) runs
unchanged against *both* adapters. Each new broker is then ~one adapter file + a
gated real-broker round-trip.

---

## 5. Architecture & data flow

```
                  ┌──────────────────── Hoglah instance ───────────────────┐
 producers   ──►  │  ┌──────────────┐  enqueue      ┌──────────┐            │
 publish to       │  │ AMQP consumer │ (idempotent  │ JobStore │  _deliver()│
 `hoglah-jobs`    │  │ thread        ├──by corr_id)─►│ SQLite / │◄─┐ on      │
 queue            │  │ consume→      │               │ Mongo    │  │ terminal│
                  │  │ enqueue→ACK   │               └────┬─────┘  │         │
                  │  │ (poison→NACK  │                    │ claim  │         │
                  │  │  →DLX)        │              ┌─────▼─────┐  │         │
                  │  └──────────────┘              │ serial     │  │         │
                  │                                │ worker     ├──┘ set_result
                  │  ┌──────────────┐  publish     │ (unchanged)│            │
 consumers   ◄──  │  │ AMQP publisher│◄────────────┴────────────┘            │
 read `hoglah-    │  │ (confirms)    │   result → results queue or reply_to, │
  results` /      │  └──────────────┘   echoing correlation_id               │
  reply_to        └─────────────────────────────────────────────────────────┘
                         poison → DLX → `hoglah-jobs-dlq`
```

1. A producer publishes a job request to the **input queue** (default
   `hoglah-jobs`, durable).
2. The **consumer thread** receives it (manual ack, `auto_ack=false`),
   deserialises/validates, **idempotently enqueues** it (§6), then **`basic.ack`**.
   A poison message is **`basic.nack(requeue=false)`** → routed to the DLX.
3. The **serial worker** processes the job exactly as today.
4. On terminal status the **`_deliver()` hook** publishes the result (with
   **publisher confirms**) to the results queue or the message's `reply_to`,
   echoing `correlation_id`, and marks it published only after the confirm.

---

## 6. Reliability & delivery semantics (same contract, AMQP mechanics)

- **Ingress — idempotent consumer.** `auto_ack=false`; ack *only after* a durable,
  idempotent `enqueue(correlation_id=…)`. Crash between enqueue and ack → the
  message is un-acked → broker redelivers (to this or another competing consumer)
  → idempotent enqueue is a no-op. No loss, no duplicate. Set **prefetch small**
  (default 1, matching the serial worker) so a crashed/slow consumer doesn't sit
  on a backlog of unacked messages.
- **Poison — native dead-letter.** Parse/validate failure → `basic.nack(requeue=
  false)`; the broker routes to the DLX → dead-letter queue. If the connection
  drops before the nack is processed, the message is simply redelivered and
  re-nacked — so, unlike the Kafka path, there is **no "DLT write failed but we
  committed" window** to guard against.
- **Egress — transactional outbox.** Publish with **publisher confirms**; wait for
  the confirm, *then* `mark_result_published`. Crash between confirm and mark →
  `republish_unpublished()` re-emits on startup. AMQP has no producer-side
  dedup, so a re-emit is a duplicate message — de-duped downstream by
  `correlation_id`, giving exactly-once *effect* (identical contract to Kafka).
- **`correlation_id` / `reply_to`** are read from the **native AMQP properties**
  when present, falling back to the JSON body fields — keeping the cross-broker
  message contract identical (§7) while using AMQP idioms.

---

## 7. Message contracts

**Unchanged from the Kafka bridge** so producers are broker-agnostic. JSON body
(input → `JobRequest`, output ← `JobResult`) exactly as in
`docs/kafka-bridge-design.md` §5. The only AMQP-specific nuance: `correlation_id`
and `reply_to` may be supplied as AMQP message *properties* instead of (or in
addition to) body fields; properties win when both are set.

Input (body): `{ correlation_id, model, prompt|messages, kind?, options?, tags?,
reply_to?, metadata? }`. Output (body): `{ correlation_id, job_id, status, model,
output, embedding, embedding_dim, error, truncated, truncation_reason, tags,
parent_job_id, timings, metadata }`.

---

## 8. Topology & threading

- **Topology declaration.** On startup, optionally declare (idempotently) the
  durable input queue, the results queue, the DLX, and the dead-letter queue with
  the input queue's `x-dead-letter-exchange` argument set. Make declaration
  switchable off for locked-down clusters where the operator pre-provisions
  everything (open decision §13.5).
- **Threading — note the pika caveat.** Unlike `confluent-kafka`'s thread-safe
  producer, **pika channels/connections are not thread-safe.** The consumer runs
  on its own thread (as today). Egress publishes happen from per-job daemon
  threads (the ADR-015 pattern), so they must NOT share the consumer's channel.
  Plan: a **dedicated publisher connection + channel guarded by a lock** (or
  publish via the consumer connection using `connection.add_callback_threadsafe`).
  This is the main adapter-specific complexity and is called out as open
  decision §13.2.
- The asyncio worker loop is untouched; `_deliver` stays non-blocking (egress on
  a daemon thread).

---

## 9. Configuration & optional dependency

Mirror the Kafka/Mongo pattern: optional extra, lazy import, `HOGLAH_`-prefixed
env + constructor config. Off unless enabled.

- **Optional extra:** `pip install "hoglah[rabbitmq]"` (adds `pika`), lazy-imported.
- **New config fields:**
  - `rabbitmq_enabled: bool = False`
  - `rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"`
  - `rabbitmq_input_queue: str = "hoglah-jobs"`
  - `rabbitmq_results_queue: str = "hoglah-results"`
  - `rabbitmq_dlx: str = "hoglah-dlx"` (+ `rabbitmq_dlq: str = "hoglah-jobs-dlq"`)
  - `rabbitmq_prefetch: int = 1`
  - `rabbitmq_declare_topology: bool = True`
- **Exactly one bridge per instance** for v1: if both `kafka_enabled` and
  `rabbitmq_enabled` are set, the client logs a warning and uses the first (or
  errors — open decision §13.4). A future `messaging_backend` selector could
  consolidate this (§13.4).

---

## 10. CLI

- `hoglah rabbitmq-bridge` — worker + RabbitMQ bridge in the foreground (the AMQP
  analogue of `hoglah kafka-bridge`); `--url`, `--input-queue`, `--results-queue`,
  `--prefetch`, `--backend`. Requires `pip install "hoglah[rabbitmq]"`.

---

## 11. Relationship to existing decisions

- **ADR-018 / kafka design** — this generalizes that bridge; the Kafka adapter and
  its public surface are preserved.
- **ADR-014 / ADR-015** — Kafka and RabbitMQ egress are both `_deliver()` sinks;
  all of output-folder, HTTP-callback, and a message bridge can be on at once.
- **ADR-016** — recovery stays a worker responsibility; the bridge only enqueues.
- **ADR-017** — sharing one MongoDB store across competing consumers gives
  fleet-wide exactly-once execution via the server-side atomic claim (same as the
  Kafka §8 story).

---

## 12. Non-goals (v1 of the RabbitMQ bridge)

- Hoglah does **not** manage the RabbitMQ cluster (vhosts, users, policies,
  quorum/mirrored-queue config are the operator's infra).
- **No AMQP transactions (`tx.*`)** — publisher confirms are lighter and
  sufficient.
- **No exactly-once via the broker** — exactly-once *effect* via `correlation_id`
  dedup, as with Kafka.
- **JSON only** — no schema registry / content-type negotiation yet.
- **No RPC-server framing beyond `reply_to`** — we honour the `reply_to` property
  but don't impose a full RPC convention.

---

## 13. Open decisions (→ ADR-019+ on approval)

1. **Client library.** Recommend **`pika`** (synchronous `BlockingConnection`,
   the de-facto standard, fits the dedicated-consumer-thread model). Alternative:
   **`aio-pika`** (asyncio) — only attractive if we later move the consumer onto
   the worker's event loop, which we deliberately don't.
2. **Publisher thread-safety** (the real adapter complexity): dedicated publisher
   connection + lock, a small publisher-channel pool, or
   `add_callback_threadsafe` onto the consumer connection. Recommend the
   dedicated-connection-+-lock approach for simplicity.
3. **`reply_to` / `correlation_id` source** — AMQP property first, JSON body
   fallback (recommended), vs body-only (consistent with Kafka but ignores AMQP
   idioms).
4. **Multiple bridges per instance** — forbid / pick-first-enabled / allow-many.
   And whether to introduce a unified `messaging_backend` selector now or keep
   per-broker `*_enabled` bools (back-compat with the released `kafka_enabled`).
5. **Topology declaration** — declare durable queues + DLX on startup
   (idempotent, recommended default) vs assume operator pre-declares (for
   locked-down clusters). Exposed as `rabbitmq_declare_topology`.
6. **Prefetch default** — `1` (strict serial backpressure) vs a small N for
   pipelining ingest ahead of the worker.

---

## 14. Suggested phased implementation (once approved)

1. **Refactor (no behaviour change):** `KafkaTransport`→`MessageTransport`
   (+`ack`/`nack`), `KafkaBridge`→`MessageBridge`, `KafkaMessage`→`Message`; move
   the poison path to `transport.nack`; keep Kafka adapter + public surface +
   the full test suite green. Ship as a patch (e.g. v0.5.2, internal-only).
2. **RabbitMQ adapter:** `PikaTransport` (consume w/ manual ack, idempotent
   enqueue, `nack`→DLX poison, publisher-confirm egress, optional topology
   declaration, publisher-connection threading).
3. **Surface:** `rabbitmq_*` config, `hoglah rabbitmq-bridge` CLI,
   `hoglah[rabbitmq]` extra.
4. **Tests:** the shared `FakeTransport` crash suite already covers it; add a
   gated (`RUN_RABBITMQ_TESTS=1`) real-broker round-trip (RabbitMQ via Docker or
   a local install), plus an AMQP-property `correlation_id`/`reply_to` test.

Each phase is independently shippable. Phase 1 (the generalization) is the
high-value, low-risk groundwork that makes RabbitMQ — and later Redis Streams /
SQS — nearly free.
