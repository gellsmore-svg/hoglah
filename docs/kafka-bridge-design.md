# Design: Kafka Bridge (transport adapter)

**Status:** **Accepted + implemented in v0.5.0** (ADR-018).
**Date:** 2026-06-15

> Implemented per this design. Decisions from §13: client library =
> `confluent-kafka`; idempotency key = a separate UNIQUE `correlation_id`
> column with an internal UUID `job_id`; DLT topic name is configurable
> (`kafka_dlt_topic`); security/auth surface kept thin for now. The phased plan
> (§14) was delivered together: egress, ingress, CLI, and a gated real-broker
> test. Code: `src/hoglah/kafka_bridge.py`; tests: `tests/test_kafka_bridge.py`.

---

## 1. Summary

Let Hoglah act as a **lightweight bridge** between an existing Apache Kafka
deployment and its own durable job queue:

- **Ingress:** consume job-request messages from a Kafka input topic →
  enqueue them as normal Hoglah jobs in the existing `JobStore`
  (SQLite or MongoDB).
- **Processing:** the existing serial asyncio worker runs the LLM inference
  exactly as it does today — unchanged.
- **Egress:** on terminal status (completed / failed / cancelled), produce a
  result message back to Kafka (a results topic, or a per-request `reply_to`
  topic), tagged with the original `correlation_id`.

Hoglah does **not** own, configure, or become the hub of the Kafka cluster. It
is one more producer/consumer in an existing ecosystem, enabled or disabled by
config, with the SQLite/Mongo backends untouched.

---

## 2. Glossary (Kafka terms used below)

Defined here because they recur throughout; skip if familiar.

- **Kafka** — a distributed, append-only **commit log** used as a messaging
  backbone. Unlike a database it is optimised for sequential append + replay,
  *not* random lookup or in-place update.
- **Topic** — a named stream of messages (e.g. `hoglah-jobs`).
- **Partition** — a topic is split into partitions; ordering is guaranteed
  *only within* a partition, never across them.
- **Offset** — a message's sequential position within a partition. A consumer
  *commits* an offset to record "I have processed up to here."
- **Producer** — a client that appends messages to a topic.
- **Consumer** — a client that reads messages from a topic.
- **Consumer group** — a set of consumers that share the work: Kafka assigns
  each partition to exactly one member, so adding members scales throughput.
  Re-assignment when members join/leave is called a **rebalance**.
- **Dead-letter topic (DLT)** — a side topic where un-processable ("poison")
  messages are parked so they don't block the partition.
- **`correlation_id`** — a caller-supplied unique id echoed back on the
  response, so an asynchronous caller can match a result to its request.
- **`reply_to`** — a caller-supplied topic name telling Hoglah where to send
  *this* request's result (request–reply pattern).

---

## 3. The key architectural decision: transport adapter, **not** a storage backend

The original sketch contained a tension: most of it describes Kafka as a
*bridge in front of* the existing queue, but one bullet describes "a new Kafka
backend that implements the same internal queue interface." Those are different
things, and only the first is viable:

| Role | Viable? | Why |
|------|---------|-----|
| Kafka as a **transport adapter** (ingress + egress around the JobStore) | ✅ **Yes — this proposal** | Kafka does what it's good at (durable async messaging); SQLite/Mongo keep doing state, lookup, and atomic claim. |
| Kafka as a **`JobStore` backend** (implements `enqueue/get/list/claim/set_result/…`) | ❌ No | The `JobStore` contract needs `get(job_id)`, `claim_for_processing` (atomic status flip), `list(status=…)`, `get_status_counts`, `delete_job`. A Kafka log has no keyed random read, no in-place update, no secondary-index query. Emulating them means building a materialised view (compacted topic + Kafka Streams state store / ksqlDB) — i.e. re-implementing a database, which directly contradicts "keep SQLite/Mongo untouched." |

**Decision: Kafka is a transport adapter.** It is the conceptual sibling of the
existing decoupled-delivery mechanisms — output-folder (ADR-014) and outbound
HTTP callback (ADR-015) — extended to cover *ingress* as well as egress. The
`JobStore` (SQLite default, Mongo option) remains the single source of truth for
job state.

---

## 4. Architecture & data flow

```
                  ┌──────────────────────── Hoglah instance ───────────────────────┐
                  │                                                                 │
  external        │   ┌──────────────┐   enqueue    ┌────────────┐   _deliver()     │
  producers  ──►  │   │ Kafka         │  (idempotent │ JobStore   │   on terminal    │
  (hoglah-jobs)   │   │ consumer       ├────────────►│ SQLite /   │◄──┐ status        │
                  │   │ thread         │  by corr_id) │ Mongo      │   │               │
                  │   │ poll→enqueue   │              └─────┬──────┘   │               │
                  │   │ →commit offset │                    │ claim    │               │
                  │   └──────────────┘              ┌──────▼──────┐   │               │
                  │                                 │ asyncio      │   │               │
                  │                                 │ worker       ├───┘ set_result    │
                  │                                 │ (serial,     │                   │
                  │                                 │  unchanged)  │                   │
                  │                                 └──────┬──────┘                   │
                  │                                        │ terminal result          │
                  │   ┌──────────────┐   produce          │                          │
  external    ◄── │   │ Kafka         │◄──────────────────┘  (results topic or        │
  consumers       │   │ producer      │                       reply_to, + corr_id)    │
  (hoglah-results │   └──────────────┘                                                │
   / reply_to)    └─────────────────────────────────────────────────────────────────┘
```

Flow, step by step:

1. An external producer sends a job request to the **input topic** (default
   `hoglah-jobs`).
2. Hoglah's **consumer thread** polls, deserialises, validates, and **enqueues
   the job into the `JobStore`** (idempotently — see §6), then **commits the
   offset**. It does *not* run inference and does *not* block on the worker.
3. The existing **serial asyncio worker** claims and processes the job exactly
   as today (`claim_for_processing` → adapter → `set_result`).
4. On terminal status the existing **`_deliver()` hook** fires a **Kafka
   producer** that emits the result to the **results topic** (default
   `hoglah-results`) or to the request's `reply_to`, always carrying the
   original `correlation_id`.

---

## 5. Message contracts

JSON to start (see §12 for why not Avro/schema-registry yet). UTF-8 encoded.

### 5.1 Input message (→ `JobRequest`)

```jsonc
{
  "correlation_id": "9b1c…",        // REQUIRED, unique; doubles as idempotency key
  "model": "gemma3:1b",             // REQUIRED
  "prompt": "…",                    // prompt OR messages (chat), per ADR-004
  "messages": [ /* … */ ],          // optional, mutually exclusive with prompt
  "kind": "generate",               // "generate" | "embed" (ADR-013), default generate
  "options": { "temperature": 0.7, "num_ctx": 4096, /* … */ },
  "tags": ["kafka", "team-x"],      // optional
  "reply_to": "team-x-replies",     // optional; overrides the default results topic
  "metadata": { /* opaque, echoed back */ }
}
```

Mapping: `correlation_id` and `reply_to` are stored in `JobRequest.metadata`
(so the worker/`_deliver()` can read them on completion); the rest maps onto the
existing `JobRequest` fields. **The Kafka message key SHOULD be the
`correlation_id`** so all messages for one logical request land on the same
partition (ordering) and so the idempotency key is also the partition key.

### 5.2 Output message (from `JobResult`)

```jsonc
{
  "correlation_id": "9b1c…",        // echoed from the request
  "job_id": "…",                    // Hoglah's internal id
  "status": "completed",            // completed | failed | cancelled
  "model": "gemma3:1b",
  "output": "…",                    // for generate; null for embed
  "embedding": [ /* floats */ ],    // for embed; null otherwise (ADR-013)
  "embedding_dim": 1024,
  "error": null,                    // populated on failure
  "truncated": false,               // ADR-009 truncation reporting
  "timings": { /* queued/started/finished */ },
  "metadata": { /* echoed */ }
}
```

This is the same payload already produced for ADR-014 (output folder) and
ADR-015 (HTTP callback); the Kafka producer is just a third sink for it.

---

## 6. Reliability & delivery semantics — **the load-bearing section**

This is what the original sketch left unspecified and what determines whether
the bridge loses or duplicates jobs.

### 6.1 Consumer offset commit: manual, after durable enqueue

- **Disable Kafka auto-commit.** Auto-commit advances the offset on a timer,
  independent of whether the job actually reached the `JobStore` — that loses
  jobs on crash.
- The consumer commits the offset **only after** the job is durably written to
  the `JobStore`. Order: `poll → validate → enqueue (fsync'd) → commit`.

This gives **at-least-once** delivery into the queue: a crash between enqueue
and commit re-delivers the message on restart.

### 6.2 Idempotent enqueue → exactly-once *effect*

At-least-once redelivery would create **duplicate jobs**. We make enqueue
idempotent by using `correlation_id` as the deterministic job identity:

- **SQLite:** `INSERT … ON CONFLICT(id) DO NOTHING` (or a UNIQUE index on a
  `correlation_id` column); a redelivered message is a no-op.
- **Mongo:** `_id = correlation_id` (or a unique index on it); a duplicate
  `insert_one` raises `DuplicateKeyError`, which the bridge treats as "already
  enqueued, fine" and commits the offset.

Net effect: **each `correlation_id` is processed at most once**, even though the
Kafka layer is at-least-once. This is the standard "idempotent consumer" pattern
and avoids the complexity of Kafka's transactional exactly-once semantics (EOS),
which we explicitly defer (§12).

### 6.3 Poison messages → dead-letter topic

A message that cannot be deserialised or fails validation must not block its
partition forever (a consumer that never commits re-reads the same bad message
on every poll). Policy:

- On unrecoverable deserialisation/validation error: log it, **produce the raw
  message + error reason to a dead-letter topic** (default
  `hoglah-jobs-dlt`), then **commit the offset** and move on.

### 6.4 Producer (egress) durability

- Result production uses `acks=all` and the producer's built-in retries so a
  transient broker hiccup doesn't drop a result.
- Egress is **best-effort with the same fallback discipline as ADR-014/015**: a
  Kafka produce failure logs a warning and (if output-folder/HTTP callback are
  also configured) leaves those as the durable fallback — it never alters the
  persisted terminal job status.

---

## 7. Threading & integration with the existing worker

- The Kafka **consumer runs in its own dedicated thread** (the same pattern as
  the ADR-015 callback delivery daemon thread), *not* on the asyncio worker
  loop. Rationale: it must poll frequently. If the poll loop were ever blocked
  on slow LLM inference, Kafka's `max.poll.interval.ms` would expire and the
  broker would evict the consumer and **rebalance the group** — pathological.
  Decoupling ingest (fast: poll→enqueue→commit) from processing (slow: the
  worker) is mandatory, not optional.
- The **producer (egress)** is invoked from the existing `_deliver()` terminal
  hook. It may share one long-lived producer instance (thread-safe in the
  common client libraries).
- This means the "or directly hand it to the worker if keeping the adapter
  minimal" option from the sketch is **rejected**: always go through the
  `JobStore`. Going through the store is what preserves restart recovery
  (ADR-016), the serial-access guarantee, status tracking, and fast offset
  commits.

---

## 8. Scaling, ordering, partitioning

- **Horizontal scale:** run N Hoglah instances in one consumer group; Kafka
  hands each a subset of partitions. Each instance keeps its own **serial**
  worker (concurrency=1 default), so the fleet does N jobs concurrently while
  any single LLM instance stays serial — exactly the stated goal.
- **Exactly-once execution across the fleet:** if the N instances share **one
  MongoDB `JobStore`**, the v0.4.0 server-side atomic claim already guarantees
  each job runs once even if (via redelivery/rebalance) it were enqueued from
  two instances with the same `correlation_id` → same `_id` → second insert is a
  no-op. This is where the Kafka bridge and the Mongo backend compose neatly. A
  per-instance SQLite store also works as long as Kafka partitioning keeps a
  given `correlation_id` on one instance.
- **Ordering:** Kafka orders only within a partition. Keying by
  `correlation_id` keeps a logical request's messages ordered; cross-request
  ordering is neither guaranteed nor needed (jobs are independent).

---

## 9. Configuration & optional dependency

Mirror the Mongo pattern exactly (ADR-017): optional extra, lazy import,
`HOGLAH_`-prefixed env + constructor config.

- **Optional extra:** `pip install "hoglah[kafka]"`, lazy-imported so non-Kafka
  users never need the client library and it stays off the default path.
- **New config fields** (all optional; bridge is off unless enabled):
  - `kafka_enabled: bool = False`
  - `kafka_bootstrap_servers: str` (e.g. `"broker1:9092,broker2:9092"`)
  - `kafka_input_topic: str = "hoglah-jobs"`
  - `kafka_results_topic: str = "hoglah-results"`
  - `kafka_dlt_topic: str = "hoglah-jobs-dlt"`
  - `kafka_group_id: str = "hoglah"`
  - `kafka_security_*` (SASL/TLS passthrough) — thin, defer specifics.
- Enabling the bridge is orthogonal to the storage backend: any
  `{sqlite, mongo} × kafka` combination is valid.

---

## 10. CLI

- `hoglah kafka-bridge` — foreground process that runs the consumer + the worker
  (the Kafka analogue of `hoglah run`). Suitable as a container entrypoint /
  systemd unit for a dedicated bridge node.
- `hoglah run` could gain a `--kafka` flag for the combined case, but a distinct
  subcommand keeps the surface clear.

---

## 11. Relationship to existing decisions

- **ADR-004** (prompt vs messages) — input message supports both.
- **ADR-009** (truncation reporting) — surfaced in the output message.
- **ADR-013** (embedding jobs) — `kind: "embed"` supported end-to-end.
- **ADR-014 / ADR-015** (output folder / HTTP callback) — Kafka egress is a
  third delivery sink on the same `_deliver()` hook; all three can be on at once
  and act as each other's fallback.
- **ADR-016** (recovery is a worker responsibility) — unchanged; the bridge only
  enqueues.
- **ADR-017** (Mongo backend) — composes with the bridge to give fleet-wide
  exactly-once (§8).

---

## 12. Non-goals (V1 of the bridge)

Kept deliberately small, in the spirit of the existing V1 non-goals:

- **Hoglah does not own/operate the Kafka cluster.** Topics, partitions, ACLs,
  retention are the existing platform's concern.
- **No Avro / Protobuf / Schema Registry yet** — JSON first. Schema Registry can
  be added later behind the same message contract.
- **No Kafka transactional exactly-once-semantics (EOS) producer/consumer** —
  the idempotent-consumer pattern (§6.2) achieves exactly-once *effect* far more
  simply. Revisit only if a concrete need appears.
- **No Kafka Streams / ksqlDB inside Hoglah** — those are consumer-side concerns
  for downstream systems building views off `hoglah-results`; out of scope here.
- **No consuming as a `JobStore` backend** (§3).

---

## 13. Open decisions (→ become ADR-018+ on approval)

1. **Client library.** Recommendation: **`confluent-kafka`** (wraps the C
   `librdkafka` — robust, fast, the de-facto production choice; needs a wheel
   but they exist for common platforms). Alternative: **`aiokafka`** (pure-python
   asyncio, would integrate with the worker's event loop) or **`kafka-python`**
   (pure-python, lighter, but less actively maintained). The dedicated-thread
   design (§7) means we do *not* need an asyncio-native client, which argues for
   `confluent-kafka`'s reliability. **Needs a decision.**
2. **Idempotency key column/field** — confirm `correlation_id` becomes the
   Hoglah `job_id` directly, vs a separate unique `correlation_id` field with an
   internal UUID job_id. (Former is simplest; latter decouples external id from
   internal id.)
3. **DLT default-on vs require explicit `kafka_dlt_topic`** — auto-create
   behaviour depends on cluster policy.
4. **Security/auth surface** — how much SASL/TLS config to expose vs accept a
   raw librdkafka config dict passthrough.

---

## 14. Suggested phased implementation (once approved)

1. **Phase 1 — egress only.** Add Kafka as a `_deliver()` sink (results topic /
   `reply_to`). Low risk, immediately useful, reuses existing terminal hook.
2. **Phase 2 — ingress.** Consumer thread + idempotent enqueue + manual offset
   commit + DLT. The reliability core (§6).
3. **Phase 3 — CLI + ops.** `hoglah kafka-bridge`, security config, docs, a
   gated integration test (against a local Kafka via Docker, gated like
   `RUN_KAFKA_TESTS=1` mirroring the Mongo test gating).

Each phase is independently shippable and testable.
