# Hoglah — feature comparison & gap list

**Status:** living document  
**Captured:** 2026-08-07  
**Package baseline:** Hoglah **0.9.x**  
**Purpose:** Restart-friendly checklist for prioritising work against similar
queueing / LLM-serving tools. Update this file when shipping features or
revising scope.

**How to use on CLI restart**

1. Open this file: `docs/feature-comparison-and-gaps.md`
2. Re-check §3 strength map and §6 gaps against current `src/hoglah/`
3. Move closed gaps to §7 “Closed”
4. Bump “Last reviewed” and package baseline when done

**Last reviewed:** 2026-08-07  
**Next review trigger:** any release that changes queue semantics, scheduling,
leases, rate limits, or multi-worker crash recovery

---

## 1. Product positioning (do not forget)

Hoglah is **not** a general “run any Python function” queue (Celery/RQ class).

It is a **local-first, durable job queue specialised for Ollama LLM work**
(generate / chat / embed), with optional multi-machine backends and messaging
bridges.

**Core value**

- Serial / low-concurrency inference on constrained hardware (default concurrency 1)
- Submit → job ID → durable result (poll, callback, HTTP, file drop)
- Safe stub adapter by default; real Ollama opt-in
- Multi-Ollama host dispatch with warm-affinity
- Family observability (Galeed spine + full I/O capture)

**Non-goals (still)**

- Full distributed workflow engine (Temporal / Celery Canvas)
- Multi-tenant auth for public exposure of `hoglah serve`
- Owning continuous batching / paged KV (belongs in the inference server)
- Becoming a multi-cloud LLM spend gateway (LiteLLM niche)

Related: `docs/requirements-v1.0.md`, `docs/architecture-decisions.md`, `okf/`.

---

## 2. Comparison set

| Tool | What it is | Fair comparison? |
|---|---|---|
| **RQ** | Simple Redis job queue (any callable) | Partial — simplicity class |
| **Huey** | Lightweight Redis/SQLite tasks | Partial |
| **Dramatiq** | Modern broker tasks, retries, rate limits | Partial — reliability class |
| **Celery** | Full distributed task platform | Partial — queue ops only |
| **ARQ / Taskiq** | Async-native Python queues | Partial |
| **BullMQ / Sidekiq** | Production job queues (Node/Ruby) | Feature checklist only |
| **vLLM / continuous batching** | High-throughput inference engine | Different layer |
| **LiteLLM Proxy** | Multi-provider gateway + spend/limits | LLM gateway class |
| **Temporal / Prefect** | Workflow orchestration | Different layer |

Closest niche: **specialised local LLM inference queue** — closer to “RQ + Ollama
sidecar with durable results” than Celery Canvas.

---

## 3. Strength map (what Hoglah already has)

Update checkboxes when verifying against code.

| Capability | Status | Notes / code touchpoints |
|---|---|---|
| Submit → job id → async result | ✅ | `client.Hoglah.submit` |
| Job kinds: generate, chat, embed | ✅ | `models.JobRequest.kind`, `submit_embedding` |
| Durable store SQLite | ✅ | `store.py`, default `~/.hoglah/hoglah.db` |
| Durable store Mongo (multi-worker) | ✅ | `mongo_store.py`, extra `[mongo]` |
| Priority scheduling | ✅ | `JobRequest.priority`, store sort |
| Concurrency limit | ✅ | `config.concurrency` (default 1) |
| Retries | ✅ | `max_retries`, transient retry in worker |
| Per-job timeout | ✅ | `timeout_seconds` |
| Cancel (best-effort) | ✅ | `Hoglah.cancel`, status `cancelled` |
| Poll / wait | ✅ | `status`, `wait` |
| In-process callback + named registry | ✅ | restart-safe via `callback_key` |
| HTTP callback | ✅ | `callback_url`, retries/backoff for POST |
| Output folder JSON drop | ✅ | `output_dir` / `HOGLAH_OUTPUT_DIR` |
| Context truncation metadata | ✅ | `JobResult.truncated*`, ADR-009 |
| Multi-Ollama dispatch (warm + least-loaded) | ✅ | `dispatch.py`, `ollama_hosts` |
| Kafka / RabbitMQ / Redis Streams bridges | ✅ | crash-safe consume + DLQ patterns |
| MessagingSubmitter (publish + await) | ✅ | `messaging_submitter.py` |
| CLI: submit, ps, wait, stats, clear, rm, run, doctor | ✅ | `cli.py` |
| CLI live `monitor` | ✅ | throughput + recent jobs |
| Web monitor `hoglah serve` | ✅ | read-only UI + JSON API |
| Galeed job lifecycle events | ✅ | `tracing.py`, extra `[galeed]` |
| Galeed full I/O capture (`llm_calls`) | ✅ | call_id = job_id |
| Step names for debug views | ✅ | `submit(step_name=…)` / `--step` |
| SessionPriorityQueue (in-process) | ✅ | `priority_queue.py` — per-key serial |
| Parent linkage | ✅ | `parent_job_id` (lineage, not DAG engine) |
| Tags / metadata | ✅ | filter/list |
| Keturah capability manifest | ✅ | `manifest.py` |
| Safe stub adapter default | ✅ | real Ollama opt-in |
| Correlation idempotency on bridges | ✅ | Mongo unique correlation_id |

---

## 4. Feature matrix (selected competitors)

Legend: **Y** yes · **P** partial · **N** no · **N/A** wrong layer

| Feature | Hoglah | RQ | Huey | Dramatiq | Celery | LiteLLM-ish |
|---|---|---|---|---|---|---|
| Zero-broker local default | **Y** (SQLite) | N | P | N | N | N |
| LLM-native job model | **Y** | N | N | N | N | Y |
| Generate/chat/embed unified | **Y** | N | N | N | N | P |
| Context truncation metadata | **Y** | N | N | N | N | P |
| Multi-host Ollama affinity | **Y** | N | N | N | N | P |
| Priority | Y | P | Y | Y | Y | P |
| Retries + backoff | P | P | Y | **Y** | Y | Y |
| Per-job timeout | Y | Y | Y | Y | Y | Y |
| Cancel in-flight | P | P | P | P | P | P |
| Delayed / scheduled / cron | **N** | P | Y | P | **Y** | P |
| Rate limiting / quotas | **N** | N | P | **Y** | Y | **Y** |
| Unique / dedupe jobs | P | N | P | P | P | P |
| Chains / chords / DAG | **N** | N | P | P | **Y** | N |
| Dead-letter (broker poison) | Y | P | P | Y | Y | P |
| Dead-letter (failed jobs UX) | P | P | P | P | P | P |
| Multi-broker consume | **Y** | N | N | P | Y | N |
| Result webhooks | **Y** | N | N | N | P | Y |
| File drop results | **Y** | N | N | N | N | N |
| Full prompt/output debug capture | **Y** | N | N | N | N | P |
| Auth / multi-tenant | **N** | N | N | N | P | Y |
| Streaming tokens to client | **N** | N | N | N | N | Y |
| Continuous batching | **N** | N/A | N/A | N/A | N/A | P |
| Horizontal workers + shared store | Y | Y | Y | Y | Y | Y |
| Built-in monitor UI | Y | P | N | N | Flower | Y |
| Safe stub for tests | **Y** | N | N | N | N | N |

---

## 5. Where Hoglah is *ahead* (protect these)

1. LLM-native result model (truncation, embeddings, usage/timings)
2. Safe stub by default (CI, no accidental GPU load)
3. Warm multi-Ollama dispatch (reload-aware)
4. Four delivery paths including file drop
5. Galeed full In→Out capture tied to job_id
6. SQLite zero-setup + Mongo multi-worker path
7. Crash-safe messaging bridges (outbox / DLQ), not afterthoughts

---

## 6. Open gaps (prioritised)

Mark status: `open` | `in_progress` | `deferred` | `wontfix`.  
When closing, move the row to §7 with date + version.

### P0 — High value, fits product

| ID | Gap | Status | Why it matters | Suggested cut |
|---|---|---|---|---|
| G1 | Delayed / scheduled jobs | open | Agent backoff, off-peak embedding, deferred re-ingest | `submit(..., run_at=… \| delay_seconds=N)` + due-index |
| G2 | Richer retry policy | open | OOM vs timeout vs 5xx need different behaviour | `{max, backoff, jitter, retry_on}` |
| G3 | Lease / heartbeat for PROCESSING | open | Multi-worker Mongo: dead worker must requeue | Heartbeat interval; stale → requeue |
| G4 | Harder in-flight cancel | open | Cancel often only affects queued | Coordinate with lease + adapter cancel |
| G5 | Rate limits / fairness | open | Many agents share one GPU | Token bucket per tag/session; per-model slots |
| G6 | Idempotent submit API | open | Agent loops double-enqueue | `idempotency_key=` / unique constraint |

### P1 — Production local estate

| ID | Gap | Status | Why it matters | Suggested cut |
|---|---|---|---|---|
| G7 | Dependencies beyond parent_id | open | parent_id is lineage only | Minimal `depends_on: [job_ids]` or enqueue-child-on-complete |
| G8 | Streaming results | open | Interactive UIs want tokens | SSE on serve or chunk files; keep store final |
| G9 | Failed-job DLQ view + requeue | open | Poison messages ≠ failed inference | CLI `requeue` + web filter on failed |
| G10 | Per-model concurrency / slots | open | 70B vs 8B share one queue poorly | Slot table next to dispatch |
| G11 | Metrics export | open | Ops parity with Flower/Horizon | Prometheus counters (queue depth, latency, fail) |

### P2 — Nice-to-have / explicit non-goals

| ID | Gap | Status | Notes |
|---|---|---|---|
| G12 | General Python callables | deferred / wontfix | Not the product; keep LLM I/O focus |
| G13 | Cloud brokers primary (SQS, Pub/Sub) | deferred | Kafka/RMQ/Redis already cover many estates |
| G14 | Auth / multi-tenancy | deferred | Local-only; required if expose serve beyond localhost |
| G15 | Full workflow engine | wontfix | Deborah/Tirzah own process graphs |
| G16 | vLLM continuous batching | wontfix | Inference server concern |
| G17 | Non-Ollama backends | deferred | Listed non-goal V1; revisit if family needs OpenAI-compatible workers |

### Spec vs code notes (requirements-v1.0)

| Spec item | Status |
|---|---|
| Cancel by ID | Present (best-effort) |
| max_retries / timeout / priority | Present |
| parent_job_id chaining | Lineage yes; automatic dependency execution no |
| Advanced multi-tenancy / auth | Non-goal |
| Non-Ollama backends | Deferred |

---

## 7. Closed gaps

| ID | Gap | Closed in version | Date | Notes |
|---|---|---|---|---|
| — | — | — | — | — |

Example row when shipping:

| G1 | Delayed jobs | 0.10.0 | 2026-… | `run_at` + store index |

---

## 8. Recommended implementation order

If resuming without further instruction, default order:

1. **G1** Delayed / scheduled enqueue  
2. **G3** PROCESSING lease + reclaim  
3. **G2** Retry policy object  
4. **G6** Idempotency key on submit  
5. **G10** Per-model concurrency slots  
6. **G5** Fairness / rate limit  
7. **G7** Minimal depends_on  
8. **G9** Failed-job DLQ + requeue  
9. **G11** Prometheus metrics  
10. **G8** Optional token streaming  

Avoid: Celery Canvas, Beat clone, or vLLM-inside-Hoglah.

---

## 9. Code map (for gap work)

| Area | Paths |
|---|---|
| Public API | `src/hoglah/client.py`, `__init__.py` |
| Models | `src/hoglah/models.py` |
| Config | `src/hoglah/config.py` |
| SQLite store | `src/hoglah/store.py` |
| Mongo store | `src/hoglah/mongo_store.py` |
| Multi-host dispatch | `src/hoglah/dispatch.py` |
| Adapters (stub/real) | `src/hoglah/adapters.py` |
| Bridges | `kafka_bridge.py`, `rabbitmq.py`, `redis_streams.py` |
| Messaging submitter | `messaging_submitter.py` |
| In-process PQ | `priority_queue.py` |
| CLI | `cli.py` |
| Web monitor | `web.py` |
| Tracing | `tracing.py` |
| Tests | `tests/` |

---

## 10. Review log

| Date | Reviewer / session | Changes |
|---|---|---|
| 2026-08-07 | Grok CLI session | Initial capture from Hoglah vs RQ/Huey/Dramatiq/Celery/LiteLLM/etc. |

---

## 11. Sources (external context, not normative)

- Python task-queue comparisons (Celery, Dramatiq, RQ, Huey, Taskiq, etc.) — community posts 2024–2026  
- Hoglah README, CHANGELOG 0.9.0, `docs/requirements-v1.0.md`, ADRs, okf concepts  

This document is the **source of truth for gap tracking**; competitor feature cells may go stale — re-verify when prioritising a gap against a specific tool.
