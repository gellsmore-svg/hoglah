# Hoglah — feature comparison & gap list

**Status:** living document  
**Captured:** 2026-08-07  
**Package baseline:** Hoglah **0.10.1**  
**Purpose:** Restart-friendly checklist for prioritising work against similar
queueing / LLM-serving tools. Update this file when shipping features or
revising scope.

**How to use on CLI restart**

1. Open this file: `docs/feature-comparison-and-gaps.md`
2. Re-check §3 strength map and §6 gaps against current `src/hoglah/`
3. Move closed gaps to §7 “Closed”
4. Bump “Last reviewed” and package baseline when done

**Last reviewed:** 2026-08-07 (G1–G3, G6, G9, G10 closed → 0.10.0)  
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
| Retries | ✅ | `RetryPolicy` (max/backoff/jitter/retry_on) + legacy `max_retries` |
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
| Delayed / scheduled enqueue | ✅ | `delay_seconds` / `run_at`; store due-index; no cron |
| PROCESSING lease + heartbeat | ✅ | token + `lease_expires_at`; stale reclaim only |
| Richer retry policy | ✅ | `RetryPolicy`; named classes; jittered exponential |
| Idempotent submit | ✅ | `idempotency_key` unique index (≠ bridge correlation_id) |
| Failed-job DLQ + requeue | ✅ | `hoglah dlq` / `requeue`; store reset to QUEUED |
| Per-model concurrency slots | ✅ | `model_slots` / `default_model_slots` under global concurrency |
| Fairness / rate limits | ✅ | session/tag slots + token-bucket start rates |
| Prometheus metrics | ✅ | `/metrics` + `hoglah metrics`; no extra dep |
| Minimal depends_on | ✅ | wait for COMPLETED; fail if dep dead; not a DAG engine |

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
| Retries + backoff | **Y** (policy object) | P | Y | **Y** | Y | Y |
| Per-job timeout | Y | Y | Y | Y | Y | Y |
| Cancel in-flight | P | P | P | P | P | P |
| Worker lease / reclaim | **Y** | N | P | Y | P | N |
| Delayed / scheduled / cron | **P** (delay/run_at; no cron) | P | Y | P | **Y** | P |
| Rate limiting / quotas | **P** (session/tag slots + token bucket) | N | P | **Y** | Y | **Y** |
| Unique / dedupe jobs | **Y** (`idempotency_key`) | N | P | P | P | P |
| Chains / chords / DAG | **P** (`depends_on` list only) | N | P | P | **Y** | N |
| Dead-letter (broker poison) | Y | P | P | Y | Y | P |
| Dead-letter (failed jobs UX) | **Y** (dlq + requeue) | P | P | P | P | P |
| Multi-broker consume | **Y** | N | N | P | Y | N |
| Result webhooks | **Y** | N | N | N | P | Y |
| File drop results | **Y** | N | N | N | N | N |
| Full prompt/output debug capture | **Y** | N | N | N | N | P |
| Auth / multi-tenant | **N** | N | N | N | P | Y |
| Streaming tokens to client | **N** | N | N | N | N | Y |
| Continuous batching | **N** | N/A | N/A | N/A | N/A | P |
| Horizontal workers + shared store | Y | Y | Y | Y | Y | Y |
| Built-in monitor UI | Y | P | N | N | Flower | Y |
| Prometheus metrics | **Y** | N | N | P | P | Y |
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
| G4 | Harder in-flight cancel | open | Cancel often only affects queued | Coordinate with lease + adapter cancel |

### P1 — Production local estate

| ID | Gap | Status | Why it matters | Suggested cut |
|---|---|---|---|---|
| G8 | Streaming results | open | Interactive UIs want tokens | SSE on serve or chunk files; keep store final |

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
| parent_job_id chaining | Lineage yes; `depends_on` for execution wait (G7) |
| Advanced multi-tenancy / auth | Non-goal |
| Non-Ollama backends | Deferred |

---

## 7. Closed gaps

| ID | Gap | Closed in version | Date | Notes |
|---|---|---|---|---|
| G1 | Delayed / scheduled jobs | 0.10.0 | 2026-08-07 | `delay_seconds` / `run_at`; due-index; CLI `--delay` / `--run-at`. |
| G2 | Richer retry policy | 0.10.0 | 2026-08-07 | `RetryPolicy`; oom/timeout opt-in; `max_retries=0` fix. |
| G3 | PROCESSING lease + heartbeat | 0.10.0 | 2026-08-07 | lease token + heartbeat; stale reclaim; token-gated complete. |
| G6 | Idempotent submit | 0.10.0 | 2026-08-07 | `idempotency_key` unique index. |
| G9 | Failed-job DLQ + requeue | 0.10.0 | 2026-08-07 | `dlq` / `requeue`; FAILED→QUEUED. |
| G10 | Per-model concurrency slots | 0.10.0 | 2026-08-07 | `model_slots` / `default_model_slots`; peer PROCESSING counted. |
| G5 | Fairness / rate limits | 0.10.1 | 2026-08-07 | session/tag concurrent slots + token-bucket rates; fair session order. |
| G11 | Prometheus metrics | 0.10.1 | 2026-08-07 | text exposition; gauges + counters + latency summary; CLI + /metrics. |
| G7 | Minimal depends_on | unreleased (→0.10.2) | 2026-08-07 | `depends_on` list; wait COMPLETED; fail if dep FAILED/CANCELLED/missing. |

---

## 8. Recommended implementation order

If resuming without further instruction, default order:

1. **G8** Optional token streaming  
2. **G4** Harder in-flight cancel (builds on G3 leases)  

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
| 2026-08-07 | Grok CLI session | Closed G1 delayed/scheduled jobs (`run_at` / `delay_seconds`). |
| 2026-08-07 | Grok CLI session | Closed G3 PROCESSING leases + heartbeat + stale reclaim. |
| 2026-08-07 | Grok CLI session | Closed G2 RetryPolicy (max/backoff/jitter/retry_on). |
| 2026-08-07 | Grok CLI session | Closed G6 idempotency_key on submit. |
| 2026-08-07 | Grok CLI session | Closed G9 failed-job dlq + requeue. |
| 2026-08-07 | Grok CLI session | Closed G10 model slots; cut 0.10.0. |
| 2026-08-07 | Grok CLI session | Closed G5 session/tag fairness + rate limits. |
| 2026-08-07 | Grok CLI session | Closed G11 Prometheus metrics. |
| 2026-08-07 | Grok CLI session | Closed G7 minimal depends_on. |

---

## 11. Sources (external context, not normative)

- Python task-queue comparisons (Celery, Dramatiq, RQ, Huey, Taskiq, etc.) — community posts 2024–2026  
- Hoglah README, CHANGELOG 0.9.0, `docs/requirements-v1.0.md`, ADRs, okf concepts  

This document is the **source of truth for gap tracking**; competitor feature cells may go stale — re-verify when prioritising a gap against a specific tool.
