# Hoglah user guide

**Version:** 0.10.2  
**Audience:** developers and operators who run local (or multi-machine) LLM
queues on Ollama.  
**Companion:** [AI user guide](ai-user-guide.md) for agent-oriented contracts.

---

## 1. What Hoglah is

Hoglah is a **durable job queue specialised for Ollama** (generate, chat, embed).

You submit work and get a **job ID immediately**. A background worker claims jobs
from a durable store (SQLite by default, MongoDB for multi-worker), runs them
through Ollama (or a safe stub in tests), and delivers results by poll, callback,
HTTP webhook, file drop, and/or a messaging broker.

**It is not:**

- A general “run any Python function” queue (Celery/RQ class)
- A multi-cloud LLM spend gateway (LiteLLM niche)
- A full workflow engine (use `depends_on` for simple chains only)
- An inference server (continuous batching stays in Ollama / vLLM)

**It is good at:**

- Serial / low-concurrency inference on constrained hardware (default concurrency 1)
- Safe defaults (stub adapter; real Ollama is opt-in)
- Multi-agent fairness (session/tag slots and rates, per-model slots)
- Crash-safe multi-worker reclaim (leases + heartbeat)
- Crash-safe messaging bridges (Kafka / RabbitMQ / Redis Streams)

---

## 2. Install

Requires **Python 3.11+**.

```bash
pip install hoglah                # library
pip install "hoglah[cli]"         # + CLI (typer)
pip install "hoglah[mongo]"       # MongoDB backend
pip install "hoglah[kafka]"       # Kafka bridge
pip install "hoglah[rabbitmq]"    # RabbitMQ bridge
pip install "hoglah[redis]"       # Redis Streams bridge
pip install "hoglah[web]"         # web monitor (serve)
pip install "hoglah[galeed]"      # family trace spine + I/O capture
pip install "hoglah[dev]"         # pytest, ruff, etc.
```

From a git checkout:

```bash
pip install -e ".[cli,dev]"
```

---

## 3. Mental model

```
  submit() ──► JobStore (SQLite / Mongo)
                    │
                    ▼
              Worker (concurrency + slots + fairness)
                    │
                    ▼
              Adapter (Stub | Ollama, multi-host pool)
                    │
                    ▼
         Result: poll | callback | HTTP | file | broker
```

| Status | Meaning |
|---|---|
| `queued` | Waiting (may be delayed, waiting on deps, or waiting for a free slot) |
| `processing` | Claimed by a worker; lease is heartbeated |
| `completed` | Success (may still set `truncated=True`) |
| `failed` | Terminal error (retries exhausted, bad dep, etc.) |
| `cancelled` | User cancel (queued or in-flight) |

**Job kinds:**

| Kind | How to submit | Result fields |
|---|---|---|
| Generate | `submit(prompt=..., model=...)` | `output` text |
| Chat | `submit(messages=[...], model=...)` | `output` text |
| Embed | `submit_embedding(text, model=...)` or `submit(kind="embed", ...)` | `embedding`, `embedding_dim` |

---

## 4. Library quick start

```python
from hoglah import Hoglah

# Context manager starts/stops the background worker cleanly.
with Hoglah(use_real=True) as h:   # omit use_real → StubAdapter (no GPU/network)
    job_id = h.submit(
        prompt="Explain context windows in one sentence.",
        model="gemma3:1b",
        tags=["demo"],
    )
    result = h.wait(job_id, timeout=120)
    print(result.status, result.output)
```

### Pure submitter (no worker in this process)

```python
h = Hoglah(start_worker=False)   # only enqueue; a separate `hoglah run` drains the queue
job_id = h.submit(prompt="…", model="gemma3:1b")
```

### Dedicated worker process

```bash
hoglah run --real --concurrency 1
# or multi-GPU hosts:
hoglah run --real --ollama-hosts http://gpu1:11434,http://gpu2:11434
```

### Common `submit` options

| Parameter | Purpose |
|---|---|
| `prompt` / `messages` | Generate vs chat input |
| `model` | **Required** model name |
| `tags` | List filters; fairness tag slots/rates |
| `priority` | Higher runs first among due jobs |
| `timeout_seconds` | Per-attempt wall clock; terminal by default |
| `max_retries` / `retry_policy` | Retry policy (see §6) |
| `delay_seconds` / `run_at` | Schedule later |
| `idempotency_key` | Same key → same job id (no duplicate) |
| `depends_on` | Wait until listed jobs complete |
| `parent_job_id` | Lineage only (not execution wait) |
| `step_name` | Label for debug / Galeed views |
| `callback` / `callback_key` / `callback_url` | Delivery hooks |
| `metadata` | Free dict; use `session_id` for fairness |

### Collecting results

```python
h.status(job_id)                 # JobStatus enum
h.get(job_id)                    # JobResult (any status)
h.wait(job_id, timeout=60)       # block until terminal or TimeoutError
h.list(status="failed", limit=20)
h.cancel(job_id)                 # queued or in-flight; dependents fail
h.remove(job_id)                 # cancel if needed, then delete the row
h.submit_batch([...], model="…") # named jobs + intra-batch depends_on
h.wait_batch(batch.batch_id)
h.cancel_batch(batch.batch_id)
h.requeue(job_id)                # FAILED → QUEUED
h.requeue_failed(limit=50)
h.stats()                        # counts by status
h.metrics_text()                 # Prometheus exposition
```

---

## 5. CLI reference (essentials)

Install CLI: `pip install "hoglah[cli]"`.

```bash
hoglah submit "Hello" --model gemma3:1b --wait --real
hoglah submit "later" --model gemma3:1b --delay 60
hoglah submit "nightly" --model gemma3:1b --run-at 2026-08-08T02:00:00Z
hoglah submit "once" --model gemma3:1b --idempotency-key agent-step-3
hoglah submit "step2" --model gemma3:1b --depends-on <parent-job-id>
hoglah ps --json
hoglah status <job-id> --json
hoglah wait <job-id> --timeout 120
hoglah cancel <job-id>
hoglah stats
hoglah metrics
hoglah monitor -i 2
hoglah dlq
hoglah requeue <job-id>
hoglah requeue --all-failed --yes
hoglah run --real -c 2 --model-slots 'llama3.1:70b=1,gemma3:1b=2'
hoglah run --session-slots 1 --tag-slots 'agent-a=1' --session-rate 30
hoglah serve --port 8781          # needs hoglah[web]
hoglah doctor --real
hoglah models --real
hoglah pull MODEL --real
hoglah clear --status completed --older-than 7 --yes
hoglah rm <job-id> --yes
```

**Stub vs real:** default is the safe stub (no Ollama calls). Pass `--real` or set
`HOGLAH_USE_REAL_ADAPTER=1`.

**Web monitor:** `hoglah serve` binds `127.0.0.1:8781` by default (no auth).  
Prometheus scrape: `http://127.0.0.1:8781/metrics`.

---

## 6. Scheduling, retries, dependencies, idempotency

### Delayed jobs

```python
h.submit(prompt="…", model="m", delay_seconds=60)
h.submit(prompt="…", model="m", run_at="2026-08-08T02:00:00Z")
# Pass at most one of delay_seconds / run_at.
```

Job stays `queued` until due; workers will not claim early.

### Retry policy

```python
from hoglah import RetryPolicy

h.submit(
    prompt="…", model="m",
    retry_policy=RetryPolicy(
        max_retries=4,          # extra attempts after the first
        base_delay=1.0,
        backoff_factor=2.0,
        max_delay=30.0,
        jitter=0.1,             # 0..1 equal jitter
        retry_on=("transient", "oom"),  # oom is opt-in
    ),
)
```

| `retry_on` class | Default? | Notes |
|---|---|---|
| `transient` | yes | connection, message-timeout, rate_limit, server |
| `connection` / `timeout` / `rate_limit` / `server` | via transient | fine-grained |
| `oom` | **no** | CUDA/host OOM |
| `timeout` (job wall-clock) | **no** | `timeout_seconds` is terminal unless opted in |
| `all` / `none` | — | always / never |

Legacy `max_retries=N` still works. `max_retries=0` means **one** attempt.

### Idempotency

```python
h.submit(prompt="…", model="m", idempotency_key="session-42/step-3")
# same key again → same job id, no second row
```

Independent of messaging `correlation_id` (broker redelivery).

### Dependencies

```python
a = h.submit(prompt="step 1", model="m")
b = h.submit(prompt="step 2", model="m", depends_on=[a], parent_job_id=a)
```

| Field | Role |
|---|---|
| `depends_on` | **Execution:** wait until each id is `completed` |
| `parent_job_id` | **Lineage only** for debugging / filters |

If any dependency is `failed`, `cancelled`, or missing, the child fails without
running.

---

## 7. Concurrency, slots, and fairness

All of these sit **under** the global `concurrency` semaphore.

### Global concurrency

```python
Hoglah(config={"concurrency": 2})
# env: HOGLAH_CONCURRENCY=2
```

### Per-model slots

```python
Hoglah(config={
    "concurrency": 2,
    "model_slots": {"llama3.1:70b": 1, "gemma3:1b": 2},
    "default_model_slots": 1,   # optional cap for unlisted models
})
```

```bash
hoglah run --real -c 2 --model-slots 'llama3.1:70b=1,gemma3:1b=2'
# env: HOGLAH_MODEL_SLOTS=llama3.1:70b=1,gemma3:1b=2
```

### Multi-agent fairness

Use `metadata={"session_id": "..."}` and/or `tags=[...]`.

```python
Hoglah(config={
    "concurrency": 4,
    "session_slots": 1,                    # one in-flight job per session
    "tag_slots": {"agent-a": 1},
    "session_rate_per_minute": 30,         # token-bucket starts
    "tag_rates_per_minute": {"agent-a": 6},
})
```

```bash
hoglah run --session-slots 1 --tag-slots 'a=1' --session-rate 30 --tag-rates 'a=6'
```

When fairness gates are on, the worker prefers **less-loaded sessions** among due
jobs.

### Multi-host Ollama

```python
Hoglah(use_real=True, config={
    "ollama_hosts": ["http://gpu1:11434", "http://gpu2:11434"],
})
```

Dispatch prefers a host that recently ran the same model (warm affinity), then
least-loaded.

---

## 8. Storage backends

### SQLite (default)

- Path: `~/.hoglah/hoglah.db` (override `db_path` / `HOGLAH_DB_PATH`)
- Zero extra deps; WAL + busy timeout for submitter + worker on one machine
- Good for single host; multi-process on one SQLite file is possible but Mongo is
  cleaner for multi-machine

### MongoDB

```bash
pip install "hoglah[mongo]"
```

```python
Hoglah(config={
    "backend": "mongo",
    "mongo_uri": "mongodb://localhost:27017",
    "mongo_db": "hoglah",
    "mongo_collection": "jobs",
})
```

Atomic `claim_for_processing` via `find_one_and_update` → multi-machine workers
with one queue.

---

## 9. Result delivery

Use any combination:

1. **Poll / wait** — `status`, `get`, `wait`
2. **In-process callback** — `callback=fn` (this process only) or named
   `callback_key` + `Hoglah(callbacks={...})` for restart re-delivery
3. **HTTP callback** — `callback_url=...` (daemon thread, retries, SSRF-aware)
4. **Output folder** — `output_dir` / `HOGLAH_OUTPUT_DIR` →
   `<dir>/<job_id>.json` atomic write
5. **Messaging** — result published when the job came from a bridge (outbox)

`callback_allow_private_hosts` defaults to **True** (local-first). Set `False`
when untrusted submitters can set `callback_url` via bridges.

---

## 10. Leases, cancel, requeue

### Leases (multi-worker safety)

- Claim sets `lease_token` + `lease_expires_at` (default 30s)
- Worker heartbeats (default every 10s)
- Stale leases are reclaimed to `queued` (startup + each poll)
- Token-gated completion prevents a dead worker from overwriting a reclaimed job

Config: `lease_seconds`, `heartbeat_interval_seconds` /
`HOGLAH_LEASE_SECONDS`, `HOGLAH_HEARTBEAT_INTERVAL_SECONDS`.

### Cancel

```python
h.cancel(job_id)   # True if transitioned to cancelled
```

Works for **queued and in-flight**. Another process can cancel without holding
the worker’s task handle: the worker’s **cancel-watch** (every 250ms) aborts the
local task when the store says `cancelled`. Lease is cleared so reclaim will not
revive the job.

### Failed-job DLQ (inference failures)

Distinct from messaging poison DLQs.

```bash
hoglah dlq
hoglah requeue <job-id>
hoglah requeue --all-failed --yes
```

```python
h.requeue(job_id)
h.requeue_failed(limit=20)
h.requeue(job_id, allow_cancelled=True)  # optional
```

---

## 11. Messaging bridges

Bridges are **transport**, not storage. Enable **at most one** of Kafka /
RabbitMQ / Redis per instance.

Shared contract:

**Request (JSON):**

```json
{
  "correlation_id": "unique-required",
  "model": "gemma3:1b",
  "prompt": "…",
  "kind": "generate",
  "options": { "temperature": 0.7 },
  "reply_to": "optional-override-destination"
}
```

**Result (JSON):**

```json
{
  "correlation_id": "unique-required",
  "job_id": "…",
  "status": "completed",
  "output": "…",
  "error": null
}
```

Crash safety: idempotent enqueue on `correlation_id`; transactional outbox on
egress; poison → dead-letter.

```bash
hoglah kafka-bridge --bootstrap-servers localhost:9092
hoglah rabbitmq-bridge --url amqp://guest:guest@localhost:5672/
hoglah redis-bridge --url redis://localhost:6379/0
```

See [kafka-bridge-design.md](kafka-bridge-design.md) and
[rabbitmq-bridge-design.md](rabbitmq-bridge-design.md).

---

## 12. Observability

### Prometheus

```bash
hoglah metrics
# or
curl -s http://127.0.0.1:8781/metrics
```

Notable series: `hoglah_jobs{status=…}`, `hoglah_jobs_submitted_total`,
`hoglah_jobs_terminal_total{status=…}`, `hoglah_job_requeues_total`,
`hoglah_lease_reclaims_total`, `hoglah_job_duration_seconds`,
`hoglah_process_uptime_seconds`.

### Galeed (optional)

```bash
pip install "hoglah[galeed]"
export HOGLAH_GALEED_ENABLED=1
```

Job lifecycle events + full prompt/output capture (`llm_calls`). Label with
`step_name` / `--step`. View with `hoglah debug` (needs `galeed[cli]`).

### Web monitor

```bash
pip install "hoglah[web]"
hoglah serve --host 127.0.0.1 --port 8781
```

Read-only; maintenance (`clear` / `rm` / `requeue` / `cancel`) stays on the CLI.

---

## 13. Configuration cheat sheet

Every field can be set via `Hoglah(config={...})` or `HOGLAH_*` env vars.

| Setting | Env | Default | Purpose |
|---|---|---|---|
| `db_path` | `HOGLAH_DB_PATH` | `~/.hoglah/hoglah.db` | SQLite path |
| `backend` | `HOGLAH_BACKEND` | `sqlite` | `sqlite` \| `mongo` |
| `concurrency` | `HOGLAH_CONCURRENCY` | `1` | Global parallel jobs |
| `lease_seconds` | `HOGLAH_LEASE_SECONDS` | `30` | PROCESSING lease TTL |
| `heartbeat_interval_seconds` | `HOGLAH_HEARTBEAT_INTERVAL_SECONDS` | `10` | Lease heartbeat |
| `model_slots` | `HOGLAH_MODEL_SLOTS` | `{}` | `name=n,name=n` |
| `default_model_slots` | `HOGLAH_DEFAULT_MODEL_SLOTS` | `None` | Cap for unlisted models |
| `session_slots` | `HOGLAH_SESSION_SLOTS` | `None` | Concurrent per session_id |
| `tag_slots` | `HOGLAH_TAG_SLOTS` | `{}` | Concurrent per tag |
| `session_rate_per_minute` | `HOGLAH_SESSION_RATE_PER_MINUTE` | `None` | Start rate / session |
| `tag_rates_per_minute` | `HOGLAH_TAG_RATES_PER_MINUTE` | `{}` | Start rate / tag |
| `ollama_host` | `HOGLAH_OLLAMA_HOST` | client default | Single Ollama URL |
| `ollama_hosts` | `HOGLAH_OLLAMA_HOSTS` | `[]` | Multi-host list |
| `output_dir` | `HOGLAH_OUTPUT_DIR` | `None` | Result file drop |
| `log_level` | `HOGLAH_LOG_LEVEL` | `INFO` | Logger level |
| — | `HOGLAH_USE_REAL_ADAPTER` | unset | Force real Ollama |

`hoglah doctor` prints active backend/transport **without** connection secrets.

---

## 14. Topologies (recipes)

### A. Single process script

```python
with Hoglah(use_real=True) as h:
    ...
```

### B. App submits, separate worker

```python
# app
Hoglah(start_worker=False).submit(...)
```

```bash
# worker host
hoglah run --real --db ~/.hoglah/hoglah.db
```

### C. Multi-machine Mongo fleet

```bash
export HOGLAH_BACKEND=mongo HOGLAH_MONGO_URI=mongodb://mongo:27017
hoglah run --real -c 1   # on each GPU box
```

### D. Agent loops

```python
h.submit(
    prompt=...,
    model=...,
    idempotency_key=f"{session}/{step}",
    metadata={"session_id": session},
    tags=[agent_name],
    depends_on=prev_ids or None,
    step_name=step,
)
```

### E. Broker front door

Run `hoglah kafka-bridge` (or rabbitmq/redis) with Mongo store; producers only
speak the JSON contract.

---

## 15. Real Ollama notes

- Enable: `use_real=True` / `--real` / `HOGLAH_USE_REAL_ADAPTER=1`
- Pull: `hoglah pull MODEL --real` (supports `hf.co/...:QUANT` GGUF names)
- Native Linux: default `http://localhost:11434`
- **WSL2:** Ollama on Windows is not `localhost` from WSL — set `OLLAMA_HOST`
  on Windows and point the client at the WSL gateway IP (see root README)

---

## 16. Troubleshooting

| Symptom | Checks |
|---|---|
| Jobs stuck `queued` | Worker running? `depends_on` unmet? `run_at` in future? Model/session/tag at capacity? |
| Jobs stuck `processing` | Worker crash? Wait for lease reclaim (`lease_seconds`) or restart worker |
| Double execution | Two workers + broken store? Prefer Mongo for multi-machine; leases must be heartbeating |
| No real inference | Forgot `--real` / stub still default |
| Cancel does nothing | Job already terminal? Check `hoglah status` |
| Callback never fires | Direct `callback=` is process-local; use `callback_key` + registry or `callback_url` / `output_dir` |
| `doctor` shows free / zero cost with Galeed | Older galeed version skew — upgrade galeed |

Include `hoglah doctor --real` output when filing bugs.

---

## 17. Further reading

- [AI user guide](ai-user-guide.md) — agent contracts and invariants  
- [feature-comparison-and-gaps.md](feature-comparison-and-gaps.md) — roadmap  
- [architecture-decisions.md](architecture-decisions.md) — ADRs  
- [../okf/index.md](../okf/index.md) — structured knowledge map  
- [../CHANGELOG.md](../CHANGELOG.md)
