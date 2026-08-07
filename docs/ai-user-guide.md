# Hoglah — AI / agent user guide

**Version:** 0.10.2  
**Audience:** coding agents, multi-agent runtimes, and tool-using models that
enqueue LLM work through Hoglah.  
**Human companion:** [user-guide.md](user-guide.md).

This document is written for **machines that plan and call tools**. Prefer
deterministic contracts, invariants, and failure modes over narrative.

---

## 0. Purpose (one line)

Durable, local-first **Ollama job queue**: submit generate/chat/embed work,
get `job_id` immediately, collect terminal `JobResult` asynchronously.

---

## 1. Identity and non-goals

| Is | Is not |
|---|---|
| LLM-specialised durable queue | Generic task queue (arbitrary Python callables) |
| Ollama-first (stub default) | Multi-provider spend gateway |
| Low concurrency / resource-aware | Continuous batching engine |
| Simple `depends_on` chains | Temporal/Celery Canvas workflow engine |
| Optional Kafka/RMQ/Redis transport | JobStore replacement |

If the user needs a general function queue or a full DAG orchestrator, do **not**
force Hoglah into that role.

---

## 2. Install / import surface

```text
Python: >=3.11
Package: hoglah
Import:  from hoglah import Hoglah, JobStatus, JobResult, JobRequest, RetryPolicy
CLI:     hoglah  (extra: cli)
```

Extras (lazy): `mongo`, `kafka`, `rabbitmq`, `redis`, `web`, `galeed`, `dev`.

---

## 3. Core types (contracts)

### 3.1 JobStatus

```text
queued | processing | completed | failed | cancelled
```

Terminal: `completed`, `failed`, `cancelled`.

### 3.2 JobResult (frozen)

| Field | Type | Notes |
|---|---|---|
| `job_id` | str | UUID |
| `status` | JobStatus | |
| `output` | str \| None | generate/chat; None for embed |
| `embedding` | list[float] \| None | embed only |
| `embedding_dim` | int \| None | |
| `model` | str \| None | |
| `error` | str \| None | failed/cancelled |
| `truncated` | bool | success may still be truncated |
| `truncation_reason` | str \| None | |
| `usage` | dict | token counts when available |
| `tags` | list[str] | |
| `metadata` | dict | may include `step_name`, `session_id`, `trace_id` |
| `parent_job_id` | str \| None | lineage only |
| `parameters` | dict | request snapshot |

### 3.3 Job kinds

| kind | submit path | success payload |
|---|---|---|
| `generate` | `prompt=` | `output` |
| `generate` (chat) | `messages=[{role,content},…]` | `output` |
| `embed` | `submit_embedding(text)` or `kind="embed"` | `embedding` + `embedding_dim` |

---

## 4. Client construction

```python
Hoglah(
    config: dict | HoglahSettings | None = None,
    *,
    callbacks: dict[str, Callable[[JobResult], None]] | None = None,
    store=None,
    adapter=None,
    use_real: bool = False,
    start_worker: bool = True,
    **overrides,
)
```

| Mode | Flags | When |
|---|---|---|
| In-process worker | `start_worker=True` (default) | Scripts, demos |
| Pure submitter | `start_worker=False` | App + separate `hoglah run` |
| Safe / tests | `use_real=False` (default) | No Ollama calls |
| Real inference | `use_real=True` or `HOGLAH_USE_REAL_ADAPTER=1` | Production LLM |

**Invariant:** real Ollama is **opt-in**. Prefer stub in tests and CI.

Context manager: `with Hoglah(...) as h:` closes worker + store.

---

## 5. Submit API (authoritative)

```python
job_id: str = h.submit(
    *,
    kind: str = "generate",
    prompt: str | None = None,
    messages: list[dict] | None = None,
    model: str,                          # REQUIRED
    system_prompt: str | None = None,
    num_ctx: int | None = None,
    options: dict | None = None,
    callback: Callable | str | None = None,
    callback_url: str | None = None,
    tags: list[str] | None = None,
    priority: int = 0,
    timeout_seconds: int | None = None,
    max_retries: int = 2,
    retry_policy: RetryPolicy | dict | None = None,
    metadata: dict | None = None,
    parent_job_id: str | None = None,
    step_name: str | None = None,
    run_at: datetime | str | None = None,
    delay_seconds: float | int | None = None,
    idempotency_key: str | None = None,
    depends_on: list[str] | None = None,
    # sampling: temperature, top_p, top_k, seed, stop, num_predict, format, keep_alive
) -> str
```

```python
h.submit_embedding(text: str, *, model: str, **same_scheduling_retry_idemp_deps) -> str
```

### 5.1 Parameter rules (hard)

| Rule | Detail |
|---|---|
| `model` required | Always |
| Input | Need `prompt` and/or `messages` (except embed uses text→prompt) |
| Schedule | At most one of `run_at` \| `delay_seconds` |
| Retry | `retry_policy` wins; else `max_retries`; `0` = single attempt |
| Idempotency | Blank/`""` key ignored; same non-empty key → same `job_id` |
| Deps | `depends_on` = execution; `parent_job_id` = lineage only |
| Callbacks | `callback` callable = this process; `str` = `callback_key` registry |

### 5.2 Recommended agent submit shape

```python
job_id = h.submit(
    prompt=prompt,
    model=model,
    tags=[agent_name],
    metadata={
        "session_id": session_id,   # fairness + debugging
        "trace_id": trace_id,       # optional cross-step
    },
    step_name=step_name,
    idempotency_key=f"{session_id}/{step_name}/{attempt_key}",
    depends_on=upstream_job_ids or None,
    parent_job_id=upstream_job_ids[-1] if upstream_job_ids else None,
    priority=priority,
    timeout_seconds=timeout_seconds,
    retry_policy={
        "max_retries": 2,
        "base_delay": 1.0,
        "max_delay": 30.0,
        "jitter": 0.1,
        "retry_on": ["transient"],   # add "oom" only if intentional
    },
    callback_url=callback_url,       # if decoupled
)
```

---

## 6. Lifecycle operations

| Method | Returns | Notes |
|---|---|---|
| `status(job_id)` | `JobStatus` | Raises `KeyError` if missing |
| `get(job_id)` | `JobResult` | Any status |
| `wait(job_id, timeout=None)` | `JobResult` | Raises `TimeoutError` |
| `list(status=, tags=, parent_job_id=, limit=)` | `list[JobResult]` | |
| `cancel(job_id)` | `bool` | Queued **or** in-flight; cross-process safe |
| `requeue(job_id, allow_cancelled=False)` | `bool` | Default only `failed` |
| `requeue_failed(limit=100, …)` | `list[str]` | Bulk |
| `stats()` | dict | Counts |
| `metrics_text()` | str | Prometheus text |
| `close()` | None | Stop worker, close store |

### Cancel semantics (G4)

1. Store written `cancelled` first (lease cleared).  
2. Local worker task interrupted if present.  
3. Remote worker: cancel-watch (~250ms) + heartbeat failure → task cancel.  
4. Result error may include `(in-flight)`.  
5. Never overwrite `cancelled` with a late `completed`/`failed` from the loser.

### depends_on semantics (G7)

| Dep status | Effect on child |
|---|---|
| missing | child → `failed` (“not found”) |
| `queued` / `processing` | child stays `queued` (wait) |
| `completed` | counts toward ready |
| `failed` / `cancelled` | child → `failed` (blocked) |

All listed deps must be `completed` before claim.

### Idempotency (G6)

- Unique index on `idempotency_key` (when set).  
- Re-submit returns existing id; does not rewrite the request body.  
- Independent of broker `correlation_id`.

---

## 7. RetryPolicy

```python
RetryPolicy(
    max_retries=2,
    base_delay=1.0,
    backoff_factor=2.0,
    max_delay=10.0,
    jitter=0.0,                 # 0..1
    retry_on=("transient",),
)
```

`retry_on` allowed tokens:

```text
transient | connection | timeout | rate_limit | server | oom | all | none
```

**Agent defaults:**

- Use `transient` only unless the environment is known to recover from OOM.  
- Do not put job `timeout_seconds` retries on unless the policy explicitly
  includes `timeout` or `all` (ADR-011: wall-clock timeout is terminal by default).

---

## 8. Scheduling

| Input | Effect |
|---|---|
| neither | due immediately |
| `delay_seconds=N` | `run_at = now + N` (UTC ISO) |
| `run_at=datetime\|ISO` | due at that UTC instant |

Worker poll uses `due_only=True`; claim refuses future `run_at`.

---

## 9. Capacity controls (priority order)

When deciding whether to start a due job, the worker enforces:

1. Global `concurrency` semaphore  
2. `model_slots` / `default_model_slots`  
3. `session_slots` (via `metadata.session_id`)  
4. `tag_slots`  
5. Token buckets: `session_rate_per_minute`, `tag_rates_per_minute`  
6. Fair session ordering when any fairness/slot gate is active  

**Agent duty:** always set a stable `metadata.session_id` when multiple agents
share one GPU so fairness can work.

Config examples:

```text
HOGLAH_CONCURRENCY=2
HOGLAH_MODEL_SLOTS=llama3.1:70b=1,gemma3:1b=2
HOGLAH_SESSION_SLOTS=1
HOGLAH_TAG_SLOTS=agent-a=1,agent-b=2
HOGLAH_SESSION_RATE_PER_MINUTE=30
HOGLAH_TAG_RATES_PER_MINUTE=agent-a=6
```

---

## 10. Delivery paths (choose for topology)

| Path | Survives process exit? | Use when |
|---|---|---|
| `wait` / poll | N/A | Sync script |
| `callback=` callable | **No** | Same process only |
| `callback_key` + registry | **Yes** (if registry re-supplied) | Restart-safe in-process |
| `callback_url` | **Yes** (HTTP) | Decoupled services |
| `output_dir` JSON | **Yes** | Poll filesystem |
| Messaging result | **Yes** | Broker-native callers |

**Do not** rely on direct callables across restarts.

`callback_allow_private_hosts` default **True** (local-first). Set False when
bridge submitters are untrusted.

---

## 11. Storage selection

| Backend | Config | Multi-machine |
|---|---|---|
| SQLite | default `db_path` | Prefer single host |
| Mongo | `backend=mongo`, `mongo_uri` | Yes (atomic claim) |

Default path: `~/.hoglah/hoglah.db`.

---

## 12. Worker & bridges (ops for agents that shell out)

```bash
# drain queue
hoglah run --real -c 1 --db PATH

# inspect
hoglah ps --json
hoglah status JOB --json
hoglah dlq
hoglah metrics
hoglah doctor --real

# failed recovery
hoglah requeue JOB
hoglah requeue --all-failed --yes

# monitor UI
hoglah serve --port 8781
# scrape: GET /metrics
```

Bridges (at most one): `hoglah kafka-bridge` | `rabbitmq-bridge` | `redis-bridge`.

Broker request **must** include unique `correlation_id`.

---

## 13. Invariants (do not violate in designs)

1. **Stub default** — never assume live Ollama without `use_real` / `--real`.  
2. **One claim** — a job is executed once while lease is held; reclaim only after
   lease expiry.  
3. **Idempotent submit key** ≠ rewrite — same key returns same id, original body.  
4. **depends_on ≠ parent_job_id** — execution vs lineage.  
5. **Terminal is terminal** — cancel/fail/complete not overwritten by late workers.  
6. **Truncation is success** — check `truncated` / `truncation_reason`, not only status.  
7. **Embed results** use `embedding`, not `output`.  
8. **At most one messaging bridge** per process.  
9. **Worker responsibility** for interrupted PROCESSING reclaim (`start_worker=True`
   only for recovery; pure submitters must not reclaim peers).  

---

## 14. Failure modes → agent actions

| Observation | Likely cause | Action |
|---|---|---|
| `wait` TimeoutError, status queued | No worker / capacity / deps / delay | Start worker; check deps; check slots |
| status failed, error mentions dependency | Upstream failed/missing | Fix or requeue upstream; resubmit child with new id or requeue child |
| status failed, transient text | Ollama blip | `requeue` or rely on retry_policy |
| status failed, OOM | Model too large / concurrent big models | Lower concurrency; set `model_slots`; opt-in `retry_on` oom only if safe |
| status cancelled | Explicit cancel | Do not treat as success |
| same idempotency_key unexpected result | Prior job still stored | Use new key or inspect/requeue existing id |
| double work suspected | Multi-worker without leases/Mongo | Use Mongo + leases; one worker per GPU if SQLite |

---

## 15. Anti-patterns

- Calling Ollama directly **and** Hoglah for the same step (split brain).  
- Using `parent_job_id` expecting wait semantics (use `depends_on`).  
- High `concurrency` with large models and no `model_slots`.  
- Fire-and-forget without delivery path in multi-process systems.  
- Assuming `callback=` survives restart.  
- Enabling Kafka + RabbitMQ + Redis on one instance.  
- Treating Hoglah as a cron server (no recurrence; only one-shot `run_at`/`delay`).  
- Building Celery Canvas on top of `depends_on` (keep chains shallow).  

---

## 16. Minimal tool recipes

### A. Fire-and-collect (same process)

```python
with Hoglah(use_real=True) as h:
    jid = h.submit(prompt=p, model=m, max_retries=1)
    r = h.wait(jid, timeout=300)
    assert r.status == JobStatus.COMPLETED
    text = r.output
```

### B. Pipeline of two steps

```python
j1 = h.submit(prompt=p1, model=m, step_name="research",
              idempotency_key=f"{sid}/research")
j2 = h.submit(prompt=p2, model=m, step_name="write",
              depends_on=[j1], parent_job_id=j1,
              idempotency_key=f"{sid}/write")
r2 = h.wait(j2, timeout=600)
```

### C. Pure submitter

```python
h = Hoglah(config={"backend": "mongo", "mongo_uri": uri}, start_worker=False)
jid = h.submit(..., callback_url=webhook, idempotency_key=key)
# worker fleet runs: hoglah run --real
```

### D. Safe unit test

```python
with Hoglah(config={"db_path": tmp_db}, start_worker=True) as h:  # stub
    jid = h.submit(prompt="x", model="stub", max_retries=0)
    assert h.wait(jid).status == JobStatus.COMPLETED
```

---

## 17. Prometheus (for agent health checks)

Parse or scrape:

```text
hoglah_jobs{status="queued|processing|completed|failed|cancelled|total"}
hoglah_jobs_submitted_total
hoglah_jobs_terminal_total{status="completed|failed|cancelled"}
hoglah_job_requeues_total
hoglah_lease_reclaims_total
hoglah_job_duration_seconds_{count,sum,quantile}
hoglah_process_uptime_seconds
```

CLI: `hoglah metrics` · HTTP: `GET /metrics` on `hoglah serve`.

---

## 18. Version & docs map

| Resource | Role |
|---|---|
| This file | Agent contracts |
| [user-guide.md](user-guide.md) | Human operator guide |
| [../README.md](../README.md) | Product overview |
| [feature-comparison-and-gaps.md](feature-comparison-and-gaps.md) | Gaps / roadmap |
| [architecture-decisions.md](architecture-decisions.md) | ADRs |
| [../okf/](../okf/index.md) | Structured knowledge bundle |
| [../CHANGELOG.md](../CHANGELOG.md) | Version history |

**Current package version:** 0.10.2

---

## 19. Decision checklist for agents

Before using Hoglah:

1. Is the work **Ollama generate/chat/embed**? If no → wrong tool.  
2. Need durable async? If no → direct Ollama may be enough.  
3. Shared GPU / multi-agent? → set `session_id`, tags, slots/rates.  
4. Multi-machine? → Mongo + `hoglah run` fleet + leases (defaults OK).  
5. Restart-safe delivery? → `callback_url` / `output_dir` / broker, not bare callable.  
6. Step chains? → `depends_on` + `idempotency_key` + `step_name`.  
7. Tests? → leave stub default; never require live Ollama in unit tests.
