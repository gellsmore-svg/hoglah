# Hoglah

**Hoglah** is a lightweight, local-first job queue manager and Ollama wrapper designed for resource-constrained environments.

It lets applications submit LLM inference requests (generate or chat) asynchronously, receive a job ID immediately, monitor progress, retrieve full results, and receive completion callbacks — even when the underlying hardware can only run one (or very few) model inferences at a time.

Named after one of the daughters of Zelophehad (Numbers 26/27/36, Joshua 17), continuing the Old Testament women's names pattern used by sister projects in the domains family (a separate application, a name, etc.).

## Core Value Proposition

- Simple Python-native interface for internal/library use
- Reliable queuing with durable persistence (survives restarts)
- Smart handling of context windows and model capabilities
- Fire-and-forget + callback patterns for workflow orchestration
- Fully local, privacy-focused, zero-cloud dependency
- Extensible foundation (web API / webhooks / distributed backends planned for later versions)

**Target users**: Developers building multi-agent systems, background task processors, or local AI tooling that needs to safely queue and manage LLM calls.

## Goals (V1)

- Clean, reliable abstraction over Ollama for queuing
- Configurable concurrency (default: 1 for low-resource setups)
- Model discovery, context calibration, and basic resource awareness
- Easy integration into existing Python applications
- Persistent job state across process restarts
- Keep V1 simple, focused, and production-ready for local use

## Non-Goals (V1)

- Full distributed orchestration or high-availability clustering
- Built-in web UI (deferred to V2)
- Advanced authentication / multi-tenancy
- Non-Ollama backends
- Real-time streaming UI surfaces (file + callback sufficient)

## Installation

### From PyPI (recommended)
```bash
pip install hoglah
# With the CLI
pip install "hoglah[cli]"
# With the MongoDB backend (optional — adds pymongo)
pip install "hoglah[mongo]"
# With the Kafka bridge (optional — adds confluent-kafka)
pip install "hoglah[kafka]"
# With the RabbitMQ bridge (optional — adds pika)
pip install "hoglah[rabbitmq]"
# With the Redis Streams bridge (optional — adds redis)
pip install "hoglah[redis]"
```
Hoglah is published on PyPI: https://pypi.org/project/hoglah/

### From GitHub Releases (no PyPI needed)
Every `vX.Y.Z` tag publishes a GitHub Release with a wheel + sdist:

```bash
# Latest wheel
pip install "hoglah[cli] @ https://github.com/gellsmore-svg/hoglah/releases/latest/download/hoglah-0.2.2-py3-none-any.whl"

# Or a specific version
pip install "hoglah[cli] @ https://github.com/gellsmore-svg/hoglah/releases/download/v0.2.2/hoglah-0.2.2-py3-none-any.whl"
```

### From source (for development)
```bash
git clone https://github.com/gellsmore-svg/hoglah
cd hoglah
python -m venv .venv
.venv/bin/pip install -e ".[dev,cli]"
```

### Maintainers: releasing

Releases are automated. Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`,
which builds the wheel + sdist, creates the GitHub Release, and publishes to PyPI
via OIDC trusted publishing (no API token stored). PyPI trusted publishing is
already configured for this repo (publisher: `gellsmore-svg/hoglah`, workflow
`release.yml`, no environment). So a release is just:

```bash
# bump version in pyproject.toml + update CHANGELOG, commit, then:
git tag vX.Y.Z && git push origin vX.Y.Z
```

## Quick Start (Planned)

Once implemented:

```bash
git clone https://github.com/gellsmore-svg/hoglah
cd hoglah
python -m venv .venv && .venv/bin/pip install -e ".[dev,cli]"
```

```python
from hoglah import Hoglah

h = Hoglah()  # or Hoglah(config_path="...")

job_id = h.submit(
    prompt="Explain the significance of Hoglah in the biblical land allotment.",
    model="gemma3:1b",
    tags=["research", "bible"],
    callback=lambda result: print("Done:", result.job_id, result.output[:100]),
)

print("Submitted:", job_id)
print(h.status(job_id))

result = h.wait(job_id, timeout=120)
print(result.output)

# Recommended: context manager for auto cleanup of the background worker
with Hoglah() as h:
    job_id = h.submit(prompt="...", model="gemma3:1b")
    print(h.wait(job_id).output)
```

CLI:

```bash
hoglah submit "Explain Hoglah" --model gemma3:1b --wait
hoglah list --status completed
hoglah ps --json                 # alias for list, machine-readable
hoglah stats --json              # queue overview (counts by status)
hoglah info --json               # config + adapter + log_level + stats snapshot
hoglah show gemma3:1b --json     # model details (context, template, etc.)
hoglah clear --status completed --older-than 7 --yes  # prune old jobs
hoglah rm <job-id> --yes  # remove specific job
hoglah wait <job-id> --timeout 60 --json  # block until done, machine readable
hoglah doctor --real  # diagnose setup and real Ollama/llama.cpp connectivity
hoglah status <job-id> --json
```

## Backends

Hoglah stores jobs through a pluggable `JobStore`. Two are built in; the default
needs no setup.

### SQLite (default)

A single file (`~/.hoglah/hoglah.db` by default), zero extra dependencies. Best
for one machine. The store uses write-ahead logging + a busy timeout so a
submitter and the worker can share the file without "database is locked" errors.

```python
h = Hoglah()  # SQLite at ~/.hoglah/hoglah.db
h = Hoglah(config={"db_path": "/data/queue.db"})
```

### MongoDB (optional)

Point Hoglah at a MongoDB **server** instead. Install the extra (`pymongo`):

```bash
pip install "hoglah[mongo]"
```

```python
h = Hoglah(config={
    "backend": "mongo",
    "mongo_uri": "mongodb://localhost:27017",  # default
    "mongo_db": "hoglah",                       # default
    "mongo_collection": "jobs",                 # default
})
# or via environment:
#   HOGLAH_BACKEND=mongo HOGLAH_MONGO_URI=mongodb://host:27017
```

Everything else — `submit`, `wait`, the CLI, callbacks, recovery — works
identically. Use Mongo when you want:

- **No single-file lock.** A server has no SQLite file-locking concern, and job
  claiming is atomic server-side (`find_one_and_update`), so **multiple workers,
  even on different machines**, can drain one shared queue and still run each job
  exactly once.
- **External visibility.** Jobs are stored as native documents (request, result
  and tags are sub-documents, not opaque JSON blobs), so you can watch the queue
  live from `mongosh` or Compass:

  ```js
  use hoglah
  db.jobs.find({ status: "queued" }).sort({ priority: -1, created_at: 1 })
  db.jobs.aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }])
  ```

## Kafka bridge (optional)

Hoglah can bridge an existing Apache Kafka deployment **without owning the
cluster**: it consumes job-request messages from an input topic into the durable
queue, processes them with the normal serial worker, and produces result
messages back to Kafka. It is a *transport adapter*, not a storage backend — the
SQLite/Mongo store remains the source of truth, and Kafka is a third decoupled
delivery mechanism alongside the output folder and HTTP callback.

```bash
pip install "hoglah[kafka]"
# run the worker + bridge in the foreground (Ctrl-C to stop)
hoglah kafka-bridge --bootstrap-servers localhost:9092 \
                    --input-topic hoglah-jobs --results-topic hoglah-results
```

Or from the library / env:

```python
h = Hoglah(config={
    "kafka_enabled": True,
    "kafka_bootstrap_servers": "localhost:9092",
    "kafka_input_topic": "hoglah-jobs",
    "kafka_results_topic": "hoglah-results",   # overridable per-message by reply_to
})
# env equivalent: HOGLAH_KAFKA_ENABLED=1 HOGLAH_KAFKA_BOOTSTRAP_SERVERS=...
```

**Input message** (JSON on `hoglah-jobs`): a unique `correlation_id` is required
and doubles as the idempotency key.

```json
{ "correlation_id": "9b1c…", "model": "gemma3:1b", "prompt": "…",
  "options": { "temperature": 0.7 }, "reply_to": "team-x-replies" }
```

**Output message** (on `hoglah-results` or the request's `reply_to`) echoes the
`correlation_id` so async callers can match it:

```json
{ "correlation_id": "9b1c…", "job_id": "…", "status": "completed",
  "output": "…", "error": null }
```

**Crash safety is built in** (see `docs/kafka-bridge-design.md`):
- *Ingress* commits the Kafka offset only **after** a durable, idempotent enqueue
  keyed on `correlation_id` — a redelivery after a crash is a harmless no-op
  (no lost or duplicated jobs).
- *Egress* uses a transactional outbox — a result is marked published only after
  the broker acks; on restart, computed-but-unpublished results are re-emitted.
- Poison (un-parseable) messages go to a dead-letter topic so they never block a
  partition.

Scale horizontally by running several `hoglah kafka-bridge` processes in one
consumer group; each keeps its own serial worker. Sharing one MongoDB store
gives fleet-wide exactly-once execution via the server-side atomic claim.

## RabbitMQ bridge (optional)

The same bridge runs over RabbitMQ (AMQP) instead of Kafka — identical message
contract and crash-safety guarantees, a different broker. Use it if you run
RabbitMQ rather than a Kafka cluster.

```bash
pip install "hoglah[rabbitmq]"
hoglah rabbitmq-bridge --url amqp://guest:guest@localhost:5672/ \
                       --input-queue hoglah-jobs --results-queue hoglah-results
```

```python
h = Hoglah(config={
    "rabbitmq_enabled": True,
    "rabbitmq_url": "amqp://guest:guest@localhost:5672/",
    "rabbitmq_input_queue": "hoglah-jobs",
    "rabbitmq_results_queue": "hoglah-results",
})
```

Same input/output messages as the Kafka bridge (a `correlation_id` is required
and echoed back). RabbitMQ's per-message model is a particularly clean fit:
ingress acks each message after a durable enqueue (no head-of-line blocking),
poison messages are `nack`'d to a **dead-letter exchange**, and egress uses
**publisher confirms** so a result is marked delivered only after the broker
acks it. Enable at most one of the Kafka / RabbitMQ / Redis bridges per instance.

## Redis Streams bridge (optional)

The lightest-weight option — Redis is often already in the stack. Same message
contract and crash-safety guarantees.

```bash
pip install "hoglah[redis]"
hoglah redis-bridge --url redis://localhost:6379/0 \
                    --input-stream hoglah-jobs --results-stream hoglah-results
```

```python
h = Hoglah(config={
    "redis_enabled": True,
    "redis_url": "redis://localhost:6379/0",
    "redis_input_stream": "hoglah-jobs",
    "redis_results_stream": "hoglah-results",
    "redis_consumer_name": "hoglah-1",   # stable across restarts for crash recovery
})
```

Uses a consumer group with explicit `XACK` after a durable enqueue; a
consumer's unacked entries (its Pending Entries List) are recovered on restart
via a stable consumer name, so a crash mid-processing re-delivers rather than
loses. Poison messages go to a dead-letter stream. Run several `redis-bridge`
processes (distinct `--consumer-name`s) in one group to scale out.

## V1 Scope

Hoglah 0.2.1 implements the full V1 specification from `docs/requirements-v1.0.md` and `docs/project-brief.md`.

**Included (V1):**
- Submit (prompt or messages/chat), immediate UUID.
- Status, get result (with output, usage, timings, metadata, parent, **truncated** reporting + effective_num_ctx).
- List (status, tags, **parent_job_id** filters; rich human + --json with preview).
- Cancel (best-effort).
- Wait (standalone or via submit --wait).
- rm / clear (per-job or bulk by status/age).
- info / stats (config, adapter, queue overview).
- Models: list + show (details, context size, template, family).
- pull (auto on real submit, or explicit).
- run (foreground worker).
- In-process callbacks (direct + named registry for restart re-delivery).
- Restart recovery (interrupted jobs + callback re-delivery).
- Pluggable adapters (safe Stub default + real Ollama with auto-pull, model-aware context, truncation via done_reason).
- Configurable concurrency (default 1), log_level, db, ollama host.
- Full submit surface (temperature, top_p/k, num_ctx, format, keep_alive, metadata, parent, etc.).
- Persistence (SQLite), context manager, --json everywhere.

**Explicitly not in V1 (per non-goals):**
- Web UI / HTTP server (V2).
- Webhooks / callback_url.
- Distributed / multi-node.
- Non-Ollama backends.
- Complex dependency graph execution (parent_job_id is for traceability only; no automatic waiting/fan-out).
- Real-time streaming UI (polling wait + final callbacks sufficient).

See the full requirements review and V1 completeness note in `.restart.md`.

You can also run the packaged install smoke test after installing the wheel:
```bash
python scripts/test_packaged_install.py
```

To validate with your working local Ollama (full real adapter paths including show, pull, context auto-detect):
```bash
RUN_OLLAMA_TESTS=1 python scripts/test_packaged_install.py
# or
HOGLAH_USE_REAL_ADAPTER=1 python scripts/test_packaged_install.py
```

**Real Ollama / llama.cpp:** Opt-in via `use_real=True` / `HOGLAH_USE_REAL_ADAPTER=1` / `--real`. The "real" adapter talks to Ollama (which uses llama.cpp for inference).

**Real-Ollama validation status:** v0.2.2 has been validated end-to-end against a live Ollama (submit → worker → real inference, plus the gated integration test and the packaged-wheel smoke test in real mode). To reproduce on your machine:
```bash
python3 -m venv /tmp/hoglah-validate
/tmp/hoglah-validate/bin/pip install "hoglah[cli]"            # from PyPI
RUN_OLLAMA_TESTS=1 /tmp/hoglah-validate/bin/python scripts/test_packaged_install.py

# Or the gated integration test from a source checkout
RUN_OLLAMA_TESTS=1 python -m pytest tests/test_worker_execution.py::test_real_ollama_adapter_end_to_end -q -s
```

**WSL2 note:** if Ollama runs as the Windows binary and your code runs in WSL, the daemon is *not* reachable at `localhost` over HTTP. Set `OLLAMA_HOST=0.0.0.0` on the Windows side (`setx OLLAMA_HOST "0.0.0.0"`, then restart Ollama) and point the client at the WSL2 gateway IP, e.g.:
```bash
OLLAMA_HOST="http://$(ip route show default | awk '{print $3}'):11434" \
  RUN_OLLAMA_TESTS=1 python scripts/test_packaged_install.py
```
```bash
hoglah cancel <job-id>
hoglah models
hoglah run --real                # foreground worker using real Ollama
```

By default `hoglah` and `Hoglah()` use the safe stub adapter (no LLM calls). Use `--real` (CLI) or pass `adapter=OllamaAdapter(...)` (library) when you want actual inference.

`hoglah --version` / `-V` and `hoglah version` are supported. Use `with Hoglah(...) as h:` for automatic cleanup.

CLI now also includes `hoglah ps` (list alias) and `--json` output on list/ps/status/models. `hoglah submit` supports `--metadata` (JSON) and `--parent-job-id`. Real integration tests are gated behind `RUN_OLLAMA_TESTS=1`.

See `docs/requirements-v1.0.md` for the full initial specification.

## Submit API (Initial Draft)

```python
job_id = hoglah.submit(
    prompt: str | None = None,                    # or messages for chat
    messages: list[dict] | None = None,           # OpenAI-style chat history
    model: str,                                   # e.g. "gemma:7b", "mistral"
    system_prompt: str | None = None,
    num_ctx: int | None = None,                   # Context window size
    options: dict | None = None,                  # Passthrough for llama.cpp params
    callback: Callable[[JobResult], None] | None = None,  # Python callable
    callback_url: str | None = None,              # V2: HTTP webhook
    tags: list[str] | None = None,
    priority: int = 0,                            # Higher = earlier
    timeout_seconds: int | None = None,
    max_retries: int = 2,
    metadata: dict | None = None,                 # User-defined data
    parent_job_id: str | None = None,             # For chaining/dependencies
    temperature: float | None = None,
    top_p: float | None = None,
    top_k: int | None = None,
    repeat_penalty: float | None = None,
    seed: int | None = None,                      # Reproducibility
    stop: list[str] | None = None,                # Stop sequences
    num_predict: int | None = None,               # Max output tokens
    format: str | None = None,                    # e.g. "json"
    keep_alive: str | int | None = None,
    # ... full options dict covers the rest
)
```

## Current Status

**2026-06-12 (updated)**: Core implementation complete (Chunks 1-3 + follow-on polish).

- Full durable queue + background asyncio worker (concurrency=1 default)
- Pluggable adapters: `StubAdapter` (default, safe) + `OllamaAdapter` (real, opt-in via `use_real=True` or `--real`)
- `Hoglah(use_real=True)` convenience + `HOGLAH_USE_REAL_ADAPTER` env var
- Submit (prompt **or** messages/chat), rich generation params, status, get, list, cancel, wait, named+direct callbacks
- Restart recovery (interrupted jobs + callback re-delivery)
- Truncation metadata always surfaced (never fails the job)
- CLI: `list`, `status`, `cancel`, `submit` (with --messages, --temperature, --num-ctx etc.), `run`, `models`, `version`
- `examples/basic_usage.py` demonstrating the common patterns
- 26 passing tests (+1 gated real-Ollama test that passes against a live server); the default suite needs no Ollama (stub adapter).

See `docs/requirements-v1.0.md`, `docs/architecture-decisions.md`, and `.restart.md` for history and how to continue.

See sister domains for style and quality references:
- [a separate application](https://github.com/gellsmore-svg/a separate application)
- [a name](https://github.com/gellsmore-svg/removed-project)

## Architecture Sketch (Early)

- Client library (`Hoglah` or similar) for submit / status / wait / list / cancel
- SQLite-backed job store (jobs table + results / events)
- Worker loop (thread or task) with concurrency semaphore
- Ollama adapter (generate + chat paths, model info)
- In-process callback dispatch after completion
- CLI entrypoint for inspection and operations
- Config via constructor + env + small config file

Full details will evolve in `docs/architecture-decisions.md` and implementation docs.

## License

Apache 2.0 — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
