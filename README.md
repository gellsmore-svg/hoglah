# Hoglah

**Hoglah** is a lightweight, local-first job queue for [Ollama](https://ollama.com).
It lets an application submit LLM inference work — text generation, chat, or
embeddings — asynchronously: you get a job ID immediately, the work runs on a
durable background queue, and you collect the result by polling, an in-process
callback, an HTTP callback, or a written output file.

It is built for resource-constrained, single-machine setups where the hardware
can only run one (or very few) model inferences at a time, but scales out to
multiple workers and machines when you point it at a shared backend.

## Features

- **Submit and forget** — `submit()` returns a job ID instantly; the work runs on
  a background worker.
- **Generate, chat, and embeddings** — one queue for all three job kinds.
- **Durable and restart-safe** — jobs and results persist; interrupted jobs and
  undelivered callbacks are recovered on restart.
- **Configurable concurrency** — default 1, tune for your hardware.
- **Context-aware** — auto-detects a model's context window and reports truncation
  rather than failing.
- **Four result-delivery paths** — poll/`wait()`, in-process callbacks (including a
  named registry that survives restarts), per-job HTTP callbacks, and an
  output-folder drop.
- **Pluggable storage** — SQLite by default (zero setup), MongoDB for
  multi-worker / multi-machine queues.
- **Messaging bridges** — consume job requests from / publish results to **Kafka**,
  **RabbitMQ**, or **Redis Streams**, all crash-safe.
- **Safe by default** — ships with a deterministic stub adapter; real Ollama
  inference is explicitly opt-in.
- **Rich CLI** — submit, inspect, wait, prune, diagnose, run a worker, and a live
  `monitor` (auto-refreshing status counts, throughput, and recent jobs).

## Installation

```bash
pip install hoglah               # library
pip install "hoglah[cli]"        # + command-line interface
```

Optional backends and transports are separate extras (all lazy-imported, so the
base install stays dependency-light):

```bash
pip install "hoglah[mongo]"      # MongoDB backend (pymongo)
pip install "hoglah[kafka]"      # Kafka bridge (confluent-kafka)
pip install "hoglah[rabbitmq]"   # RabbitMQ bridge (pika)
pip install "hoglah[redis]"      # Redis Streams bridge (redis)
```

Published on PyPI: <https://pypi.org/project/hoglah/>. Requires Python 3.11+.

## Quick start

```python
from hoglah import Hoglah

# Context manager cleans up the background worker on exit.
with Hoglah(use_real=True) as h:        # omit use_real for the safe stub adapter
    job_id = h.submit(
        prompt="Explain context windows in one sentence.",
        model="gemma3:1b",
        tags=["demo"],
    )
    print("submitted:", job_id)

    result = h.wait(job_id, timeout=120)
    print(result.status, "->", result.output)
```

Prefer callbacks over blocking? Pass one to `submit()`:

```python
h.submit(
    prompt="...", model="gemma3:1b",
    callback=lambda r: print("done:", r.job_id, r.output[:80]),
)
```

### CLI

```bash
hoglah submit "Explain Hoglah" --model gemma3:1b --wait --real
hoglah ps --json                  # list jobs (machine-readable)
hoglah status <job-id> --json
hoglah wait <job-id> --timeout 60
hoglah stats                      # queue overview (counts by status)
hoglah models --real              # available models
hoglah show gemma3:1b             # model details (context size, template, ...)
hoglah clear --status completed --older-than 7 --yes
hoglah rm <job-id> --yes
hoglah run --real                 # run a foreground worker (dedicated processor)
hoglah doctor --real              # diagnose setup, backend, transport, connectivity
```

By default both `Hoglah()` and the CLI use a **safe stub adapter** that makes no
model calls — handy for tests and shared environments. Opt into real inference
with `use_real=True` (library), `--real` (CLI), or `HOGLAH_USE_REAL_ADAPTER=1`.

## Job kinds

### Generate and chat

```python
h.submit(prompt="Summarise this paragraph: ...", model="gemma3:1b")
h.submit(
    messages=[                                   # OpenAI-style chat history
        {"role": "system", "content": "You are concise."},
        {"role": "user", "content": "Define entropy."},
    ],
    model="gemma3:1b",
)
```

### Embeddings

```python
job_id = h.submit_embedding("text to embed", model="bge-m3")
result = h.wait(job_id)
print(result.embedding_dim, len(result.embedding))   # e.g. 1024 1024
```

## Result delivery

A terminal result can reach you four ways — use whichever fits, or several at once:

1. **Poll / wait** — `h.status(job_id)` or `h.wait(job_id, timeout=...)`.
2. **In-process callback** — `submit(..., callback=fn)`. For delivery that
   survives a restart, register a named callback and pass `callback_key=...`.
3. **HTTP callback** — `submit(..., callback_url="https://...")` POSTs the
   `JobResult` JSON when the job finishes, on a background thread with retries and
   backoff (a slow or dead endpoint never blocks the worker).
4. **Output folder** — set `output_dir` (or `HOGLAH_OUTPUT_DIR`) and each terminal
   job is written atomically to `<output_dir>/<job_id>.json`.

## Backends

Hoglah stores jobs through a pluggable `JobStore`. Two are built in; the default
needs no setup.

### SQLite (default)

A single file (`~/.hoglah/hoglah.db` by default), zero extra dependencies. Best
for one machine. The store uses write-ahead logging + a busy timeout so a
submitter and the worker can share the file without "database is locked" errors.

```python
h = Hoglah()                                      # SQLite at ~/.hoglah/hoglah.db
h = Hoglah(config={"db_path": "/data/queue.db"})
```

### MongoDB (optional)

Point Hoglah at a MongoDB **server** instead (`pip install "hoglah[mongo]"`):

```python
h = Hoglah(config={
    "backend": "mongo",
    "mongo_uri": "mongodb://localhost:27017",  # default
    "mongo_db": "hoglah",                       # default
    "mongo_collection": "jobs",                 # default
})
# env equivalent: HOGLAH_BACKEND=mongo HOGLAH_MONGO_URI=mongodb://host:27017
```

Everything else — `submit`, `wait`, the CLI, callbacks, recovery — works
identically. Use Mongo when you want:

- **No single-file lock.** Job claiming is atomic server-side
  (`find_one_and_update`), so **multiple workers, even on different machines**,
  can drain one shared queue and still run each job exactly once.
- **External visibility.** Jobs are native documents (request, result and tags are
  sub-documents, not opaque JSON blobs), so you can watch the queue live from
  `mongosh` or Compass.

## Messaging bridges

Hoglah can bridge an existing message broker **without owning it**: it consumes
job-request messages from an input topic/queue/stream into the durable queue,
processes them with the normal serial worker, and publishes result messages back.
A bridge is a *transport adapter*, not a storage backend — the SQLite/Mongo store
remains the source of truth.

All three bridges share one message contract and the same crash-safety design:

- **Input** (JSON): a unique `correlation_id` is required and doubles as the
  idempotency key. Optional `reply_to` overrides the result destination
  per-message.

  ```json
  { "correlation_id": "9b1c…", "model": "gemma3:1b", "prompt": "…",
    "options": { "temperature": 0.7 }, "reply_to": "team-x-replies" }
  ```

- **Output** echoes the `correlation_id` so async callers can match results:

  ```json
  { "correlation_id": "9b1c…", "job_id": "…", "status": "completed",
    "output": "…", "error": null }
  ```

- **Crash safety:** ingress commits the broker offset only **after** a durable,
  idempotent enqueue (a redelivery after a crash is a harmless no-op); egress uses
  a transactional outbox (a result is marked published only after the broker acks,
  and unpublished results are re-emitted on restart); poison (un-parseable)
  messages are routed to a dead-letter destination so they never block the queue.

Enable **at most one** bridge per instance (precedence kafka > rabbitmq > redis).

### Kafka

```bash
pip install "hoglah[kafka]"
hoglah kafka-bridge --bootstrap-servers localhost:9092 \
                    --input-topic hoglah-jobs --results-topic hoglah-results
```

```python
h = Hoglah(config={
    "kafka_enabled": True,
    "kafka_bootstrap_servers": "localhost:9092",
    "kafka_input_topic": "hoglah-jobs",
    "kafka_results_topic": "hoglah-results",
})
```

Scale horizontally by running several `hoglah kafka-bridge` processes in one
consumer group; sharing one MongoDB store gives fleet-wide exactly-once execution
via the server-side atomic claim.

### RabbitMQ

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

RabbitMQ's per-message model is a clean fit: ingress acks each message after a
durable enqueue (no head-of-line blocking), poison messages are `nack`'d to a
**dead-letter exchange**, and egress uses **publisher confirms**.

### Redis Streams

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

The lightest-weight option — Redis is often already in the stack. Uses a consumer
group with explicit `XACK` after a durable enqueue; a consumer's unacked entries
(its Pending Entries List) are recovered on restart via a stable consumer name, so
a crash mid-processing re-delivers rather than loses. Run several `redis-bridge`
processes (distinct `--consumer-name`s) in one group to scale out.

## Configuration

Every option can be set in the `Hoglah(config={...})` dict or via a `HOGLAH_*`
environment variable. Common ones:

| Setting | Env var | Default | Purpose |
|---|---|---|---|
| `db_path` | `HOGLAH_DB_PATH` | `~/.hoglah/hoglah.db` | SQLite database file |
| `backend` | `HOGLAH_BACKEND` | `sqlite` | `sqlite` or `mongo` |
| `concurrency` | `HOGLAH_CONCURRENCY` | `1` | Parallel jobs per worker |
| `ollama_host` | `HOGLAH_OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint |
| `output_dir` | `HOGLAH_OUTPUT_DIR` | `None` | Write terminal results here |
| `log_level` | `HOGLAH_LOG_LEVEL` | `INFO` | Logging level |

Backend- and bridge-specific keys (`mongo_*`, `kafka_*`, `rabbitmq_*`, `redis_*`)
follow the same pattern; see the examples above. `hoglah doctor` prints the active
backend and transport (and never prints connection URLs).

## Real Ollama notes

The real adapter talks to Ollama (which uses llama.cpp for inference). Enable it
with `use_real=True` / `--real` / `HOGLAH_USE_REAL_ADAPTER=1`, and make sure the
model is available (`hoglah pull <model>` or auto-pull on first real submit).

**Installing models from Hugging Face.** Ollama can pull any GGUF model straight
from the Hugging Face Hub, so `hoglah pull` does too — just use an `hf.co/...`
name with the quant as the tag:

```bash
hoglah pull --real "hf.co/bartowski/Llama-3.2-3B-Instruct-GGUF:Q4_K_M"
# then submit to it like any other model:
hoglah submit "hello" --model "hf.co/bartowski/Llama-3.2-3B-Instruct-GGUF:Q4_K_M" --real --wait
```

The model then works everywhere a model name is accepted (CLI, library, the
messaging bridges). Size it to your hardware: a model must fit in VRAM (with
spillover to system RAM, which is much slower on CPU) — frontier models like
MiniMax-M3 or Kimi-K2 (hundreds of GB) need server-class hardware, not a laptop.

**WSL2:** if Ollama runs as the Windows binary and your code runs in WSL, the
daemon is *not* reachable at `localhost`. Set `OLLAMA_HOST=0.0.0.0` on the Windows
side (`setx OLLAMA_HOST "0.0.0.0"`, then restart Ollama) and point the client at
the WSL2 gateway IP:

```bash
OLLAMA_HOST="http://$(ip route show default | awk '{print $3}'):11434" \
  hoglah doctor --real
```

## Reporting bugs & support

Open an issue at
[github.com/gellsmore-svg/hoglah/issues](https://github.com/gellsmore-svg/hoglah/issues)
— the **Bug report** template prompts for what's needed. To make a report
actionable, include:

- the output of **`hoglah doctor`** (`--real` if you use a real Ollama host) — it
  reports version, adapter, backend, and active transport, and is safe to paste
  (it never prints connection URLs/credentials);
- your Hoglah version (`hoglah --version`), Python version, and OS;
- a minimal reproduction.

**Security issues:** please report privately — see [SECURITY.md](SECURITY.md) —
rather than opening a public issue.

## Knowledge bundle

A machine- and human-readable knowledge map of Hoglah's concepts, modules, and CLI
is published as an [Open Knowledge Format](https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing)
bundle under [`okf/`](okf/index.md) — markdown with YAML frontmatter, linked into a
concept graph.

## License

Apache 2.0 — see [LICENSE](LICENSE). Named after one of the daughters of
Zelophehad (Numbers 27).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
