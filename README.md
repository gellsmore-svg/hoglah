# Hoglah

**Hoglah is a local-first job queue for LLM work.** Your code submits a prompt
and gets a job ID back immediately; a background worker runs it when the model
is free; you collect the result when it is ready.

It exists for the common single-machine problem: many callers (scripts, agents,
services) and hardware that can run only one model call, or a few, at a time.
Hoglah puts those calls in a durable line, runs them at the rate your hardware
allows, and keeps the results until you collect them.

## Is it for you?

**Good fit** — you run models locally (Ollama, an OpenAI-compatible server such
as vLLM or llama.cpp, or a headless coding CLI like `claude` or `codex`) and
need to:

- share one GPU fairly between several agents or apps;
- fire off work without blocking, and collect results later or by callback;
- keep queued work across restarts, on one machine or a small fleet.

**Not a fit** — you need a general workflow engine (Hoglah does simple
`depends_on` chains only), an inference server (batching stays in Ollama or
vLLM), or exactly-once execution (see [Know before you rely on it](#know-before-you-rely-on-it)).

## Try it

```bash
pipx install hoglah        # Python 3.11+; Windows zip and other options: docs/install.md
hoglah doctor              # checks setup; safe to paste into bug reports
```

```python
from hoglah import Hoglah

with Hoglah(use_real=True) as h:   # omit use_real to use the safe stub (no model calls)
    job_id = h.submit(prompt="Explain context windows in one sentence.",
                      model="gemma3:1b")
    print(h.wait(job_id, timeout=120).output)
```

Or from the shell: `hoglah submit "Explain Hoglah" --model gemma3:1b --wait --real`.

Hoglah **makes no model calls until you opt in** (`use_real=True`, `--real`, or
`HOGLAH_USE_REAL_ADAPTER=1`). Until then a deterministic stub answers, so tests
and shared environments are safe by default.

## How it works

Every job is a row in a store: a SQLite file by default
(`~/.hoglah/hoglah.db`, nothing to set up), or MongoDB when several workers or
machines share one queue. A worker claims a job, holds a heartbeat lease on it
while the model runs, and writes the result back. The store, not the worker, is
the source of truth, so a job whose worker dies is reclaimed by another worker
when the lease expires.

You get a finished result in whichever way suits the caller: `wait()` /
polling, an in-process callback, an HTTP callback, or a JSON file in an output
folder.

## Add what you need

Everything below is optional. Each link goes to the
[user guide](docs/user-guide.md) section that covers it.

| When you need… | Use | Guide |
|---|---|---|
| Delays, schedules, retries, de-duplication, A-before-B chains, batches | `delay_seconds` / `run_at`, `RetryPolicy`, `idempotency_key`, `depends_on`, `submit_batch` | [§6](docs/user-guide.md#6-scheduling-retries-dependencies-idempotency) |
| Fair sharing: one 70B job at a time, caps per agent or tag | `concurrency`, `model_slots`, session/tag slots and rates | [§7](docs/user-guide.md#7-concurrency-slots-and-fairness) |
| A backend other than Ollama | OpenAI-compatible HTTP; `claude` / `codex` / `gemini` / `grok` / `kiro` CLIs | [§7](docs/user-guide.md#execution-backends-ollama-openai-compat-clis) |
| Chat, embeddings, token streaming | `messages=`, `submit_embedding`, NDJSON + SSE | [§4](docs/user-guide.md#4-library-quick-start), [§9](docs/user-guide.md#9-result-delivery) |
| Several workers or machines on one queue | MongoDB store (`hoglah[mongo]`) | [§8](docs/user-guide.md#8-storage-backends), [§14](docs/user-guide.md#14-topologies-recipes) |
| Jobs arriving from Kafka, RabbitMQ or Redis Streams | `hoglah kafka-bridge` / `rabbitmq-bridge` / `redis-bridge` | [§11](docs/user-guide.md#11-messaging-bridges) |
| Watching the queue | `hoglah monitor`, `hoglah serve` (web), Prometheus, Galeed traces | [§12](docs/user-guide.md#12-observability) |
| Every setting and its `HOGLAH_*` variable | — | [§13](docs/user-guide.md#13-configuration-cheat-sheet) |
| Hugging Face GGUF models, WSL2 networking | `hoglah pull "hf.co/…:Q4_K_M"` | [§15](docs/user-guide.md#15-real-ollama-notes) |

## Know before you rely on it

A failure analysis of v0.13.0 ([docs/robustness](docs/robustness/DECISION-BRIEF.md))
reproduced these. Fixes are planned but not yet shipped.

- **A job can run more than once.** After a worker crash, a lost heartbeat or a
  clock jump, a job may be executed again, including CLI agent jobs that edit
  files. Make such jobs safe to repeat. Execution is at-least-once, including on
  MongoDB.
- **`hoglah clear` deletes before it asks.** Answering "n" does not undo it.
- **The Kafka bridge can skip a message** when the store errors, despite logging
  "will retry". RabbitMQ and Redis bridge recovery has not been tested against
  live brokers.
- **Don't put secrets in job `metadata`.** It is copied into results, output
  files and callback payloads.
- **If Galeed tracing is enabled and unreachable, every submit stalls about 2 s.**

## Documentation

| For | Read |
|---|---|
| Using Hoglah (operators, app developers) | [docs/user-guide.md](docs/user-guide.md) |
| AI agents and tool-using models | [docs/ai-user-guide.md](docs/ai-user-guide.md) |
| Coding agents working in this repo | [AGENTS.md](AGENTS.md) |
| Everything else: design, requirements, robustness, roadmap | [docs/README.md](docs/README.md) |
| Concept map (Open Knowledge Format) | [okf/index.md](okf/index.md) |

Also: [CHANGELOG](CHANGELOG.md) · [feature comparison and gaps](docs/feature-comparison-and-gaps.md).

## Bugs and support

Open an [issue](https://github.com/gellsmore-svg/hoglah/issues) using the bug
report template, and include `hoglah doctor` output (add `--real` if you use a
real backend; it never prints URLs or credentials), `hoglah --version`, your
Python version and OS, and a minimal reproduction. Report security issues
privately: see [SECURITY.md](SECURITY.md).

## License and contributing

Apache 2.0, see [LICENSE](LICENSE). Contributions: [CONTRIBUTING.md](CONTRIBUTING.md).
Named after one of the daughters of Zelophehad (Numbers 27).
