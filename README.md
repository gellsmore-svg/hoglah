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

## Quick Start (Planned)

Once implemented:

```bash
git clone https://github.com/gellsmore-svg/hoglah
cd hoglah
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
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
```

CLI (sketch):

```bash
hoglah list --status queued,processing
hoglah status <job-id>
hoglah cancel <job-id>
hoglah models
```

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

**2026-06-12**: Initial requirements specification captured and project metadata scaffolded.

- `docs/requirements-v1.0.md` — verbatim initial spec
- `docs/project-brief.md`
- `docs/architecture-decisions.md` — early ADRs + open questions
- `pyproject.toml` skeleton
- Basic directory layout and registry updates

No implementation code yet. Next work will focus on the core job lifecycle (submit → persistent queue → single-worker execution against Ollama → result + callback).

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
