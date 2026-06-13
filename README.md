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

### From PyPI (recommended once published)
```bash
pip install hoglah
# With CLI
pip install "hoglah[cli]"
```

### From GitHub Releases (immediate, no PyPI needed)
After a release is published (via `git tag vX.Y.Z && git push --tags`):

```bash
# Latest wheel
pip install "https://github.com/gellsmore-svg/hoglah/releases/latest/download/hoglah-*.whl"

# Or specific version
pip install "https://github.com/gellsmore-svg/hoglah/releases/download/v0.2.1/hoglah-0.2.1-py3-none-any.whl"
```

### From source (for development)
```bash
git clone https://github.com/gellsmore-svg/hoglah
cd hoglah
python -m venv .venv
.venv/bin/pip install -e ".[dev,cli]"
```

### Publishing to PyPI (so `pip install hoglah` just works)

The release workflow already builds the packages. To publish them to PyPI automatically on every `v*` tag:

1. Go to https://pypi.org/manage/project/hoglah/ (create the project first if it doesn't exist by doing a manual upload once).
2. Go to "Publishing" → "Add a trusted publisher".
3. Choose **GitHub**.
4. Fill in:
   - **Repository**: `gellsmore-svg/hoglah`
   - **Workflow**: `release.yml` (or leave blank to allow any workflow)
   - **Environment**: (optional but recommended — create a GitHub Environment called `pypi` and select it here)
5. Save.

Then push a tag:
```bash
git tag v0.2.1
git push origin v0.2.1
```

The release workflow will now publish to PyPI using OIDC (no API token required — this is the modern secure way).

You can also do a one-time manual upload with `twine` if you prefer.

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
hoglah status <job-id> --json

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
(This is what we used to validate that `pip install hoglah-0.2.1-py3-none-any.whl[cli]` produces a fully working installation.)

**Real Ollama:** Opt-in via `use_real=True` / `HOGLAH_USE_REAL_ADAPTER=1` / `--real`. Auto-pulls models, uses model info for context, reports real truncation/usage. A gated integration test exists (`RUN_OLLAMA_TESTS=1 pytest ...`). The vast majority of real paths are also covered by unit mocks.
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
- 13 passing tests. No real Ollama required (everything exercises safely via stub).

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
