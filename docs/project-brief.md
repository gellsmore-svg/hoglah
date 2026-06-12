# Hoglah Project Brief

**Last updated**: 2026-06-12

## Purpose

Hoglah is a lightweight, local-first job queue manager and Ollama wrapper for resource-constrained environments. It lets applications (especially multi-agent systems) submit LLM inference work asynchronously, receive an immediate job ID, and later retrieve results or be notified via Python callbacks — without blocking on hardware that can only run one (or very few) model(s) at a time.

It sits as a reliable orchestration layer between calling code and a local Ollama instance. The focus is simplicity, restart-survivable persistence, and safe serial (or low-concurrency) execution.

## Core Goals (V1)

- Fire-and-forget job submission with immediate ID return.
- Persistent job state (survives restarts).
- Configurable concurrency (default 1).
- Clean Python library interface + thin CLI for inspection.
- Support for both raw `prompt` (generate) and `messages` (chat) submission styles.
- Basic context/model capability awareness.
- Completion callbacks (in-process Python callables).
- Simple but robust error handling, retries, and best-effort cancellation.

## Non-Goals for V1

See the full requirements document (`docs/requirements-v1.0.md`).

Key deliberate exclusions:
- Web UI / HTTP API server (V2)
- Webhooks
- Distributed / multi-node operation
- Non-Ollama backends
- Complex dependency graph execution (parent_job_id is recorded for traceability only in V1)

## Primary Users

- Developers building local multi-agent workflows.
- Background processors that need to queue LLM calls safely.
- Tools that must integrate LLM work without controlling the main event loop.

## First Useful Outcome

A working library + CLI where you can:

1. `pip install -e .` (or equivalent)
2. Submit a job and immediately receive a UUID.
3. Query status and retrieve a completed result after the (serial) worker finishes.
4. Provide an in-process callback that fires on completion.
5. Kill/restart the process and still see previously submitted jobs and their final state.

This corresponds to the core job management loop described in the initial requirements.

## Status

- 2026-06-12: Initial requirements + review decisions + Chunks 1-3 (client, store, worker, pluggable adapters + full real OllamaAdapter, rich CLI, examples).
- `Hoglah(use_real=True)`, expanded submit (messages + sampling params), `hoglah run`, polished list, 13 tests.
- Default is always-safe StubAdapter. Real path is one flag/env/kwarg away.
- Version 0.2.0. examples/basic_usage.py + very usable from both Python and CLI.
- All tests green. Ready for real Ollama when the server is free.
