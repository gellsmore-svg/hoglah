# Changelog

All notable changes to Hoglah will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (none yet)

## [0.4.0] - 2026-06-15

### Added
- **MongoDB backend (ADR-017 — fulfils the ADR-002 option / DQ-010 promise).**
  A drop-in `MongoJobStore` selected with `Hoglah(config={"backend": "mongo", ...})`
  or `HOGLAH_BACKEND=mongo` (configurable `mongo_uri` / `mongo_db` / `mongo_collection`;
  defaults `mongodb://localhost:27017` / `hoglah` / `jobs`). `pymongo` is an
  **optional** dependency — `pip install "hoglah[mongo]"` — imported lazily, so
  SQLite users never need it and the default stays SQLite. Why a server backend:
  - **No single-file lock.** A Mongo server sidesteps SQLite's file locking, so
    the WAL / `busy_timeout` dance does not apply. `claim_for_processing` is
    atomic server-side via `find_one_and_update` (QUEUED → PROCESSING), so
    concurrent workers — even on different machines — still execute each job once.
  - **External queue visibility.** Jobs are stored as native documents (request /
    result / tags are sub-documents, not opaque JSON blobs), so the queue is
    directly inspectable from `mongosh`, Compass, or any other service.
  - **Multi-machine workers** can share one queue over the network.
  Returned rows mirror `SQLiteJobStore` exactly (parsed `request`/`result`/`tags`
  plus the raw `*_json` strings the client also reads), so it is a true drop-in.
  Validated by a gated contract test **and** a full client end-to-end test against
  a real local MongoDB (`RUN_MONGO_TESTS=1`, throwaway collection).

## [0.3.3] - 2026-06-15

### Fixed
- **Cancel can no longer be clobbered by the generic-exception path.** The
  `except Exception` branch in `_process_job` now re-checks status and won't
  overwrite a CANCELLED job with FAILED (matches the success-path race guard).
- **Worker no longer double-spawns a task for an in-flight job.** The poll loop
  skips a `job_id` already in `self._inflight` (a task created on a prior poll
  may not have claimed the job yet, so it can still read as QUEUED) — previously
  a second task could overwrite the in-flight entry and make `cancel()` target
  the wrong task.

### Docs
- Corrected the shutdown-drain comment: cancelled stragglers are intentionally
  left PROCESSING and re-queued for retry by startup recovery (DQ-004); no
  terminal result is written there (the prior comment wrongly implied one was).

(Findings from a read-only code review of the 0.3.2 hardening.)

## [0.3.2] - 2026-06-15

### Changed
- **`cancel()` now interrupts a running job, not just a queued one.** In-flight
  job tasks are tracked per job_id; `cancel()` (main thread) interrupts the
  executing task on the worker's loop via `call_soon_threadsafe(task.cancel)`.
  The CANCELLED result is recorded first and `_process_job` has a race guard
  that refuses to overwrite a cancellation, and treats `CancelledError`
  distinctly so an interrupted job never becomes FAILED. Test added (no Ollama).
- **SQLite store uses WAL + `busy_timeout=5000`.** A reader (submit/get) no
  longer blocks the worker's writes, and contending writers wait briefly
  instead of failing immediately with "database is locked" — matters once a
  submitter and worker (or two worker processes) share the file.

## [0.3.1] - 2026-06-14

### Changed
- **OllamaAdapter memoizes model presence + info per process.** The hot path
  previously did two `client.show()` round-trips per job (one in `pull_model`,
  one in `show_model`); a long-running worker now does one on a model's first
  job (the presence-check seeds the show cache) and zero thereafter. Per-model,
  process-lifetime; `pull_model(..., force=True)` / `show_model(..., force=True)`
  bypass, and a fresh CLI process always starts cold. Test added (no Ollama).

## [0.3.0] - 2026-06-13

### Added
- **Embedding jobs (ADR-013).** A first-class job `kind` (`generate` | `embed`).
  `kind="embed"` carries the input text in `prompt`, the worker routes it to
  `adapter.embed()` (Ollama `/api/embed`), and the result carries
  `embedding: list[float]` + `embedding_dim` (with `output=None`). Non-finite
  vectors (NaN/Inf — a known instability in some models like bge-m3) raise
  rather than returning a bogus vector. `StubAdapter.embed` returns a
  deterministic finite vector so the default/test path needs no server.
  Convenience: `Hoglah.submit_embedding(text, model=...)`.
- **Output-folder result delivery (ADR-014).** Optional `output_dir` config
  (env `HOGLAH_OUTPUT_DIR`); the worker writes each terminal job's full result
  to `<output_dir>/<job_id>.json` atomically (temp + `os.replace`) so a poller
  never reads a partial file. Lets a decoupled submitter collect results
  without sharing the worker's in-process callbacks. `None` (default) =
  disabled.
- **Outbound HTTP callback delivery (ADR-015).** Optional per-job
  `callback_url` on `submit()` / `submit_embedding()`; on terminal status the
  worker POSTs the result JSON to it (daemon thread; retries with backoff;
  4xx stops early; failure leaves the output file as fallback). Generic — any
  caller supplies its own URL. Stdlib `urllib` only, no new dependency.
  **Supersedes the prior "no webhooks / callback_url is V2" non-goal.**
  Config: `callback_max_retries` (3), `callback_timeout_seconds` (10).

### Changed
- **Interrupted-job recovery is now a worker responsibility (ADR-016).** Only
  `start_worker=True` instances run `_recover_interrupted_jobs()`. This makes
  the shared-queue topology safe: a pure submitter (`start_worker=False`)
  feeding a separate worker daemon no longer re-queues the daemon's in-flight
  jobs on construction.

## [0.2.2] - 2026-06-13

### Fixed
- **Sync facades now work inside a running event loop.** `Hoglah.show_model()`
  and `pull_model()` used a `try asyncio.run() except RuntimeError ->
  run_until_complete()` fallback that could not recover when a loop was
  already running (notebooks, async handlers, the gated real test). Replaced
  with a loop-safe `_run_async` helper (runs in a one-shot worker thread when
  a loop is live). Regression test added (no Ollama needed).
- **OllamaAdapter no longer breaks across event loops.** The cached
  `ollama.AsyncClient` was bound to the first loop that used it, raising
  "bound to a different event loop" when reused from another (every sync
  facade call spins a fresh loop). Now cached per-loop and recreated when the
  running loop changes.
- **`timeout_seconds` is now enforced (ADR-011).** It was stored but ignored;
  a stuck generation could hold a worker slot forever. Each attempt is now
  bounded by `asyncio.wait_for`; on expiry the job is marked FAILED (terminal,
  not retried) with `metadata.timed_out = True`, and the in-flight call is
  cancelled, freeing the slot.

### Changed
- **Graceful worker shutdown.** In-flight jobs are now tracked and drained
  within a bounded window on `close()` (under the existing 3s thread join)
  instead of having their event loop destroyed mid-request; stragglers are
  cancelled so a terminal result is still recorded.

### Validated
- **First real end-to-end inference confirmed** against a live Ollama
  (submit → worker → OllamaAdapter → result; gated `RUN_OLLAMA_TESTS=1` test
  now PASSES). Prior "untested with real Ollama" caveat is resolved. The
  earlier unreachability was WSL→Windows networking (reach the Windows daemon
  at the WSL2 gateway IP with `OLLAMA_HOST=0.0.0.0`), not a hard limitation.

## [0.2.1] - 2026-06-13

### Added
- `hoglah show <model>` CLI and Hoglah.show_model() / adapter.show_model() for inspecting model details (context size, template, family, etc.).
- `hoglah clear` (and Hoglah.clear) for pruning old/terminal jobs by status or age.
- `hoglah info` (and Hoglah.info) for config/adapter/stats snapshot (now includes version).
- Configurable log_level (HOGLAH_LOG_LEVEL / config.log_level, default INFO).
- `hoglah rm <job-id>` for deleting specific jobs (with --yes).
- --parent filter to list/ps, enriched --json with 'preview', dynamic PARENT column in human table.
- `hoglah wait <job-id>` (standalone, supports --json) to block until terminal and print result.
- --json support for rm and wait.
- Smart context handling in OllamaAdapter (uses show_model to auto-detect num_ctx from model parameters if not specified, sets effective_num_ctx).
- GitHub release workflow (.github/workflows/release.yml).
- Comprehensive mocked unit tests for OllamaAdapter paths (show, pull, run with context/truncation).
- 24 passing tests + 1 gated real-Ollama test.

### Changed
- Improved list/ps human/JSON output for better DX and parent_job chaining visibility.
- Real adapter now auto-pulls missing models and has smarter truncation/context support.
- Version bumped to 0.2.1.

## [0.2.0] - 2026-06-12

### Added
- Pluggable execution adapters: `StubAdapter` (default, safe, no network) and `OllamaAdapter` (real Ollama via official client).
- `Hoglah(use_real=True)` constructor kwarg and `HOGLAH_USE_REAL_ADAPTER` environment variable for easy real mode.
- Rich `hoglah submit` command supporting both prompt and `--messages` (JSON chat), plus full generation parameters (`--temperature`, `--num-ctx`, `--seed`, etc.).
- `hoglah run` for running the background worker in the foreground.
- `hoglah models` for model discovery (stub or real).
- `hoglah ps` as a convenient alias for `list`.
- `--json` output for `list`, `status`, `models`, and `ps` (machine-readable scripting support).
- Root `hoglah --version` / `-V` flag (in addition to `hoglah version` subcommand).
- Context manager support: `with Hoglah(...) as h:` for automatic worker + store cleanup on exit.
- `examples/basic_usage.py` demonstrating submit (prompt + chat), callbacks (direct + named registry), wait, list, and context manager usage.
- 14 passing tests (including CLI via Typer CliRunner, adapter param mapping, context manager, restart behavior).
- `BaseAdapter`, `OllamaAdapter`, `StubAdapter` now exported in the public API.

### Changed
- Default adapter is always the safe `StubAdapter`; real execution is opt-in.
- Improved `list` table formatting with headers.
- CLI factory and commands cleaned up to consistently support `--real` / `--ollama-host`.
- Ruff lint clean (imports, unused code cleaned).
- pyproject.toml metadata improved (license classifier, Typing::Typed).
- Full job lifecycle (submit → queue → worker → result + callbacks) works reliably with restart recovery and truncation reporting.

### Fixed
- Various small issues around import ordering, unused variables, and CLI delegation.

## [0.1.0] - 2026-06-12 (initial)

- Core `Hoglah` client, `JobStore` (SQLite), background asyncio worker (concurrency=1).
- Submit/get/list/status/cancel/wait with full parameter surface and named/direct callbacks.
- Persistence that survives restarts, interrupted job recovery, best-effort retries.
- Basic CLI (version, list, status, cancel).
- Tests for persistence, callbacks, worker execution via stub.
- Initial docs, requirements capture, architecture decisions.

[0.4.0]: https://github.com/gellsmore-svg/hoglah/compare/v0.3.3...v0.4.0
[0.2.0]: https://github.com/gellsmore-svg/hoglah/compare/v0.1.0...v0.2.0
