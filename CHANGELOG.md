# Changelog

All notable changes to Hoglah will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[0.2.0]: https://github.com/gellsmore-svg/hoglah/compare/v0.1.0...v0.2.0
