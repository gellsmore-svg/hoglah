# Architecture Decisions

**Last updated**: 2026-06-12

ADRs are append-only. If a decision is reversed, add a new ADR rather than editing the old one, and note the supersession in both entries.

## Accepted Decisions

| ID | Decision | Rationale |
|---|---|---|
| ADR-001 | Use Python 3.11+ | Consistent with sister domains (Mahalath, Tirzah). Good typing, modern stdlib (asyncio, pathlib, uuid, etc.), and matches the existing local development environment. |
| ADR-002 | Use SQLite for job persistence (single-file `hoglah.db` or configurable path) | Lightweight, zero external dependencies (no MongoDB requirement like the heavier sisters), ACID, easy to inspect with `sqlite3` CLI or tools. Perfect fit for a "lightweight local queue". Jobs, results, and metadata fit naturally in relational tables. |
| ADR-003 | Single primary worker loop with configurable semaphore for concurrency | Default concurrency=1 matches the stated target environments. Semaphore + thread (or asyncio task) pool keeps the implementation simple and restart-friendly. Future higher concurrency is a small knob change. |
| ADR-004 | Two submission styles supported at the API: `prompt` (Ollama generate) and `messages` (Ollama chat) | Directly reflects common usage in agentic code and the initial requirements. The implementation will normalize both to the appropriate Ollama endpoint while presenting a unified Job model to callers. |
| ADR-005 | Prefer the official `ollama` Python package for the adapter (with httpx fallback option) | The `ollama` package provides a clean, maintained client for both generate and chat paths plus model listing. Keeps V1 thin; an adapter protocol can be introduced early so mock + direct HTTP paths are easy to add for tests. |
| ADR-006 | In-process Python callbacks only in V1; no durable callback scheduling | Matches "file/callback sufficient for V1" in non-goals. Callbacks run after the worker marks a job completed (inside the same process). On restart, recently completed jobs can optionally re-invoke any registered live callbacks if still in memory, but the contract is best-effort for the lifetime of the process. Webhooks (callback_url) are explicitly V2. |
| ADR-007 | Job identifiers are UUID4 strings (hex or standard form) | Simple, globally unique, easy to log and correlate across systems. Returned immediately from submit. |
| ADR-008 | `parent_job_id` is stored for traceability and future dependency features but does not implement waiting or graph execution in V1 | The field is useful for workflow provenance today. Full dependency orchestration (wait for parents, fan-out, etc.) is out of scope for the initial focused queue manager. |
| ADR-009 | Model context awareness via a small built-in catalog + runtime discovery | Provide reasonable defaults for popular Ollama models (Gemma, Qwen, Mistral, etc.) and fall back to `ollama show` or a safe conservative num_ctx when unknown. Users can always override `num_ctx`. Truncation policy (if any) will be explicit and logged. |
| ADR-010 | Result object (`JobResult`) is a frozen dataclass / Pydantic model containing: id, status, output (text), model, parameters snapshot, usage stats, timings, error (if any), tags, user metadata | Gives callers everything they need for logging, auditing, and downstream decisions without requiring them to re-query Ollama. |

## Open Or Pending Decisions

| ID | Question | Current Lean |
|---|---|---|
| DQ-001 | Exact shape and name of the public client (`Hoglah()`, `JobQueue()`, `ollama_queue`, module-level functions, or context manager)? | `Hoglah()` client instance (or `from hoglah import Hoglah; h = Hoglah()`). Supports config at construction time and feels natural for library use. |
| DQ-002 | CLI command name and surface? | `hoglah` (e.g. `hoglah submit`, `hoglah status <id>`, `hoglah list`, `hoglah cancel`, `hoglah models`). Thin wrapper over the library. |
| DQ-003 | Retry / backoff strategy details and which errors are retryable | Exponential backoff with jitter. Retry on transient Ollama errors (connection, 5xx). Do not retry on context-too-large or bad prompt errors by default (configurable). |
| DQ-004 | How to handle a job that was "processing" when the process died? | On startup, any jobs in "processing" state are moved to "queued" (or "failed" with a "interrupted" reason) and will be retried according to max_retries. Simple and safe for V1. More sophisticated "claim with heartbeat" can come later if needed. |
| DQ-005 | Storage location defaults and config mechanism | Default to `~/.hoglah/hoglah.db` (or cwd for dev). Support constructor arg, env var `HOGLAH_DB`, and a small YAML/JSON config file like the sisters. Use Pydantic for settings. |
| DQ-006 | Should there be an explicit `wait(job_id, timeout=None)` helper? | Yes — very useful for tests and simple scripts. It polls status with backoff or uses an internal Event per job. |
| DQ-007 | Logging / observability approach | Standard `logging` with a `hoglah` logger. Optional structured JSON logs for job lifecycle events. Keep it lightweight. |
