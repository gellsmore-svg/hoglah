# Architecture Decisions

**Last updated**: 2026-06-12

ADRs are append-only. If a decision is reversed, add a new ADR rather than editing the old one, and note the supersession in both entries.

## Accepted Decisions

| ID | Decision | Rationale |
|---|---|---|
| ADR-001 | Use Python 3.11+ | Consistent with sister domains (Mahalath, Tirzah). Good typing, modern stdlib (asyncio, pathlib, uuid, etc.), and matches the existing local development environment. |
| ADR-002 | Pluggable persistence backends: SQLite (default lightweight) + MongoDB support as an option | Operator preference for a "tiny embedded option with sqlite and mongo as options". SQLite remains the zero-dependency default (single-file `hoglah.db`). MongoDB support allows reuse of existing instances from other domains when desired. Backend chosen at `Hoglah(backend=...)` construction time. |
| ADR-003 | asyncio-based worker loop with semaphore for concurrency control (default concurrency=1) | Matches explicit operator choice. Internal event loop (run in background thread if the public API stays synchronous) manages task scheduling. Semaphore enforces the concurrency limit. |
| ADR-004 | Two submission styles supported at the API: `prompt` (Ollama generate) and `messages` (Ollama chat) | Directly reflects common usage in agentic code and the initial requirements. The implementation will normalize both to the appropriate Ollama endpoint while presenting a unified Job model to callers. |
| ADR-005 | Prefer the official `ollama` Python package for the adapter (with httpx fallback option) | The `ollama` package provides a clean, maintained client for both generate and chat paths plus model listing. Keeps V1 thin; an adapter protocol can be introduced early so mock + direct HTTP paths are easy to add for tests. |
| ADR-006 | Callbacks: attempt best-effort re-delivery of recent completions on restart | Operator preference. On `Hoglah()` startup, the system will attempt to re-invoke callbacks for jobs that reached terminal state (completed/failed) while the process was down. Because raw Python callables cannot be serialized, re-delivery will rely on either (a) the same process re-instantiating `Hoglah` with equivalent callback objects still in scope, or (b) a lightweight registered-callback mechanism (callback name/id + args) for durable cases. Callback exceptions never affect job final status. |
| ADR-007 | Job identifiers are UUID4 strings (hex or standard form) | Simple, globally unique, easy to log and correlate across systems. Returned immediately from submit. |
| ADR-008 | `parent_job_id` is stored for traceability and future dependency features but does not implement waiting or graph execution in V1 | The field is useful for workflow provenance today. Full dependency orchestration (wait for parents, fan-out, etc.) is out of scope for the initial focused queue manager. |
| ADR-009 | Context handling: succeed even on truncation; return clear truncation metadata to caller | Operator guidance: main goal is successful processing of the supplied context and returning a message to the caller if truncation occurs. Implementation will estimate/apply context limits, let the model/Ollama handle truncation when necessary, and include explicit fields in the result (e.g. `truncated: bool`, `truncation_reason`, estimated token counts) rather than failing the job. |
| ADR-010 | Result object (`JobResult`) is a frozen dataclass / Pydantic model containing: id, status, output (text), model, parameters snapshot, usage, timings (queued/started/finished), error, tags, user metadata, parent_job_id | Matches operator confirmation that the current stub shape is good. Will be extended with truncation info per ADR-009. |
| ADR-011 | Error/retry/cancellation policy: simple & pragmatic (exponential backoff on transient errors only) | Operator choice. Retry only on transient issues (connection, 5xx, etc.). Do not retry context-too-large, bad requests, or explicit user cancels by default. `timeout_seconds` marks the job failed. Best-effort cancellation attempts to interrupt a running generation via the client when possible. |
| ADR-012 | Public API: `Hoglah(config=...)` client class + methods (submit/status/get/list/cancel/wait) and thin CLI | Operator choice. Keeps a synchronous-feeling submit (returns ID immediately) while the worker is asyncio-based internally. CLI covers at minimum: list, status, cancel, models. |

## Open Or Pending Decisions

| ID | Question | Status (Resolved 2026-06-12) |
|---|---|---|
| DQ-004 | How to handle a job that was "processing" when the process died? | On startup, jobs in "processing" are moved to "queued" (respecting `max_retries`) or marked failed with "interrupted". Exact result message wording can be refined during implementation. |
| DQ-005 | Storage location defaults and exact Pydantic settings model | Default `~/.hoglah/hoglah.db` (or cwd). Constructor + env + small config file. Pydantic settings per scaffold. |
| DQ-006 | `wait(job_id, timeout=None)` helper? | Yes. |
| DQ-007 | Logging / observability approach | Standard `logging` + optional structured lifecycle events. |
| DQ-008 | Callback re-delivery mechanism (ADR-006) | **Resolved**: Support a named/durable callback registry. Callers may supply `callback=callable` or `callback_key="my_handler"`. Hoglah persists the key. On (re)start the caller provides a registry mapping of keys to callables; matching recently completed jobs get their callbacks re-invoked. |
| DQ-009 | Sync public facade vs async API (asyncio worker) | **Resolved**: Synchronous public facade. `Hoglah(config=...)` presents normal sync methods (`submit` returns immediately). An internal background thread runs the asyncio event loop + worker tasks. |
| DQ-010 | When to introduce pluggable persistence abstraction | **Resolved**: Define a small `JobStore` protocol / abstract base immediately. Implement `SQLiteJobStore` first; the interface must allow a future `MongoJobStore` (or injected store instance) with minimal change to the rest of the code. Backend selection at `Hoglah(backend=...)` or via store= instance. |

**All major questions from the 2026-06-12 requirements review rounds have been resolved with operator input.** See the "Accepted Decisions" table above for the resulting ADRs. Implementation can now proceed with high confidence. Minor details (exact wording of interruption errors, precise Pydantic field names, logging event shapes) will be worked out during coding and recorded here or in code.
