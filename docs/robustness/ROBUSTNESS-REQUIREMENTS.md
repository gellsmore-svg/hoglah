# Robustness requirements

Deliverables **F** (requirements), **G** (Galeed observability), **H**
(recovery decision matrix) and **J** (implementation sequence) of the
2026-10-01 robustness analysis of Hoglah v0.13.0 (`f0d322a`).
Index: [README.md](README.md). Evidence: [FAILURE-CATALOG.md](FAILURE-CATALOG.md),
[PROVIDER-FAILURES.md](PROVIDER-FAILURES.md), [experiments/](experiments/README.md).

Normative words follow RFC 2119: **MUST**, **MUST NOT**, **SHOULD**,
**SHOULD NOT**, **MAY**.

**Priorities.**

| Class | Meaning |
|---|---|
| **P0** | data loss, state corruption, or unrecoverable silent failure |
| **P1** | serious operational failure |
| **P2** | resilience improvement |
| **P3** | defence in depth |

The priority class is an implementation ordering. It does not replace the risk vectors in the catalogue.

The set converged over eight iterations of a recursive attack loop: STABILIZED, with two consecutive non-material passes. The record is in [convergence/](convergence/).

---

## 0. What Hoglah guarantees once these requirements hold

These are the honest delivery semantics. None of them is "exactly once".

| Boundary | Guarantee |
|---|---|
| Job state transition | Every status change is a single conditional write; a terminal row never reverts implicitly (INV-1, INV-2, INV-7). |
| Provider dispatch, rerunnable jobs (stub, Ollama, OpenAI-compatible inference) | **At least once.** A crash after dispatch may cause one more dispatch. The cost is compute, not correctness. |
| Provider dispatch, non-rerunnable jobs (CLI agent backends by default) | **At most once automatically.** An interrupted attempt is recorded `outcome=unknown` and the job is `FAILED outcome_unknown` until an operator requeues it. |
| Result delivery (output file, HTTP callback, named callback, broker) | **At least once attempted** per `(job, attempt, sink, route)`, bounded by attempts or `sink_ttl`; otherwise recorded `failed` with a reason. Consumers dedupe on `delivery_id`. |
| Broker ingress | An accepted message is acknowledged only after its job row (or a dead-letter copy) is durable. Dedupe on `correlation_id` holds for `dedupe_window`. |
| Galeed trace | Best effort and never blocking. Loss is counted, never silent, and never changes job state. The store is the system of record for retained rows. |

---

## 1. Canonical failure taxonomy

Three independent fields answer three different questions. They MUST NOT be
collapsed into one string.

| Question | Field | Where |
|---|---|---|
| What caused it? | `failure.category` | attempt record |
| What does Hoglah do? | `decision.action` | attempt record |
| What happened to the job? | `status` (5 values, unchanged) plus `attempt.outcome` | job row / attempt record |

### 1.1 Cause (`failure.category`)

The research changed the brief's starting list in three ways:
- **Split.** `quota` is separated from `rate_limit` (FM-GEMINI-002, Claude `billing_error`), and `provider_overloaded` is separated from `provider_transient` (Ollama 503, Claude `overloaded`).
- **Merged.** `malformed_response` is folded into `protocol_error`, with a subtype.
- **Added.** Hoglah-internal categories that the evidence shows are needed: `lease_lost`, `outcome_unknown`, `store_unavailable`, `corrupt_row`, `attempts_exhausted`. `delivery_failure` and `logging_failure` are kept as **sink-level only**: they never fail a job (ROB-CORE-001).

| Category | Meaning | Typical signals | Default retryable |
|---|---|---|---|
| `caller_invalid` | Request can never succeed as written | Ollama 400 / validation 500 (E19), E2BIG (E21), invalid options, prompt parsed as flag | no |
| `configuration_error` | Hoglah or the provider is misconfigured | binary missing, gemini 52/44, kiro 3, bad env (E22), codex `CODEX_HOME` | no |
| `authentication` | No or invalid credentials | claude "Not logged in", grok "Not signed in", codex 401, gemini 41, HTTP 401 | no |
| `authorisation` | Authenticated but not allowed | model not permitted, org policy, tool denied in non-interactive mode (kiro #11594) | no |
| `quota` | Allowance exhausted until a reset | billing_error, daily or weekly limit, gemini quota | at the known reset only |
| `rate_limit` | Short-term throttling | HTTP 429, Claude `rate_limit` | yes, honour Retry-After |
| `model_unavailable` | Model not installed or unknown | Ollama 404, `model_not_found`, grok "unknown model id" | no (auto-pull is opt-in, outside the attempt) |
| `context_limit` | Input exceeded or was truncated | Ollama `prompt_eval_count == num_ctx` (E19), "context length" errors | no |
| `resource_exhaustion` | Memory, VRAM, disk, fd or process limits | CUDA OOM, ENOMEM, ENOSPC, EMFILE | only if `retry_on` includes `oom` |
| `provider_overloaded` | Provider shedding load | HTTP 503 or 529, Ollama queue full | yes, long backoff |
| `provider_transient` | Provider-side transient fault | 5xx with no validation text, runner restart | yes |
| `provider_permanent` | Provider-side permanent fault | documented permanent server errors | no |
| `network_transient` | Transport could not reach the provider | ECONNREFUSED, reset, DNS, TLS handshake timeout | yes, elapsed budget sized to restart time |
| `timeout` | Attempt exceeded its execution or idle limit | Hoglah timer; CLI hang (E09) | per policy (default no, ADR-011) |
| `process_failure` | Subprocess died abnormally | negative exit / signal, non-zero exit not otherwise mapped | once |
| `protocol_error` | Output violates the backend contract | exit 0 with empty or noise-only output (E07, FM-KIRO-002), unparseable JSON, CLI schema drift; subtype `malformed`/`empty`/`noise`/`schema` | once |
| `partial_response` | Stream ended without its terminal marker | Ollama missing `done`, SSE missing `[DONE]` | rerunnable class only |
| `cancelled` | User cancel | — | no |
| `lease_lost` | Worker lost ownership mid-attempt | heartbeat failure, reclaim | handled by ROB-AMBIG-001 |
| `outcome_unknown` | Attempt dispatched; outcome unknowable | worker death after dispatch (E12, E14) | rerunnable class only |
| `store_unavailable` | Persistence failed | SQLite locked beyond budget, Mongo selection timeout | infrastructure retry; does not consume the job's attempt budget |
| `corrupt_row` | Stored job cannot be decoded or validated | E26, schema-invalid request | no (quarantine) |
| `dependency_blocked` | A dependency or earlier label is dead | `depends_on` or label FAILED/CANCELLED/missing | no (cascade on requeue) |
| `attempts_exhausted` | Retry budget for the epoch is spent | attempt counter | no |
| `unknown` | Nothing above matched | any unmapped exception | see §1.4 |

### 1.2 Action (`decision.action`)

`retry_backoff` · `retry_at` (known reset time or Retry-After) · `fail` ·
`requeue_manual` (operator decides; used for `outcome_unknown`) · `quarantine`
(`corrupt_row`) · `none`. There is **no immediate retry**: every retry passes
through a persisted `run_at` (ROB-RETRY-001).

### 1.3 State

`status` keeps its five public values. Ambiguity lives in `attempt.outcome` ∈
{succeeded, failed, unknown, lease_lost, cancelled, in_progress}. A failed job
that needs a human is `status=FAILED`, `failure.category=outcome_unknown`,
`decision.action=requeue_manual`, which is visible in `hoglah dlq`.

### 1.4 Unknown errors (§30 of the brief)

An `unknown` failure MUST:
1. capture the exception class, cause chain, sanitised excerpt (both streams for CLIs, tail preserved) and provider/CLI version
2. be retried **at most once**, and only if the job is rerunnable *and* the outcome was definite (the provider returned an error rather than vanishing)
3. otherwise end `FAILED unknown` with `decision=requeue_manual`
4. emit `attempt.failed` with `category=unknown` and increment `hoglah_unknown_errors_total{backend}`

It MUST NOT loop. It MUST NOT be reclassified as transient by matching words in the message.

---

## F. Robustness requirements

Each requirement lists its **assurance vector** (0–5): COR correctness · REC
recoverability · IDM idempotency safety · OBS observability · DET determinism ·
TST testability · POR portability · SIM simplicity · CMP compatibility with the
current architecture.

The loop's working numbers are mapped as follows. Missing numbers were merged into another requirement.

| Loop # | Requirement |
|---|---|
| R1 | ROB-STATE-001 |
| R3 | ROB-AMBIG-001 |
| R4 | ROB-LEASE-001 |
| R5 | ROB-LEASE-002 |
| R6 | ROB-LOOP-001 |
| R7 | ROB-CORE-001 |
| R8 | ROB-CORE-002 |
| R9 | ROB-SCHED-001 |
| R10 | ROB-DELIV-001 |
| R12 | ROB-BRIDGE-001 |
| R13 | ROB-CLI-001 |
| R14 | ROB-BACKEND-001 |
| R15 | ROB-CLASS-001 |
| R16 | ROB-RETRY-001 |
| R18 | ROB-LOG-001 |
| R20 | ROB-SHUT-001 |
| R22 | ROB-SEC-001 |
| R23 | ROB-CONF-001 |
| R24 | ROB-TEST-001 |
| R25 | ROB-STATE-003 |
| R26 | ROB-BATCH-001 |
| R27 | ROB-ADMIN-001 |

### ROB-STATE-001 · Persistent attempt log · P0

- **Problem.** No attempt counter is persisted. Retries live in a Python loop, and reclaim reruns a job as if it were new. As a result `max_retries=0` is ignored after a crash (E12), a poison job runs forever (E13), and requeue erases the history (C6).
- **Requirement.**
  - Every job MUST have an append-only attempt log. Each entry holds:
    - `attempt_no`
    - `phase` ∈ claimed|dispatched|returned|committed
    - `host_id`, `boot_id`
    - `started_at`, `ended_at`
    - `outcome`
    - `failure{category, retryable, provider_code, exit_code, signal, http_status, exception_class, excerpt}`
    - `cli_version`
    - `decision{action, delay}`
  - `attempt_no` MUST be incremented in the same atomic statement as the claim.
  - The effective retry policy MUST be resolved and persisted at enqueue. This already happens for explicit policies (`effective_retry_policy`) and MUST also cover defaults.
  - Attempts are counted per **epoch**. An operator requeue starts a new epoch and MUST NOT delete earlier entries.
  - When the attempts in an epoch exceed `max_retries+1`, including attempts interrupted by worker death, the job MUST become `FAILED attempts_exhausted`.
  - SQLite MAY store the log as a JSON array column or a side table. Mongo stores it as an array on the job document, which keeps writes single-document.
- **Rationale.** Recovery after total process death needs to know how many times the provider was reached. Nothing else in the row can say.
- **Components.** `store.py`, `mongo_store.py`, `client.py` (`_process_job`, `_execute_with_retries`), `models.py`.
- **Failure modes.** FM-CORE-007, FM-CORE-008, FM-CORE-019, FM-CORE-020, FM-CORE-022.
- **Acceptance.**
  - (a) `e10_kill.sh` with `max_retries=0`: a CLI job interrupted after dispatch is never auto-run again.
  - (b) `e10b_poison.sh`: the job reaches `FAILED attempts_exhausted` after exactly `max_retries+1` worker deaths.
  - (c) `requeue()` keeps all earlier entries and starts epoch 2.
  - (d) Restarting with different `RetryPolicy()` code defaults does not change the bound of an enqueued job.
- **Test strategy.** Store-level unit tests for the atomic increment, plus kill/restart integration tests.
- **Dependencies.** Ships together with ROB-RETRY-001's fenced-requeue clause (N58).
- **Risk if not implemented.** Unbounded duplicate execution of side-effecting agents, and crash loops.
- **Assurance.** COR 5 · REC 5 · IDM 4 · OBS 4 · DET 5 · TST 5 · POR 5 · SIM 3 · CMP 4

### ROB-AMBIG-001 · Represent unknowable outcomes · P0

- **Problem.** If a worker dies after handing a job to a provider, Hoglah cannot know whether the work happened. Today it silently reruns, so agent edits are duplicated (E12, E14). The orphaned CLI may even still be running (FM-CLI-005).
- **Requirement.**
  - `phase=dispatched` MUST be committed before any provider side effect. If that write fails, the worker MUST NOT dispatch.
  - When an attempt that reached `dispatched` or `returned` is reclaimed or cancelled, its outcome MUST be recorded as `unknown`.
  - Each adapter declares a **rerunnable class**. The default is rerunnable for stub, Ollama and OpenAI-compatible inference, and non-rerunnable for CLI agent backends. A job MAY override it.
  - A rerunnable job with an unknown attempt MAY be re-dispatched automatically, within ROB-STATE-001's bound.
  - A non-rerunnable job MUST become `FAILED outcome_unknown` with `decision=requeue_manual`.
  - Single `requeue()` of such a row MUST be refused while the earlier attempt may still be running: it is `in_progress`, or it is unknown and not yet reaped (ROB-CLI-001). An explicit `force=True` overrides this and is recorded.
  - Bulk `requeue_failed` / `hoglah requeue --all-failed` MUST skip `outcome_unknown`, unreapable, `corrupt_row`, attempts-exhausted and unsealed rows, and MUST report the ids it skipped.
  - A single requeue MUST report dependents that were cascade-failed. `--with-dependents` SHOULD requeue them.
- **Rationale.** This is the brief's central requirement: represent ambiguity honestly instead of inventing certainty.
- **Components.** `client.py`, `store.py`, `mongo_store.py`, `cli.py` (`requeue`, `dlq`), all adapters (class attribute).
- **Failure modes.** FM-CORE-009, FM-CLI-005, FM-CLAUDE-001, FM-ADMIN-004, FM-CORE-018.
- **Acceptance.**
  - Kill a worker during a fake-CLI job: the job ends `FAILED outcome_unknown` and is not rerun.
  - The same test with the stub adapter: it is rerun once, with `attempt[0].outcome=unknown`.
  - Requeue while the orphan is alive is refused without `force`.
  - Bulk requeue skips the row and prints its id.
- **Test strategy.** Process-kill integration tests per adapter class, plus property tests over requeue.
- **Dependencies.** ROB-STATE-001 and ROB-CLI-001 (reaping).
- **Risk if not implemented.** Duplicated agent side effects after every crash, Windows update or `wsl --shutdown`.
- **Assurance.** COR 5 · REC 4 · IDM 5 · OBS 5 · DET 4 · TST 4 · POR 4 · SIM 3 · CMP 4

### ROB-LEASE-001 · Heartbeat survives, and fails safe · P0

- **Problem.** One store exception kills the heartbeat task silently. The lease lapses and the job runs twice (E03). A completion that arrives after the lease is lost is dropped at INFO (C1). The cross-process cancel watch dies the same way (C22).
- **Requirement.**
  - The per-job heartbeat MUST retry transient store errors within the remaining lease. Each store call MUST be bounded below `lease − margin`.
  - If renewal has not succeeded by the monotonic deadline (expiry − margin), the worker MUST stop the provider work (CLI: process group SIGTERM, then SIGKILL) and record `lease_lost`, or `unknown` if the attempt was dispatched.
  - A False renewal MUST be classified with one fenced read: cancelled, row gone, or token moved.
  - The heartbeat replaces the 0.25 s cancel-watch task. Cross-process cancel latency is therefore ≤ `heartbeat_interval_seconds`, which is declared and configurable.
  - A completion whose fenced commit fails MUST be logged at WARNING. Its output MUST be kept on the attempt as `orphan_output` when the store is reachable.
  - Declared: a store outage longer than one lease can lose a computed result.
- **Components.** `client.py` (`_heartbeat_loop`, `_cancel_watch_loop` removed), `cli_adapters.py`.
- **Failure modes.** FM-CORE-003, FM-CORE-006, FM-CORE-017, FM-CORE-023.
- **Acceptance.**
  - `e06_hb.py` gives exactly one execution, and one `lease.renew_failed` WARNING.
  - Cancel from another process interrupts within the heartbeat interval.
  - A forced lease loss leaves `orphan_output` on the attempt.
- **Test strategy.** Store fault injection (raise N times, then recover), plus a cancel-latency test.
- **Dependencies.** ROB-STATE-001 (to record the outcome).
- **Risk if not implemented.** Silent duplicates on any lock contention or database blip.
- **Assurance.** COR 5 · REC 4 · IDM 5 · OBS 4 · DET 4 · TST 5 · POR 5 · SIM 4 · CMP 5

### ROB-LEASE-002 · Clock-safe reclaim · P0

- **Problem.** Reclaim compares wall-clock timestamps written by different hosts or clocks. Skew or a forward jump, such as WSL2 after sleep, reclaims live jobs (E17).
- **Requirement.**
  - A worker MUST reclaim a PROCESSING row only after observing the same `(lease_token, lease_expires_at)` unchanged across at least `lease_seconds + margin` of **its own monotonic time**.
  - Wall-clock expiry is only a prefilter. On Mongo the prefilter MUST use server time (`$$NOW`).
  - No reclaim may happen within the first observation window after process start. This raises recovery time by about one lease, which is declared.
  - `run_at` remains a wall-clock not-before hint. Under skew a scheduled job may run early or late by the skew, which is declared.
- **Components.** `store.py` and `mongo_store.py` (`reclaim_stale_leases`), `client.py`.
- **Failure modes.** FM-CORE-013.
- **Acceptance.** `e20_skew.py` (peer +60 s) gives one execution. A forward jump of the local clock by 10 min during a job gives no reclaim. A worker killed with `kill -9` is reclaimed within lease + window + one poll.
- **Test strategy.** Inject a clock through `store._now_iso` and a monotonic stub.
- **Dependencies.** None.
- **Risk if not implemented.** Duplicate execution after every WSL resume, and on any multi-host Mongo deployment with skew.
- **Assurance.** COR 5 · REC 4 · IDM 5 · OBS 3 · DET 4 · TST 4 · POR 5 · SIM 4 · CMP 5

### ROB-LOOP-001 · The worker event loop never blocks unboundedly · P1

- **Problem.**
  - Store, Galeed and OpenAI-adapter calls run synchronously on the asyncio loop:
    - Galeed down: 2 s per emit (E05)
    - OpenAI-compatible request: the whole request (E06)
    - Mongo: 30 s server selection by default
  - The heartbeats of *every* job freeze with the loop.
  - The preload tick repeats full scans (C21).
  - The metrics scrape fails with the store (C23).
- **Requirement.**
  - Every synchronous call on the loop MUST be bounded below `(lease − margin)/k`, and store work per poll MUST be bounded (ROB-SCHED-001).
  - The OpenAI-compatible adapter MUST run its HTTP call via `asyncio.to_thread` or an async client.
  - Galeed emission MUST be non-blocking (ROB-LOG-001).
  - The store and Galeed `serverSelectionTimeoutMS`/`socketTimeoutMS` and SQLite `busy_timeout` MUST fit the budget, validated at startup (ROB-CONF-001).
  - The preload tick MUST reuse the poll's readiness cache and PROCESSING snapshot.
  - No per-job periodic store polling is allowed apart from the heartbeat.
  - A loop-lag watchdog MUST log `loop_stalled` at WARNING when lag exceeds `lease/3`.
  - Liveness gauges MUST be in memory and independent of the store, and the metrics scrape MUST succeed during a store outage. The gauges are:
    - `hoglah_loop_lag_seconds`
    - `hoglah_last_successful_poll_age_seconds`
    - `hoglah_consecutive_store_errors`
    - `hoglah_delivery_backlog`
- **Components.** `client.py`, `http_adapter.py`, `tracing.py`, `mongo_store.py`, `metrics.py`.
- **Failure modes.** FM-CORE-012, FM-CORE-024, FM-CORE-025, FM-OPENAI-001, FM-GALEED-001.
- **Acceptance.**
  - `e11_openai_block.py`: maximum loop stall < 0.5 s.
  - `e12_galeed_down.py`: `submit()` < 50 ms and stub jobs finish within 1 s.
  - With the store down, `/metrics` returns 200 with `degraded` gauges.
- **Test strategy.** The loop-lag probe used in E06, plus a slow fake store.
- **Dependencies.** ROB-LOG-001 (emitter).
- **Risk if not implemented.** Mass lease lapse, so mass duplicates in multi-worker deployments, and blindness during outages.
- **Assurance.** COR 4 · REC 4 · IDM 4 · OBS 5 · DET 4 · TST 4 · POR 5 · SIM 3 · CMP 4

### ROB-CORE-001 · A successful commit is final · P0

- **Problem.**
  - A store read failure just before the commit turns a provider success into FAILED (E23).
  - Any exception after a successful commit makes Hoglah deliver FAILED for a COMPLETED job (E04).
  - A failure in the optional stream file fails the job (E16).
  - Failures in the failure path are swallowed with `pass`.
- **Requirement.**
  - There MUST be no pre-commit status read. The fenced `set_result` is the only race guard. Transient store errors on that write MUST be retried within the lease.
  - After the write returns True, nothing may change the outcome. Later exceptions (metrics, Galeed, delivery, stream finalisation, callbacks) are delivery or observability failures: they are recorded on the sink once ROB-DELIV-001 lands, and logged at WARNING before then.
  - If the write returns False, the worker MUST NOT deliver anything, and ROB-LEASE-001 applies.
  - An ambiguous commit error, such as a Mongo network error after the write was applied, MUST be resolved by re-reading the fenced row before any delivery.
  - Stream-file errors MUST disable streaming for that job and MUST NOT fail it.
- **Components.** `client.py` (`_process_job`, `_execute_with_retries`, `_open_stream`), `streaming.py`.
- **Failure modes.** FM-CORE-004, FM-CORE-005, FM-CORE-021, FM-CORE-023, FM-STORAGE-002.
- **Acceptance.** `e07_postcommit.py`: every sink says `completed`. `e07b_precommit.py`: `completed`. `e14_stream_fail.py`: `completed`, with a `stream.disabled` WARNING.
- **Test strategy.** Fault injection at every statement between provider return and delivery.
- **Dependencies.** None for the core. The sink-record wording depends on ROB-DELIV-001.
- **Risk if not implemented.** Contradictory results that callers act on.
- **Assurance.** COR 5 · REC 4 · IDM 4 · OBS 4 · DET 5 · TST 5 · POR 5 · SIM 5 · CMP 5

### ROB-CORE-002 · Cancel is a conditional write · P0

- **Problem.** `cancel()` reads the status and then writes unconditionally, so a racing completion is overwritten and its output destroyed (E02).
- **Requirement.**
  - Cancel MUST be one conditional write (`status IN (QUEUED, PROCESSING)` → CANCELLED, clearing the lease token). It MUST return False if it lost the race.
  - It MUST NOT modify a terminal row.
  - A cancelled attempt that had reached `dispatched` gets `outcome=unknown`.
  - The dependent cascade is a best-effort fast path. The poll's readiness check is authoritative.
- **Components.** `client.py` (`_cancel_one`, `_fail_dependents`), both stores.
- **Failure modes.** FM-CORE-002.
- **Acceptance.** `e05_cancel_race.py`: the row stays COMPLETED with its output, and `cancel()` returns False.
- **Test strategy.** Deterministic interleaving with a stalled reader, plus a property test (cancel ∥ commit).
- **Dependencies.** None.
- **Risk if not implemented.** Lost results, and callers told "completed" while the store says "cancelled".
- **Assurance.** COR 5 · REC 5 · IDM 5 · OBS 4 · DET 5 · TST 5 · POR 5 · SIM 5 · CMP 5

### ROB-SCHED-001 · The poll cannot stall, and every gate fails closed · P0

- **Problem.**
  - Waiting rows fill the poll window and deadlock their own dependencies (E01).
  - One undecodable SQLite row aborts every poll (E26).
  - Label-sequence evaluation and slot caps fail *open* on store errors (C11, C20).
  - A dependency read error aborts the whole poll (C19).
- **Requirement.**
  - Readiness (`depends_on`, label sequence, batch seal) MUST be evaluated once per group per poll and cached.
  - The poll MUST page until it finds claimable rows or reaches `max_scan`. Waiting rows MUST NOT consume the claim window.
  - Every admission gate (readiness, model/session/tag slots, rate limits) MUST fail **closed** on error.
  - Errors MUST be isolated per row.
  - SQLite `list` MUST tolerate undecodable rows and quarantine them to `FAILED corrupt_row`. A schema-invalid request on either store is `corrupt_row`, not "Unexpected worker error".
  - Dependency cycles MUST be rejected at enqueue.
  - A QUEUED-and-due row older than a threshold MUST raise `job_stuck`, and `max_scan` exhaustion MUST be counted.
- **Components.** `client.py` (`_worker_loop`, `_eval_*`, `_try_reserve_job`, `_preload_demand`), `store.py`, `labels.py`.
- **Failure modes.** FM-CORE-001, FM-CORE-014, FM-CORE-015, FM-CORE-016, FM-CORE-025.
- **Acceptance.**
  - `e04_hol.py N=60` completes.
  - `e26_poison_row.py`: the healthy jobs complete and the bad row becomes `corrupt_row`.
  - An injected `list()` error causes no out-of-order label dispatch.
  - An injected snapshot error causes no oversubscription.
- **Test strategy.** Fault injection per gate, plus large-queue tests.
- **Dependencies.** ROB-LOOP-001 (gauges).
- **Risk if not implemented.** Silent permanent stalls and ordering violations.
- **Assurance.** COR 5 · REC 4 · IDM 4 · OBS 5 · DET 4 · TST 5 · POR 5 · SIM 3 · CMP 4

### ROB-DELIV-001 · Durable delivery sinks · P1

- **Problem.**
  - Named callbacks are re-fired on every start for the 200 oldest rows (E15).
  - HTTP callbacks run on daemon threads with no persistence and no delivery id.
  - The output file is not fsync'd.
  - The broker outbox is swept only at startup (W14).
  - A direct callback can be lost to a race (E05).
  - A cancel from the CLI loses its notifications (N43).
  - A broker config change republishes to the wrong cluster (N53).
- **Requirement.**
  - **Sink records.** Every terminal write (fenced commit, cancel, `dependency_blocked`, `corrupt_row`) MUST write one sink record per configured sink:
    - Kinds: output_file, http_callback, named_callback, broker.
    - Fields: state `pending|delivered|failed`, attempts, a target snapshot (path, URL, callback name, or credential-free versioned `broker_id` + route), and `delivery_id = job:attempt:sink[:route]`.
    - The records MUST be written in the same row or document write as the terminal status.
    - A terminal sink state never reverts.
  - **Sweeping.**
    - A sweeper in any worker completes `pending` sinks whose exact target it can reach. Only the state flip and the attempt increment are CAS writes; concurrent duplicate sends are allowed and idempotent by `delivery_id`.
    - Sweepers MUST skip sinks younger than twice their delivery timeout.
    - Each sink gets bounded attempts, then becomes `failed` with a final event and `hoglah_delivery_failed_total`.
    - A sink still pending after `sink_ttl` (default 7 d) becomes `failed/undeliverable`. Any process may apply this.
  - **Per-sink delivery rules.**
    - HTTP POSTs MUST carry `Idempotency-Key: <delivery_id>`.
    - Broker messages MUST carry the delivery id.
    - Files MUST use a unique temp name, fsync of the file and directory, then rename.
    - Named-callback redelivery is driven by sink state, never by a "first 200" scan.
  - **Direct callables** are process-local and best effort:
    - They MUST be registered before the enqueue commit.
    - They are fired immediately on an idempotency hit of an already-terminal job.
    - A `start_worker=False` instance MUST warn that they will not fire there.
  - **Reporting.** `stats()` and the CLI MUST report pending sinks by kind with the oldest age.
- **Components.** `client.py` (`_deliver`, `_post_callback`, `_fire_callback`, `_redeliver_restart_callbacks`, `submit`), `kafka_bridge.py` (outbox), both stores.
- **Failure modes.** FM-DELIV-001…005, FM-BRIDGE-005, FM-BRIDGE-008, FM-CORE-004.
- **Acceptance.**
  - `e09_redeliver.py`: no re-fire across restarts, and an interrupted delivery completes after restart.
  - Kill the process right after completion: the HTTP sink is delivered once per `delivery_id`.
  - Broker down for 30 s: the result is published without a restart.
  - A CLI `hoglah cancel` gets its sinks completed by a worker.
- **Test strategy.** A fake HTTP receiver recording Idempotency-Keys, plus kill tests.
- **Dependencies.** ROB-CORE-001, ROB-STATE-001 (`attempt_no` in `delivery_id`).
- **Risk if not implemented.** Callers never learn results, or learn them repeatedly.
- **Assurance.** COR 4 · REC 5 · IDM 5 · OBS 5 · DET 4 · TST 4 · POR 4 · SIM 2 · CMP 3

### ROB-BRIDGE-001 · Ingress never skips; submitter outcomes are resolvable · P0

- **Problem.**
  - A Kafka handler failure is logged "not acking (will retry)", but the next commit skips the message, so it is lost (E18).
  - Rabbit has no consumer reconnect, and prefetch is 1.
  - Redis reads its pending-entries list only once.
  - `MessagingSubmitter` mints a fresh correlation id per call, so a retry after a timeout duplicates the job (N42). A retry with the same id never resolves on Rabbit or Redis (N48).
- **Requirement.**
  - Ingress MUST NOT acknowledge or commit past an unprocessed message:
    - Kafka MUST seek back to the failed offset, or pause.
    - Rabbit MUST nack with requeue plus backoff, and reconnect its consumer.
    - Redis MUST periodically `XAUTOCLAIM` idle pending entries.
  - A store outage MUST pause the consumer and not count against the message.
  - Other failures count toward a per-message poison budget. When it is exhausted, the message goes to the DLT, and is acknowledged only after the DLT write is durable.
  - `MessagingSubmitter.submit` MUST accept a caller-supplied `correlation_id`, and its documentation MUST say to reuse it on retry.
  - An unacknowledged publish or an expired wait MUST raise `SubmitOutcomeUnknown(correlation_id, resume_token)`, where the resume token is the server-side results position captured before publishing. A definite publish error raises `MessagingSubmitError`.
  - The job's execution timeout MUST be separate from the submitter's wait timeout.
  - On an ingress dedupe hit whose reply route is new (Rabbit), the bridge appends a broker sink for that route. This lands with ROB-DELIV-001, supersedes older pending routes, and is capped by `max_reply_routes` (default 8).
  - Declared:
    - Resolution beyond results-log retention is a store lookup by `correlation_id`.
    - Dedupe holds only within `dedupe_window`.
- **Components.** `kafka_bridge.py`, `rabbitmq.py`, `redis_streams.py`, `messaging_submitter.py`.
- **Failure modes.** FM-BRIDGE-001…004, FM-BRIDGE-006, FM-BRIDGE-007.
- **Acceptance.**
  - `e19_kafka_skip.py`: `c1` is enqueued.
  - A Rabbit broker restart: consumption resumes without a process restart.
  - Redis: a failed entry is re-handled without a restart.
  - A submitter retry with the same id and resume token returns the original job's result, and only one job exists.
- **Test strategy.** Fake transports that model each broker's commit semantics, plus gated live-broker tests (`RUN_*_TESTS`).
- **Dependencies.** ROB-DELIV-001 for the sink-append clause. The no-skip and submitter clauses are independent.
- **Risk if not implemented.** Silent message loss contradicting README's "crash-safe", and duplicate jobs.
- **Assurance.** COR 5 · REC 4 · IDM 5 · OBS 4 · DET 4 · TST 4 · POR 4 · SIM 3 · CMP 4

### ROB-CLI-001 · Subprocess and timeout hygiene · P1

- **Problem.**
  - CLIs inherit stdin, so Codex hangs (E09).
  - The prompt can be parsed as a flag (E08).
  - Prompts over 128 KiB fail (E21).
  - There is no default timeout, and `ollama.AsyncClient` has none either (FM-OLLAMA-005).
  - SIGKILL escalation reaches only the process-group leader.
  - Children survive a worker death (E12, E14).
- **Requirement.**
  - CLI subprocesses MUST run with `stdin=DEVNULL`, or receive the prompt via stdin or a prompt file:
    - `claude -p` reads stdin
    - `codex exec -` reads stdin
    - grok has `--prompt-file`
    - gemini and kiro use a file or stdin where supported
  - The prompt MUST NOT be passable as an option. A prompt file or stdin is mandatory above 64 KiB.
  - Every adapter MUST have default connect, idle and total timeouts, configurable per backend, so that no job runs unbounded unless explicitly requested.
  - Cancel or timeout MUST send SIGTERM to the process group, then SIGKILL to the **group** after a grace period.
  - The worker MUST persist `pid`, `pgid`, `host_id`, `boot_id` and `start_time` on the attempt.
  - A recovering worker on the same host and boot MUST reap a surviving group before any re-run. It verifies the start time to guard against pid reuse.
  - Otherwise the attempt is marked `orphan_unreapable` and blocks requeue without `force` (ROB-AMBIG-001).
- **Components.** `cli_adapters.py`, `adapters.py` (client construction), `http_adapter.py`.
- **Failure modes.** FM-CLI-001…006, FM-OLLAMA-005, FM-OPENAI-001.
- **Acceptance.**
  - A codex fake reading stdin under an open pipe finishes or times out at the default.
  - The prompt `--version` reaches the model as text.
  - A 1 MiB prompt works.
  - A SIGTERM-immune grandchild is killed on cancel.
  - After `kill -9` of the worker, the restarted worker reaps the orphan before deciding.
- **Test strategy.** `fakecli.py` modes, plus gated real-CLI smoke tests (claude, codex, grok installed; gemini and kiro by fake only).
- **Dependencies.** ROB-STATE-001 (fields on the attempt).
- **Risk if not implemented.** Stuck jobs, and concurrent duplicate agents in one workdir.
- **Assurance.** COR 5 · REC 4 · IDM 4 · OBS 3 · DET 5 · TST 5 · POR 3 (process groups are POSIX; Windows uses Job Objects) · SIM 4 · CMP 5

### ROB-BACKEND-001 · Backend result contracts · P0 (MUST parts) / P2 (doc-only parts)

- **Problem.**
  - Exit 0 is the only success criterion, so empty, stderr-only, footer-only or flag-printed output is COMPLETED (E07, E08, FM-KIRO-002).
  - Streamed CLI output is corrupted for non-ASCII text (E07).
  - Ollama truncates input silently (E19), and an empty `load` response counts as success.
  - CLI errors on stdout are dropped (E10).
  - Ollama auto-pulls inside the job (E20).
- **Requirement.**
  - A "success" with an empty, noise-only or non-final payload MUST be `protocol_error`, never COMPLETED.
  - MUST, where real-run evidence exists:
    - Per-backend structured output and exit-code parsing for claude (`--output-format json`/`stream-json`, using `is_error` and `api_retry.error`), codex (`--json`, using `turn.failed`/`error`) and grok (json).
    - Incremental UTF-8 decoding of streamed output.
    - Error excerpts taken from both streams, sanitised, tail-preserving, with any signal named.
    - Ollama input-truncation detection (`prompt_eval_count >= num_ctx` → `truncated=True`, `truncation_reason="input"`).
    - The streamed Ollama and SSE responses must end with their terminal chunk.
  - SHOULD, where only documentation exists: gemini exit codes 41/42/44/52/53, kiro exit 3, and a kiro noise denylist (declared weak).
  - Each CLI's `--version` MUST be recorded at worker start and on every attempt. A version outside the tested range raises `cli_version_drift` (WARNING), without blocking.
  - Ollama auto-pull MUST be opt-in and MUST NOT run inside a job attempt.
  - Claude SHOULD be invoked with `--bare` (see ROB-SEC-001).
- **Components.** `cli_adapters.py`, `adapters.py`, `http_adapter.py`.
- **Failure modes.** FM-CLI-003, FM-CLI-007…012, FM-CODEX-001, FM-GEMINI-001, FM-KIRO-001/002, FM-OLLAMA-002/003/004/006, FM-OPENAI-002, FM-RETRY-003.
- **Acceptance.**
  - `e13_fake.py stream`: the Hebrew output is intact.
  - empty and stderr-only → `FAILED protocol_error`.
  - Ollama with `num_ctx=128` and a 1,774-token prompt → `truncated=True` with reason `input`.
  - An empty prompt → `protocol_error`.
  - A missing model with auto-pull off → `model_unavailable` in < 1 s with no network call.
- **Test strategy.** Recorded real outputs (the probes in PROVIDER-FAILURES.md) as fixtures, plus gated smoke tests.
- **Dependencies.** ROB-CLASS-001.
- **Risk if not implemented.** Wrong answers stored as success. Silent corruption is the hardest failure class to notice.
- **Assurance.** COR 5 · REC 3 · IDM 3 · OBS 5 · DET 4 · TST 4 · POR 4 · SIM 3 · CMP 4

### ROB-CLASS-001 · Structured classification · P1

- **Problem.** `classify_error` scans `str(exc)`, so `num_ctx=512` and `503 tokens` are treated as transient (E11). Every CLI failure classifies as `[]` (E10). Only the error string is stored.
- **Requirement.**
  - Classification MUST map to the §1 taxonomy and set `failure.category`, `retryable` and `provider_code`.
  - It MUST use structured signals first: exception type, `status_code`, exit code, signal, JSON error fields, `api_retry.error`.
  - Regex MAY be used only on provider-originated error text, never on text that contains the prompt or model output.
  - `unknown` follows §1.4.
  - `store_unavailable`, `lease_lost`, `corrupt_row`, `outcome_unknown` and `attempts_exhausted` are Hoglah-internal and never come from provider text.
  - `RetryPolicy.retry_on` names stay valid as aliases (`transient` = network_transient ∪ provider_transient ∪ provider_overloaded ∪ rate_limit; `oom` = resource_exhaustion).
- **Components.** `models.py`, every adapter.
- **Failure modes.** FM-RETRY-001/003/004, FM-CLI-011, FM-GEMINI-002.
- **Acceptance.** The E10 and E11 table becomes unit tests with the expected categories. No classification depends on words in the prompt.
- **Test strategy.** Table-driven unit tests over recorded real errors.
- **Dependencies.** None.
- **Risk if not implemented.** Wrong retries and opaque failures.
- **Assurance.** COR 4 · REC 3 · IDM 4 · OBS 5 · DET 5 · TST 5 · POR 5 · SIM 4 · CMP 4

### ROB-RETRY-001 · Persisted retry scheduling · P0 (fenced requeue) / P2 (budgets)

- **Problem.** Retries sleep inside the claim. They hold a slot, they are lost on restart, and the default budget (3 attempts in about 3 s) fails every queued job during any Ollama restart (E20).
- **Requirement.**
  - P0, shipped with ROB-STATE-001: a retry MUST be a fenced `PROCESSING→QUEUED` write with `run_at = next_attempt_at`, never an in-claim sleep. One attempt equals one claim equals at most one dispatch (N58).
  - P2:
    - Per-category budgets with `max_elapsed`. `provider_unavailable`/`network_transient` budgets are sized beyond daemon restart time, defaulting to minutes.
    - `rate_limit` honours Retry-After, and `quota` waits for its known reset time.
    - `authentication`, `authorisation`, `model_unavailable`, `caller_invalid`, `context_limit` and `configuration_error` fail fast.
    - The default jitter MUST be > 0, to prevent synchronized storms.
    - Cancellation and shutdown during a backoff need nothing extra, because the job is QUEUED.
  - A per-backend health gauge (consecutive failures by category) is exported as a metric. **No circuit breaker** is required: see §13 below.
- **Components.** `client.py`, `models.py`.
- **Failure modes.** FM-CORE-020, FM-RETRY-002, FM-OLLAMA-001.
- **Acceptance.**
  - Kill during a backoff: the attempt count is preserved and the job runs at `run_at`.
  - Ollama down for 30 s with 20 queued jobs: all complete after it returns.
  - An authentication failure is not retried.
- **Test strategy.** Clock-injected unit tests, plus an integration test with the adapter down then up.
- **Dependencies.** ROB-STATE-001.
- **Risk if not implemented.** Mass permanent failure on routine restarts, and lost retry budgets.
- **Assurance.** COR 5 · REC 5 · IDM 4 · OBS 4 · DET 4 · TST 5 · POR 5 · SIM 4 · CMP 4

### ROB-LOG-001 · Galeed events that reconstruct incidents, emitted safely · P1

- **Problem.**
  - Five lifecycle events exist, carrying `error[:120]` as a summary.
  - Emission is synchronous and blocking (E05). Failures are logged at DEBUG (invisible).
  - Cancel is emitted with `status="completed"`.
  - Galeed is optional infrastructure, yet today it can change job semantics.
- **Requirement.**
  - Hoglah MUST emit the events in [§G](#g-galeed-observability-requirements), keyed by `(job_id, attempt_no, seq, event_id)`, through a bounded non-blocking queue drained by one background thread. Losses MUST be counted (`hoglah_trace_dropped_total`), and the drop count reported on the next successful event.
  - Galeed failures MUST be logged at WARNING, rate-limited.
  - Each event's `status` MUST equal the row's resulting status.
  - The emitter MUST NOT raise into callers and MUST NOT block the submit thread or the worker loop.
  - A local JSONL spool with replay deduped by `event_id` SHOULD be offered.
  - `record_io` stays opt-in for secrets (ROB-SEC-001).
  - The store is authoritative for retained rows. After `clear`, Galeed is the only history, which is declared.
- **Components.** `tracing.py`, `client.py`; Galeed (event id uniqueness).
- **Failure modes.** FM-GALEED-001…004.
- **Acceptance.**
  - Galeed down: no caller blocks, `hoglah_trace_dropped_total` rises, and one WARNING appears per interval.
  - Galeed back: the next event carries `dropped_since_last`.
  - Each fault test in the matrix asserts its event sequence.
- **Test strategy.** A fake Galeed database that raises or is slow.
- **Dependencies.** ROB-STATE-001 (`attempt_no`).
- **Risk if not implemented.** Outages that cannot be diagnosed, and the trace layer able to stall the queue.
- **Assurance.** COR 4 · REC 3 · IDM 4 · OBS 5 · DET 4 · TST 4 · POR 5 · SIM 4 · CMP 5

### ROB-SHUT-001 · Signal handling and liveness · P1

- **Problem.** SIGTERM exits immediately with no drain or `close()`, leaving orphaned CLI children (E14). `hoglah run` never notices a dead worker thread.
- **Requirement.**
  - `hoglah run`, and the bridge commands, MUST handle SIGTERM and SIGINT: stop claiming, drain within a bounded time (configurable, default less than common supervisor kill timeouts), terminate CLI process groups, record interrupted attempts per ROB-AMBIG-001, then `close()`.
  - The process MUST exit non-zero if the worker thread dies.
  - Supervisor design itself is out of scope.
- **Components.** `cli.py`, `client.py` (`close`).
- **Failure modes.** FM-CORE-010, FM-CORE-011.
- **Acceptance.** `e18_sigterm.sh`: "Shutting down", no orphan, and the interrupted attempt recorded. Killing the worker thread exits with code ≠ 0.
- **Test strategy.** Subprocess signal tests.
- **Dependencies.** ROB-CLI-001, ROB-AMBIG-001.
- **Risk if not implemented.** Every planned stop behaves like a crash.
- **Assurance.** COR 4 · REC 4 · IDM 4 · OBS 4 · DET 4 · TST 5 · POR 3 (signals differ on Windows) · SIM 5 · CMP 5

### ROB-SEC-001 · Secrets and trust boundaries · P1

- **Problem.**
  - A per-job `metadata.api_key` is copied into results, files, callbacks and broker payloads (E24).
  - `record_io` stores full prompts.
  - `claude -p` runs workdir hooks and MCP servers in untrusted folders.
  - Broker messages can select local agent backends.
- **Requirement.**
  - Secret-bearing keys (`api_key`, `authorization`, `token`, `password`, and a configurable list) MUST be redacted from every sink, excerpt, Galeed event and `record_io` record.
  - Credentials MUST NOT be stored in sink target snapshots.
  - Claude SHOULD run with `--bare`.
  - Broker-originated jobs MUST be limited to an allow-list of backends (default: non-CLI).
  - `serve` MUST warn when it binds to a non-loopback address.
- **Components.** `client.py`, `tracing.py`, `cli_adapters.py`, `kafka_bridge.py`, `cli.py`.
- **Failure modes.** FM-SEC-001, FM-SEC-002, FM-CLAUDE-002.
- **Acceptance.** Submitting with `api_key`, then grepping every sink and Galeed record, finds 0 hits. A broker job with `backend=claude` is rejected by default.
- **Test strategy.** Unit plus integration grep tests.
- **Dependencies.** None.
- **Risk if not implemented.** Credential leakage to callback receivers and log stores.
- **Assurance.** COR 4 · REC 2 · IDM 5 · OBS 4 · DET 5 · TST 5 · POR 5 · SIM 4 · CMP 4

### ROB-CONF-001 · Configuration validation · P2

- **Problem.** The documented `k=v` env formats crash startup (E22). Timeout and lease relationships are not checked. A SQLite file on `/mnt/c` with WAL is unsafe.
- **Requirement.**
  - The documented env formats MUST parse (or the docs MUST change).
  - Startup MUST validate:
    - `heartbeat_interval < lease/3`
    - store/Galeed timeouts and `busy_timeout` < the ROB-LOOP-001 budget
    - drain timeout < lease
  - Startup MUST warn when `db_path` is on drvfs or 9p.
- **Components.** `config.py`.
- **Failure modes.** FM-CONF-001, FM-STORAGE-001.
- **Acceptance.** The env matrix test passes. A drvfs path warns.
- **Test strategy.** Unit.
- **Dependencies.** None.
- **Risk if not implemented.** Misconfigurations surface as runtime failures.
- **Assurance.** COR 4 · REC 2 · IDM 3 · OBS 4 · DET 5 · TST 5 · POR 4 · SIM 5 · CMP 5

### ROB-STATE-003 · Normative transition table · P0

- **Problem.**
  - There is no single definition of legal transitions.
  - `store.requeue` accepts COMPLETED.
  - Deletes are unconditional.
  - Several paths write unconditionally.
- **Requirement.** Every status change MUST be one conditional write that matches this table. Anything else fails closed.

  | Transition | Only via |
  |---|---|
  | QUEUED → PROCESSING | claim (attempt+1; batch sealed or unbatched; ROB-SCHED-001 gates) |
  | PROCESSING → QUEUED | fenced retry (ROB-RETRY-001), or reclaim of a rerunnable job / pre-dispatch attempt (ROB-LEASE-002, ROB-AMBIG-001) |
  | PROCESSING → COMPLETED/FAILED | fenced commit by the lease holder |
  | PROCESSING → FAILED | reclaim-fail: CAS on the observed unchanged `(token, expires)` giving `outcome_unknown` or `attempts_exhausted` |
  | QUEUED/PROCESSING → CANCELLED | conditional cancel (ROB-CORE-002) |
  | QUEUED → FAILED | conditional write: `dependency_blocked`, `corrupt_row` |
  | FAILED/CANCELLED → QUEUED | operator requeue only (new epoch; ROB-AMBIG-001 preconditions) |
  | terminal → deleted | ROB-ADMIN-001 only |
  | unsealed → deleted | abandoned-batch cleanup only (ROB-BATCH-001) |

  - COMPLETED is absorbing apart from deletion.
  - Defined races: cancel vs commit, the first conditional write wins; cancel vs claim, remote abort and outcome unknown; reclaim vs commit, fencing; delete vs claim, delete loses; seal vs abandon, header CAS.
- **Components.** Both stores, `client.py`.
- **Failure modes.** FM-CORE-002, FM-CORE-018, FM-CORE-019, FM-ADMIN-002.
- **Acceptance.** A model-based property test (Hypothesis `RuleBasedStateMachine`) over submit, claim, commit, cancel, reclaim, requeue and delete, interleaved across two simulated workers, never reaches a state that violates INV-1…14.
- **Test strategy.** Property / state-machine tests.
- **Dependencies.** ROB-STATE-001.
- **Risk if not implemented.** Contradictory states with no specification to arbitrate.
- **Assurance.** COR 5 · REC 5 · IDM 5 · OBS 3 · DET 5 · TST 5 · POR 5 · SIM 4 · CMP 4

### ROB-BATCH-001 · Batch submission safety · P0 (compensation scope) / P2 (Mongo sealing)

- **Problem.** A crash mid-`submit_batch` leaves a running prefix with missing children. Compensation can delete *pre-existing* jobs matched by an idempotency key (C25).
- **Requirement.**
  - P0:
    - Item idempotency keys MUST be resolved before ids are assigned, so an existing job maps into the name table.
    - Compensation MUST touch only rows this call inserted.
    - Local names MUST NOT shadow existing ids.
    - External `depends_on` SHOULD be validated at enqueue.
  - P2, atomic release:
    - SQLite inserts all items in one transaction.
    - Mongo writes a header `open`, then rows carrying a fresh `seal_token`, then CASes the header `open→sealed`.
    - Abandonment CASes `open→abandoned` and deletes that token's rows. Sealed and abandoned are absorbing.
    - An ambiguous seal raises `BatchOutcomeUnknown(token)`, resolved by reading the header.
    - `wait_batch` resolves membership through the header.
    - `batch_id` is a non-unique grouping label, which is declared.
    - Mongo sealing requires every worker at or above the sealing version, which is declared.
- **Components.** `client.py` (`submit_batch`, `wait_batch`, `cancel_batch`), `batch.py`, `mongo_store.py`.
- **Failure modes.** FM-BATCH-001, FM-BATCH-002.
- **Acceptance.**
  - A batch whose item matches an existing key, plus a failing item: the existing job survives.
  - Kill mid-batch: no item of the batch is ever claimed (SQLite and Mongo).
- **Test strategy.** Integration tests with kill points, plus fake-Mongo partial writes.
- **Dependencies.** ROB-SCHED-001 (sealed-token gate).
- **Risk if not implemented.** Deletion of unrelated jobs, and half-run pipelines.
- **Assurance.** COR 4 · REC 4 · IDM 5 · OBS 3 · DET 4 · TST 4 · POR 4 · SIM 2 · CMP 3

### ROB-ADMIN-001 · Safe deletion and retention · P0

- **Problem.** `hoglah clear` deletes before asking and deletes anyway when the answer is "no" (E25). Store deletes are unconditional on both backends: they remove running jobs, parents of live dependents and unpublished outbox rows (C24). Dedupe disappears with the row (N41).
- **Requirement.**
  - `clear` MUST count (dry run), confirm, then delete.
  - The store-level `delete_job`/`delete_jobs` MUST by default skip:
    - non-terminal rows
    - unsealed rows
    - rows with pending sinks (before ROB-DELIV-001: `result_published=0` with a correlation id)
    - rows referenced by a live dependent or label peer
    - rows carrying a dedupe key that are younger than `dedupe_window`
  - Override map:
    - `--force` overrides only the dedupe window.
    - `--force-nonterminal` cancels first, then deletes.
    - Unsealed rows are removed only by batch cleanup.
    - Rows with live dependents are never removed.
  - `rm` MUST report "deferred: <reason>".
  - Every deletion emits `rows_deleted`.
- **Components.** `cli.py` (`clear`, `rm`), `client.py` (`clear`, `remove`), both stores.
- **Failure modes.** FM-ADMIN-001, FM-ADMIN-002, FM-ADMIN-003.
- **Acceptance.** `echo n | hoglah clear` deletes nothing. `hoglah clear` with no filter keeps QUEUED and PROCESSING rows. Deleting a parent of a live child is refused.
- **Test strategy.** CLI tests on a temporary database (SQLite and Mongo).
- **Dependencies.** None for the `clear` ordering fix, which is immediate. The full guards depend on ROB-DELIV-001.
- **Risk if not implemented.** Operator-triggered data loss with a reassuring message.
- **Assurance.** COR 5 · REC 4 · IDM 4 · OBS 4 · DET 5 · TST 5 · POR 5 · SIM 4 · CMP 3 (`clear` keeps rows it used to delete)

### ROB-TEST-001 · Robustness test suite · applies to every requirement

- **Requirement.**
  - Each requirement MUST ship with the deterministic fault-injection tests named in its acceptance criteria and in [FAULT-INJECTION-MATRIX.md](FAULT-INJECTION-MATRIX.md). Each test asserts **state, cause, action and Galeed event sequence**.
  - The suite MUST include:
    - (a) deterministic unit tests for classification, retry decisions, state transitions, parsers, exit codes and malformed outputs
    - (b) fault-injection integration tests for store, provider, callback, lease and stream failures
    - (c) restart tests that kill a real worker process and restart it on a persisted queue
    - (d) model-based property tests over ROB-STATE-003
    - (e) **gated** real-provider smoke tests (`RUN_OLLAMA_TESTS`, `RUN_CLAUDE_TESTS`, `RUN_CODEX_TESTS`, `RUN_GROK_TESTS`; gemini/kiro by fake until installed). These are never on in CI by default and use the minimum tokens: bad-model and auth probes plus one tiny prompt.
  - `experiments/` are reproductions, to be converted, not tests.
- **Assurance.** COR 4 · REC 4 · IDM 4 · OBS 4 · DET 5 · TST 5 · POR 4 · SIM 3 · CMP 5

---

## Invariants

These are derived from ROB-STATE-003 and the requirements above, not a second
source. Each one is a property-test target.

1. **INV-1** Every status change is a conditional (compare-and-set) write. No unconditional status write exists.
2. **INV-2** COMPLETED is absorbing. FAILED or CANCELLED rows leave only by operator requeue (new epoch) or ROB-ADMIN-001 deletion.
3. **INV-3** A PROCESSING row has exactly one live lease token. Only its holder can commit, retry or renew, except reclaim-fail and conditional cancel, which act by CAS on the observed lease.
4. **INV-4** `attempt_no` increases with every claim and never resets within an epoch. Attempts per epoch ≤ `max_retries+1`.
5. **INV-5** An attempt that reached `dispatched` without a recorded end has `outcome=unknown` after reclaim or cancel. It is never recorded as not-run.
6. **INV-6** A non-rerunnable job is never dispatched automatically twice within one epoch.
7. **INV-7** After a successful fenced commit, the job's status and result change only by operator requeue or deletion.
8. **INV-8** Every terminal row has at least one sink record per configured sink, each with a fixed target snapshot. A terminal sink state never reverts. Every sink becomes terminal within its bounded attempts or `sink_ttl`.
9. **INV-9** A row is claimable only if it is unbatched or its batch token is sealed. Sealed and abandoned are absorbing and mutually exclusive.
10. **INV-10** A due QUEUED row whose ROB-SCHED-001 admission gates are satisfied is admitted within a bounded number of polls, regardless of other rows' readiness or decodability.
11. **INV-11** An ingress message is acknowledged only after its job row, or a DLT copy, is durable. After a dedupe hit, the result can still be obtained on the caller's reply route, subject to `max_reply_routes`.
12. **INV-12** Deletion follows ROB-ADMIN-001's override map. No flag deletes unsealed rows or rows with live dependents.
13. **INV-13** Every Galeed event's `status` equals the row's resulting status. Galeed loss never changes job state.
14. **INV-14** Cause, action and state are separate fields. When a structured signal exists, none of them is inferred from free text.

The brief's example invariants are all kept, in stronger form. One example was rejected as worded: "a job may only be marked completed after the canonical result is durably stored" is true today *and* insufficient. Completion and result are one fenced write (`set_result`), and the real gaps are on either side of it (ROB-CORE-001).

---

## G. Galeed observability requirements

### G.1 Gaps in today's integration

**Events.** Hoglah emits only `job.queued`, `job.started`, `job.completed`, `job.failed` and `job.cancelled`, plus `llm_calls` I/O records.
- The failure detail is `error[:120]` inside a summary string.
- Events carry no attempt number, failure category, exit code or signal, and no retry decision.
- There are no events for reclaim, lease loss, outcome unknown, delivery or recovery.

**Emission.** Emission is synchronous on caller threads with a 2 s Mongo timeout (E05). Every failure goes to DEBUG.

**Accuracy.** `job.cancelled` carries `status="completed"`.

### G.2 Required events

Common fields on every event:
- `trace_id` (the caller's `metadata.trace_id`, or the job id), `job_id`, `session_id`, `batch_id`, `parent_job_id`, `depends_on`
- `attempt_no`, `seq`, `event_id`
- `worker_id` (host:pid), `host_id`, `boot_id`
- `backend`, `adapter`, `model`, `cli_version`
- `status_before`, `status_after`

| Event | When | Extra fields |
|---|---|---|
| `job.queued` | enqueue committed | priority, run_at, idempotency hit? |
| `attempt.started` | phase=dispatched committed | phase, timeout budget |
| `attempt.finished` | attempt ends | outcome, failure{category, provider_code, exit_code, signal, http_status, exception_class, cause_chain, excerpt (sanitised, ≤ 2 KiB)}, duration_ms, ambiguous? |
| `retry.scheduled` | fenced requeue | decision.action, delay, run_at, budget remaining |
| `lease.renew_failed` / `lease.lost` / `lease.result_discarded` | ROB-LEASE-001 | renewal errors, output digest |
| `lease.reclaimed` | ROB-LEASE-002 | observed window, previous owner, prior phase |
| `attempt.outcome_unknown` | ROB-AMBIG-001 | prior phase, orphan pid status (reaped/unreapable) |
| `job.completed` / `job.failed` / `job.cancelled` | terminal commit | final category, action, attempts in epoch |
| `delivery.attempted` / `delivery.failed` / `delivery.final` | ROB-DELIV-001 | sink, delivery_id, target (redacted), http_status, error |
| `ingress.paused` / `ingress.poison` / `ingress.dlt` | ROB-BRIDGE-001 | broker, offset/id, reason |
| `job.stuck` / `worker.degraded` / `worker.recovered` / `loop.stalled` | ROB-SCHED-001, ROB-LOOP-001 | ages, lag, consecutive store errors |
| `row.quarantined` / `rows.deleted` / `batch.abandoned` | ROB-SCHED-001, ROB-ADMIN-001, ROB-BATCH-001 | counts, filter, reason |
| `recovery.startup` | worker start | reclaimed, reaped, outcome_unknown, pending sinks |
| `cli.version_drift` | worker start | backend, version, tested range |
| `requeue` | operator requeue | prior attempt summary, force?, skipped ids |

Secrets MUST never appear: the redaction list in ROB-SEC-001 applies to every field.

### G.3 Degradation hierarchy (logging must not become a single point of failure)

1. **Galeed structured event.** Enqueued to a bounded in-memory queue (default 10,000), drained by one background thread with short Mongo timeouts.
2. **Queue full or Galeed unreachable.** Drop the oldest events and increment `hoglah_trace_dropped_total{reason}`. Log at WARNING through Python `logging`, at most one line per interval per reason.
3. **Optional (SHOULD) local JSONL spool.** Bounded size, replayed when Galeed returns, deduped by `event_id`.
4. **On recovery.** The next event carries `dropped_since_last`, so the gap is visible centrally.
5. Python logging is the last resort, and the job store remains authoritative. **No step may raise into job processing or block a caller.**

---

## H. Recovery decision matrix

| Failure class | Retry? | Delay | Job state after | Reclaim? | Human action? | Galeed event |
|---|---:|---|---|---:|---:|---|
| `network_transient` (provider unreachable) | yes | exp. backoff + jitter, elapsed budget ≥ minutes | QUEUED (`run_at`) | — | after budget | retry.scheduled |
| `provider_overloaded` (503/529) | yes | longer backoff; Retry-After if present | QUEUED | — | no | retry.scheduled |
| `provider_transient` (5xx, no validation text) | yes | backoff | QUEUED | — | after budget | retry.scheduled |
| 5xx carrying validation text (Ollama option error) | once | backoff | FAILED `caller_invalid` | — | fix request | attempt.finished |
| `rate_limit` (429) | yes | Retry-After or backoff | QUEUED | — | no | retry.scheduled |
| `quota` (allowance exhausted) | at reset only | until known reset, else fail | QUEUED with `run_at`=reset, or FAILED `quota` | — | top-up / wait | retry.scheduled / job.failed |
| `authentication` | no | — | FAILED | — | log in / fix key | job.failed |
| `authorisation` / tool denied | no | — | FAILED | — | grant / change mode | job.failed |
| `model_unavailable` | no | — | FAILED | — | pull model | job.failed |
| `context_limit` / input truncated | no | — | FAILED, or COMPLETED with `truncated=True` per job policy | — | shorten input | attempt.finished |
| `caller_invalid` (incl. E2BIG, bad options) | no | — | FAILED | — | fix request | job.failed |
| `configuration_error` (binary missing, bad config) | no | — | FAILED | — | fix config | job.failed |
| `resource_exhaustion` (OOM) | only if `oom` opted in | long backoff | FAILED or QUEUED | — | free memory / smaller model | attempt.finished |
| `timeout` (attempt) | per policy (default no) | backoff | FAILED `timeout` or QUEUED | — | raise limit | attempt.finished |
| `process_failure` (signal, crash) | once (rerunnable) | backoff | QUEUED or FAILED | — | if repeated | attempt.finished |
| `protocol_error` (empty/noise/schema) | once | backoff | FAILED `protocol_error` | — | check CLI version | attempt.finished, cli.version_drift |
| `partial_response` | rerunnable only | backoff | QUEUED or FAILED | — | no | attempt.finished |
| worker death **before** dispatch | yes (no attempt consumed beyond claim) | none | QUEUED | yes (ROB-LEASE-002) | no | lease.reclaimed |
| worker death **after** dispatch, rerunnable | yes, within bound | none | QUEUED | yes | no | attempt.outcome_unknown |
| worker death **after** dispatch, non-rerunnable | **no** | — | FAILED `outcome_unknown` | yes (to fail) | **yes, review and requeue** | attempt.outcome_unknown |
| repeated worker death (poison) | no | — | FAILED `attempts_exhausted` | yes | yes | job.failed |
| `lease_lost` mid-attempt | per AMBIG rule | — | per AMBIG rule | — | no | lease.lost |
| `store_unavailable` during heartbeat | renew within lease | short | unchanged, or lease_lost | — | if prolonged | lease.renew_failed, worker.degraded |
| `store_unavailable` at commit | retry commit within lease | short | COMPLETED/FAILED, or lease_lost | — | if prolonged | lease.lost |
| `corrupt_row` | no | — | FAILED `corrupt_row` (quarantine) | — | inspect row | row.quarantined |
| `dependency_blocked` | no | — | FAILED | — | requeue parent `--with-dependents` | job.failed |
| user cancel | no | — | CANCELLED (attempt unknown if dispatched) | — | no | job.cancelled |
| delivery failure (any sink) | sink retries | backoff | **job unchanged**; sink `failed` after bound | — | if final | delivery.failed / delivery.final |
| Galeed / logging failure | n/a | — | **job unchanged** | — | if sustained | (counter; WARNING) |
| ingress store outage | pause consumer | backoff | n/a | — | if prolonged | ingress.paused |
| ingress poison message | after budget | — | DLT | — | inspect DLT | ingress.dlt |
| `unknown` | at most once, rerunnable + definite only | backoff | FAILED `unknown` (`requeue_manual`) | — | yes | attempt.finished |

---

## Retry policy audit (§12 of the brief)

| Item | Today | Required |
|---|---|---|
| maximum attempts | `max_retries` (default 2), per claim, lost on restart | per epoch, persisted, survives restart (ROB-STATE-001) |
| base / max delay, exponential | 1 s × 2ⁿ, cap 10 s | kept, plus per-category budgets |
| jitter | 0 by default | > 0 by default |
| per-provider behaviour | none | via category mapping per adapter |
| Retry-After / reset times | ignored | honoured (`retry_at`) |
| elapsed-time budget | none | `max_elapsed` per category |
| cancellation during backoff | task cancel only (same process) | job is QUEUED during backoff, so cancel is a normal conditional write |
| shutdown during backoff | sleeping job dies, lease expires, rerun | nothing in flight |
| persistence across restart | none | `run_at` + attempt log |
| permanent errors | regex-dependent (E11) | category table (§1.1) |
| OOM / context / auth / quota / malformed | partly via regex | explicit rows in §H |
| retry storms | possible (no jitter) | jitter + per-category backoff |

## Circuit breaking (§13 of the brief): decision

The loop considered a per-backend breaker in iterations 0 and 5 and rejected it
both times. The only evidence (E20) shows a retry budget smaller than a daemon
restart. Per-category elapsed budgets fix that directly. A breaker that
*defers* queued jobs would also contradict INV-10.

Required instead: a per-backend health gauge (consecutive failures by category)
and Galeed events. A breaker MAY be added if load evidence later shows
retry traffic harming a recovering provider.

## Provider adapter contract (§29 of the brief)

This is the smallest coherent abstraction. It replaces ad-hoc provider logic in
`client.py`.

```python
class BaseAdapter:
    backend_name: str
    rerunnable: bool                      # ROB-AMBIG-001 default for this backend
    async def run(self, request, *, on_chunk=None, deadline=None) -> AdapterResult
    async def embed(self, request, *, deadline=None) -> AdapterResult
    async def abort(self) -> None         # kill group / close stream (exists today)
    def version(self) -> str | None       # CLI --version, server /api/version

class ProviderError(Exception):           # the only exception adapters raise for provider failures
    category: str                         # §1.1
    retryable: bool | None                # None = use category default
    provider_code: str | None             # e.g. "billing_error", "41", "turn.failed"
    http_status: int | None
    exit_code: int | None
    signal: str | None
    retry_after: float | None
    outcome_definite: bool                # False if the provider may have acted
    excerpt: str                          # sanitised, both streams, tail-preserving
```

`AdapterResult` carries output, usage and metadata. The adapter has already
validated it against its contract (ROB-BACKEND-001), so `client.py` never
inspects provider text. Health is the gauge in ROB-RETRY-001. There is no
`health_check()` method: liveness probes such as `/api/version` or `--version`
do not detect auth, quota or model problems, and real outcomes do.

## Compatibility (§31 of the brief)

These are preserved:
- the stub default
- SQLite as the default store, single file, stdlib only
- Mongo multi-worker operation
- the `submit()` / `JobResult` API
- the 5-value status enum
- `RetryPolicy` and its `retry_on` names (as aliases)
- lightweight deployment and WSL support

**No new runtime dependency is required.**

Intended behaviour changes:

| Change | From | To |
|---|---|---|
| `hoglah clear` with no filter | deleted everything | keeps non-terminal rows |
| Bulk requeue | requeued every FAILED row | skips ambiguous and poison rows |
| CLI jobs interrupted by a crash | silently rerun | now FAILED `outcome_unknown` |
| Exit-0-but-empty CLI runs | COMPLETED | FAILED `protocol_error` |
| Cross-process cancel latency | 0.25 s | ≤ the heartbeat interval |
| Direct callbacks on `start_worker=False` | silently never fired | now warn |

Each change needs a CHANGELOG entry (AGENTS.md rule 5).

---

## J. Prioritised implementation sequence

The order follows risk vectors and architectural dependency. Each step ships
with its ROB-TEST-001 tests, and every step leaves the system strictly safer
than before.

**Step 1: immediate P0 fixes. No schema change, small diffs.**
1. ROB-ADMIN-001, `clear` ordering only: confirm before deleting, and no default deletion of non-terminal rows. *(E25: an operator-visible data-loss bug.)*
2. ROB-CORE-002: conditional cancel. *(E02)*
3. ROB-CORE-001: remove the pre-commit read; post-commit errors never deliver FAILED; stream errors never fail jobs. *(E04, E16, E23)*
4. ROB-LEASE-001: heartbeat retries; cancel watch folded into the heartbeat. *(E03)*
5. ROB-SCHED-001, partial: per-row error isolation, fail-closed gates, tolerant SQLite decode with quarantine, poll paging. *(E01, E26)*
6. ROB-CLI-001, partial: `stdin=DEVNULL`, prompt via stdin/file, group SIGKILL, default timeouts for every adapter. *(E08, E09, E21)*
7. ROB-BACKEND-001, partial: incremental UTF-8 decode; exit-0-empty and Ollama `load`/empty become `protocol_error`; input-truncation flag. *(E07, E19)*
8. ROB-BRIDGE-001, partial: Kafka seek-back/pause, Rabbit reconnect + nack-requeue, Redis XAUTOCLAIM; submitter `correlation_id` + `SubmitOutcomeUnknown`. *(E18)*

**Step 2: P1 hardening, still with no schema change.**

9. ROB-LOOP-001: OpenAI adapter via `to_thread`; non-blocking Galeed emitter (with ROB-LOG-001 levels and counters); Mongo and Galeed timeouts; store-independent gauges. *(E05, E06)*
10. ROB-SHUT-001: signal handling and worker-thread liveness. *(E14)*
11. ROB-SEC-001: redaction, claude `--bare`, broker backend allow-list. *(E24)*
12. ROB-CONF-001: env formats and startup validation. *(E22)*

**Step 3: the state core, one schema migration on both stores.**

13. ROB-STATE-001 + ROB-RETRY-001 (fenced requeue) together, then ROB-STATE-003 guards and property tests. *(E12, E13; N58)*
14. ROB-AMBIG-001 + the rest of ROB-CLI-001 (pid/pgid/boot persistence, orphan reaping). *(E12, E14)*
15. ROB-LEASE-002: monotonic two-observation reclaim. *(E17)*

**Step 4: delivery and administration (schema: sink records).**

16. ROB-DELIV-001: sink records, periodic sweeper, Idempotency-Key, fsync; replaces startup redelivery and the startup-only outbox. *(E15)*
17. ROB-ADMIN-001 in full: store-level guards and the retention floor. ROB-BRIDGE-001's reply-route sinks.

**Step 5: P2 and the remaining parts.**

18. ROB-CLASS-001 in full + ROB-BACKEND-001 structured outputs (claude json / codex `--json` / grok json; gemini and kiro SHOULD) + version-drift detection.
19. ROB-RETRY-001 budgets, Retry-After, jitter defaults, health gauge.
20. ROB-BATCH-001: P0 compensation-scope fix earlier if `submit_batch` with idempotency keys is in use; Mongo sealing last.
21. ROB-LOG-001 full event set and optional spool.

**Why this order:**
- Step 1 removes every *reproduced* P0 behaviour that can be fixed locally.
- Step 3 must precede Step 4, because delivery ids and sink fencing need `attempt_no`.
- ROB-RETRY-001's fenced requeue must ship *with* the attempt log. Otherwise in-claim retries multiplied by counted reclaims allow (max_retries+1)² dispatches (N58).

**Unknowns that need further testing before or during implementation:**
- Reproduce the code-read modes N42–N58: submitter routing, Mongo seal races, Rabbit/Redis behaviour under live brokers.
- Install Gemini CLI and Kiro CLI and replace their doc-only contracts with recorded behaviour.
- Measure the cost of the two-observation reclaim window on recovery time with real workloads.
- Decide the defaults for `dedupe_window`, `sink_ttl` and per-backend timeouts.
