# Iteration 0

## Answer

### Extra code facts checked this pass (read-only, src/hoglah v0.13.0)
- C1 `client.py:1296`: the token-fenced `set_result` returns False when the lease was lost. The worker then logs at **INFO** only ("completed but lease was lost") and **discards the computed result**. No Galeed event, nothing persisted.
- C2 `client.py:1291`: a `store.get` race-guard runs *before* `set_result`. If that read raises, control falls into `except Exception`, and that path writes a fenced FAILED. The token is still valid at that point, so the write succeeds. **A provider success becomes FAILED "Unexpected worker error"**, and a non-idempotent side effect has already happened.
- C3 The `except Exception` path calls `_finish_stream`, `emit(JOB_FAILED)` and `_deliver(FAILED)` without checking `set_result`'s return value. This is the mechanism behind E04.
- C4 `SqliteJobStore` serialises every call behind one `threading.Lock` with `busy_timeout=5000`. `reclaim_stale_leases` compares ISO strings against the local `_now_iso()`. Hoglah already has `requeue(job_id, from_statuses)` (G9), `correlation_id`/`idempotency_key` UNIQUE indices, and `run_at` claim gating.

### Pass findings (summary)
- **Pass 2 (attack recovery mechanisms):** this pass was material.
  - R5 as worded does not stop a *lagging-owner* clock. The owner writes an expiry that is already in the past, and observers then see "expired" on both polls.
  - R3 depends on a `dispatched` marker write that can itself fail.
  - R10's periodic sweep runs in every instance, including `start_worker=False`, so two sweepers can double-deliver (the same shape as E15).
  - R12 "seek back" turns a poison message into an infinite partition stall.
  - R17 defers auth-failed jobs with no limit.
- **Pass 3 (propagation):** this pass was material. One store lock plus synchronous Mongo calls (30s server selection) means a single slow store call starves heartbeats for *all* in-flight jobs. The result is mass lease expiry and mass re-execution (E03 + E05 + C4 combined). Moving calls to an executor (R6) does not fix this alone.
- **Pass 4 (total death):** this pass was material.
  - Retry limits come from the RetryPolicy of whichever process is running now, so a restart with different config changes the limit for jobs already in flight.
  - Orphan reaping by pid is meaningless on Mongo stores shared across hosts, because no host identity is recorded.
  - SQLite durability: WAL with the default `synchronous` (FULL unless overridden) gives no new risk; nothing material found.
- **Pass 5 (silent failures):** this pass was material. C1 silently loses completed output, and C2 turns success into failure. Breaker deferral and ingress poison loops are also invisible unless they are counted.
- **Pass 6 (idempotency of every retry):** this pass was material.
  - The completion write itself is not retried. C2 shows a transient error at commit time converting a success into FAILED, after which a run that is re-runnable by default executes again.
  - Outbox delivery, HTTP callbacks, broker egress, Galeed spool replay and output-file rename each need their own idempotency key. The draft keyed only HTTP.
- **Pass 7 (Galeed reconstructability):** this pass was material.
  - The draft lets Galeed act as a second state source. The store MUST be authoritative, and Galeed carries copies of it keyed by (job_id, attempt_no, seq).
  - C1 events are missing entirely.
  - The secret-redaction scope must also cover `record_io`.
- **Pass 8 (simplification):** this pass was material.
  - R3's operator path should reuse the existing `requeue()` rather than a new mechanism.
  - The C2 race-guard read becomes redundant once R8 makes cancel conditional, and the fenced write is sufficient on its own.
  - R11 folds into R10 (the output file becomes a sink).
  - R19 folds into R7/R10 (the stream is diagnostic, so it is post-commit/non-outcome).
  - R2 merges into R1.
  - R18's local JSONL spool is reduced to an optional item, because the store-authoritative design makes it non-essential.

### Revised requirement set
Status enum stays at 5 values. Cause, action and state are kept as separate fields:
- **cause** = `failure.category`
- **action** = `decision.action` ∈ {retry, defer, fail, requeue_manual, none}
- **state** = `status`

The ambiguity is held in the attempt record (`outcome=unknown`), not in a new status.

**R1 ROB-STATE-001 — CHANGED (absorbs R2; adds the policy snapshot and fencing epoch)**
- Hoglah MUST persist an append-only attempt record per job: `attempt_no`, `phase` ∈ claimed|dispatched|returned|committed, `host_id`, `boot_id`, `started_at`/`ended_at`, `outcome` ∈ succeeded|failed|unknown|cancelled|lease_lost, a `failure{…}` as in Pass 1, and `decision{action, delay, reason}`.
- `attempt_no` MUST be incremented in the same atomic statement as the claim, and the lease token MUST encode `attempt_no` (fencing epoch).
- The job MUST store a `retry_policy` snapshot (max_attempts, max_elapsed, per-category overrides) at enqueue time. Limits are enforced from that snapshot across process lifetimes.
- A reclaim with phase ≥ dispatched counts as an attempt.
- When `attempt_no > max_attempts`, the job MUST go to FAILED with `category=attempts_exhausted`. This is the poison bound (E12, E13).

**R2 ROB-STATE-002 — MERGED into R1.** It duplicated R1's counter. The cross-lifetime enforcement now lives in the R1 snapshot.

**R3 ROB-AMBIG-001 — CHANGED (marker durability; reuses requeue)**
- The `phase=dispatched` write MUST commit *before* any provider side effect. If that write fails, the worker MUST NOT dispatch: it releases the claim back to QUEUED (`attempt_no` stays consumed, `outcome=failed`, `category=store_unavailable`).
- On reclaim with last phase dispatched or returned, the attempt is recorded as `outcome=unknown`.
- Automatic re-run is allowed only for the `rerunnable` class. The default is: stub, ollama and openai inference rerunnable; CLI agents not.
- Otherwise the job goes to FAILED `category=outcome_unknown`, `decision=requeue_manual`. The operator uses the existing `requeue(from_statuses=(FAILED,))`, and no new API is added.
- Documentation MUST state the guarantee: **at-least-once dispatch for rerunnable jobs and at-most-once automatic dispatch for others. No exactly-once.**

**R4 ROB-LEASE-001 — CHANGED (bounded store call; post-loss handling)**
- The heartbeat MUST survive transient store errors by retrying within the remaining lease. Each heartbeat store call MUST have a timeout shorter than `lease_seconds − margin`.
- If renewal is impossible before the expiry minus the margin (monotonic time), the worker MUST abort provider work: SIGTERM, then after a grace period SIGKILL to the *group*, for CLI jobs. It records the attempt as `outcome=lease_lost`, or `unknown` if the attempt had already been dispatched.
- A lost-lease completion (C1) MUST be logged at WARNING and emitted to Galeed as `lease_lost_result_discarded` with an output digest. The output SHOULD be kept on the attempt record (`orphan_output`) so it is not silently lost.

**R5 ROB-LEASE-002 — CHANGED (observation must be of liveness, not of a clock value)**
- Each heartbeat MUST increment a `heartbeat_seq`.
- A reclaimer MUST reclaim only when it has observed the same `(lease_token, heartbeat_seq)` unchanged across ≥ `lease_seconds + margin` of *its own monotonic time*.
- Wall-clock `lease_expires_at` remains as a hint only.
- The Mongo store SHOULD use `$$NOW` for its hint arithmetic.
- This rule defeats E17 and also the lagging-owner case (WSL).
- On startup, no reclaim may occur until the observation window has elapsed. This accepts a slower recovery time (RTO) of about one lease.

**R6 ROB-LOOP-001 — CHANGED (starvation across jobs)**
- No blocking I/O on the worker event loop: store and Galeed calls go through a bounded executor, and the OpenAI adapter uses `asyncio.to_thread` or an async client (E05, E06).
- Heartbeats MUST use a dedicated executor and connection that cannot be queued behind a slow claim, poll or Galeed call (C4).
- The Mongo store MUST set `serverSelectionTimeoutMS` and `socketTimeoutMS` below the lease margin. The defaults of 30s/none are rejected at config validation.

**R7 ROB-CORE-001 — CHANGED (commit retry; removes redundant pre-read; absorbs R19)**
- The completion `set_result` MUST be retried on transient store errors within the remaining lease, and MUST NOT be preceded by a separate status read (C2). The fenced conditional write is the only race guard.
- Once the write returns True, nothing may change the outcome. Later exceptions in metrics, Galeed, delivery, stream finalisation or callbacks are logged as `delivery`/`observability` failures, and FAILED is never delivered (E04, C3).
- If the write returns False, R4's lost-lease handling applies and *no* delivery occurs.
- Stream-file errors (open or write) MUST disable streaming for that job and MUST NOT fail it (E16).

**R8 ROB-CORE-002 — UNCHANGED.**
- Cancel MUST be one conditional write (`status IN (QUEUED, PROCESSING)`, setting CANCELLED and clearing the token). It returns False if it lost the race (E02).
- Cancelling a PROCESSING job whose attempt was dispatched records that attempt as `outcome=unknown` (side effects may already exist).

**R9 ROB-SCHED-001 — UNCHANGED.**
- Waiting rows MUST NOT occupy the claim window. They are excluded in SQL (dependency resolution moves into the query), or the poll pages until it finds claimable rows (E01).
- A job that has been QUEUED and due for longer than a threshold MUST appear in a stuck gauge and a Galeed `job_stuck` event.

**R10 ROB-DELIV-001 — CHANGED (absorbs R11; adds a sweeper lease and per-sink keys)**
- Hoglah MUST keep a durable delivery outbox with one row per (job, sink). Sinks are output_file, http_callback, named_callback and broker.
- Each row carries `state` ∈ pending|delivering|delivered|failed, `attempts`, `next_at`, and a `delivery_id = job_id:attempt_no:sink`.
- Sweepers MUST claim rows with a lease, the same pattern as jobs, so concurrent instances do not double-deliver (E15).
- Sweeps run periodically in the worker, not only at startup. Instances with `start_worker=False` MUST NOT sweep by default.
- Outbox rows are inserted in the same transaction as the terminal `set_result`.
- The HTTP POST carries `Idempotency-Key: delivery_id`. Broker messages carry it as the message id or a header.
- Output files use a unique temp file, then fsync of the file and directory, then rename (idempotent overwrite).
- Direct callables MUST be registered before the enqueue commit (E05).
- Named callbacks MUST NOT be redelivered once marked delivered. This replaces the startup re-fire of 200 rows.
- **Guarantee stated: at-least-once delivery; consumers dedupe by delivery_id.**

**R11 ROB-DELIV-002 — MERGED into R10.** The output file is one sink, and the atomic-write rule moved there.

**R12 ROB-BRIDGE-001 — CHANGED (poison bound)**
- Ingress MUST NOT skip messages (E18):
  - Kafka: seek back to the failed offset and do not commit past it.
  - Rabbit: `nack(requeue=True)` and reconnect the consumer.
  - Redis: periodic XAUTOCLAIM.
- Each message retry is bounded by attempts or elapsed time. After that the message goes to a dead-letter topic or queue (which may be the store itself, as a FAILED job with `category=ingress_poison`) and is then acked.
- *Store-unavailable* errors MUST pause the consumer with backoff rather than count against a message's poison budget.
- Ingress stays idempotent through the existing `correlation_id` UNIQUE index.

**R13 ROB-CLI-001 — CHANGED (adds host_id; argv limit)**
- `stdin=DEVNULL`, or the prompt is passed via stdin or a file (E09).
- The prompt is never parsed as an option: use `--`, a prompt file, or stdin (E08).
- Long prompts MUST use the file or stdin path, so there is no ARG_MAX failure (E21).
- A per-backend default timeout is required.
- On cancel, the process *group* gets SIGTERM, then SIGKILL.
- The worker persists `pid`, `pgid`, `host_id`, `boot_id` and `start_time`. Reaping happens only when `host_id` and `boot_id` match and the start_time check passes (this guards against pid reuse). Otherwise the job is marked `orphan_unreapable` in the attempt record.

**R14 ROB-CLI-002 — UNCHANGED.**
- Use structured output modes with per-backend classifiers.
- Exit 0 with an empty or invalid payload is a `protocol_error`, not success (E07, E08).
- Error excerpts come from both streams, are sanitised and keep the tail. Signals are named.
- Streaming uses an incremental UTF-8 decoder (E07, E10).

**R15 ROB-CLASS-001 — UNCHANGED.**
- One canonical cause taxonomy, classified from structured signals first.
- Regex is used only on provider-originated error text (E11).
- `unknown` gets conservative, bounded handling: at most 1 retry.

**R16 ROB-RETRY-001 — CHANGED (clarifies source of truth)**
- Retries MUST be persisted as QUEUED with `run_at = next_attempt_at` and a new attempt, never as an in-claim sleep. This frees slots and survives restarts.
- Defaults are set per category. `max_elapsed`, Retry-After and reset times are honoured.
- Jitter is > 0 by default.
- Limits come from the R1 snapshot.
- `provider_unavailable`/`network` failures SHOULD use a budget sized to daemon restart times (E20), and preferably go through R17 rather than burning attempts.

**R17 ROB-HEALTH-001 — CHANGED (bounded deferral; key scope)**
- A per-(backend, host) circuit breaker opens on consecutive provider_unavailable or network failures.
- Auth or billing failures open it in a *latched* state that needs an operator reset or a config change.
- Deferred jobs consume no attempt, but each MUST have a maximum deferral time. After that it goes to FAILED `category=backend_unavailable` (no infinite QUEUED).
- Half-open state allows a single probe job.
- Breaker state and the number of deferred jobs are exported.

**R18 ROB-LOG-001 — CHANGED (store-authoritative; events keyed; spool optional)**
- The store is the system of record for state. Galeed is a derived trace.
- Every event carries `(job_id, attempt_no, seq, event_id)` and covers:
  - attempt start and end (phase, category, decision)
  - retry scheduled
  - lease reclaimed, lost, or result discarded
  - outcome_unknown
  - deliveries
  - breaker transitions
  - ingress poison
  - job_stuck
  - startup recovery summary
- The emitter is non-blocking with a bounded queue, and drops are counted in a metric *and* on the next emitted event.
- Galeed failures are logged at WARNING with rate limiting.
- The local JSONL spool is SHOULD, not MUST. Replay MUST dedupe by `event_id`.
- Reconstructability is satisfied when any job's history can be rebuilt from the attempt table alone, and Galeed adds timing and IO detail.

**R19 ROB-STREAM-001 — MERGED into R7.** It is the same non-outcome rule.

**R20 ROB-SHUT-001 — UNCHANGED.**
- On SIGTERM or SIGINT: stop claiming, drain within a bounded time, terminate CLI groups, then close (E14).
- `hoglah run` monitors worker-thread liveness and exits non-zero if the thread dies.
- In-flight jobs abandoned at shutdown are marked at phase `dispatched`, so recovery treats them as unknown and does not assume they are clean.

**R21 ROB-OLLAMA-001 — UNCHANGED.**
- Default connect and read-idle timeouts.
- Truncation is detected via `prompt_eval_count` against the `num_ctx` category (`input_truncated`).
- An empty response, or `done_reason=load`, is not success.
- Auto-pull is opt-in and not part of the job attempt.
- A 5xx caused by bad options MUST NOT be retried more than once (E19, E20).

**R22 ROB-SEC-001 — CHANGED (scope includes record_io)**
- Secrets are redacted from results, files, callbacks, outbox payloads, stderr excerpts, Galeed events *and `record_io`* (which stores full prompts and outputs).
- `record_io` SHOULD be opt-in per job when metadata contains secrets.
- CLI workdir trust (`--bare` for claude) applies.
- CLI backends that a broker selects go through an allow-list.

**R23 ROB-CONF-001 — UNCHANGED.**
- Accept the documented env formats, or correct the docs (E22).
- Validate config at startup, including the Mongo timeouts from R6.

**R24 ROB-TEST-001 — CHANGED (adds new scenarios)**
- Deterministic fault-injection tests for E01–E22, plus:
  - (a) a store error in the completion pre-read or commit
  - (b) the dispatched-marker write failing
  - (c) a lagging-owner clock
  - (d) two outbox sweepers
  - (e) a Kafka poison message
  - (f) a slow Mongo call starving heartbeats
  - (g) a restart with a changed retry config
- Restart tests with a real kill, and state-machine property tests on attempt, phase and status. A key invariant is "no delivery of FAILED for a row in committed state".

**R25 ROB-STATE-003 — ADDED (state-transition table).** Without this table the attempt and phase states stay ambiguous.
- Hoglah MUST publish and test one transition table over (status, last attempt phase or outcome, decision.action).
- Allowed job transitions:
  - QUEUED→PROCESSING (claim)
  - PROCESSING→QUEUED (retry or reclaim, if rerunnable)
  - PROCESSING→COMPLETED|FAILED (fenced commit)
  - QUEUED|PROCESSING→CANCELLED (conditional)
  - FAILED|CANCELLED→QUEUED (operator `requeue` only)
- Every other write MUST be conditional and fail closed.

### New failure modes found this iteration

| ID | Failure mode | Pass | Basis | Covered by |
|---|---|---|---|---|
| N1 | Lagging-owner clock writes an already-expired lease; a two-poll expiry check still reclaims a live job | 2 | theoretical, grounded in the WSL doc and E17 | R5 |
| N2 | `dispatched` marker write fails; dispatch proceeds; recovery treats the run as never dispatched and re-runs a CLI agent | 2 | theoretical | R3 |
| N3 | Periodic outbox sweep in several instances (including `start_worker=False`) double-delivers | 2 | evidence-adjacent (E15) | R10 |
| N4 | Kafka seek-back plus a poison message stalls the partition forever | 2 | theoretical (E18 mechanism) | R12 |
| N5 | Breaker deferral after auth or billing failure leaves jobs QUEUED forever | 2 | theoretical (E10) | R17 |
| N6 | A slow store call (single lock, Mongo 30s selection) starves *all* heartbeats, causing mass reclaim and mass double execution | 3 | code-backed (C4 + facts) + E03/E05 | R6, R4 |
| N7 | Retry limit is taken from the currently running process config, so a restart with different config changes the bound for in-flight jobs | 4 | code-backed (in-memory RetryPolicy) | R1 |
| N8 | Orphan pid reap on a shared Mongo store across hosts kills an unrelated process or reaps nothing | 4 | theoretical | R13 |
| N9 | Lost-lease completion discards a computed result, with INFO only and no Galeed event | 5 | **code-backed (C1)** | R4, R18 |
| N10 | Transient store error in the pre-commit `get` turns a provider success into FAILED (and after a requeue, a second side effect) | 5/6 | **code-backed (C2)** | R7 |
| N11 | The FAILED path delivers and emits without checking whether its fenced write succeeded | 5 | **code-backed (C3)**, E04 | R7 |
| N12 | Galeed spool replay or broker egress duplicates without event or delivery ids | 6/7 | theoretical | R10, R18 |
| N13 | `record_io` stores full prompts and outputs including secrets, outside the redaction scope | 7 | code-backed (fact list) | R22 |
| N14 | Galeed treated as a parallel state source makes reconstruction ambiguous when the two diverge | 7 | theoretical | R18 |

Pass 8 removed or merged R2, R11 and R19. It also downgraded the spool to SHOULD and dropped a new operator-requeue API in favour of the existing `requeue()`.

## Score Vector
| name | meaning | polarity | value | rationale |
|---|---|---|---|---|
| failure_mode_coverage | Share of credible failure modes (E01–E22 + code-derived) mapped to a requirement | higher_better | 0.82 | All of E01–E22 are mapped, and 14 new modes are added. Gemini/Kiro-specific exit-code handling and multi-host Mongo are only lightly covered. |
| restart_reconstructability | Can correct state and next action be derived from the store alone after total process death | higher_better | 0.78 | The attempt table, policy snapshot, phase-before-dispatch rule and transition table make this possible. Within-attempt partial output and orphan outputs are not fully specified. |
| retry_idempotency_safety | Every retry or redelivery path either avoids duplicate side effects or declares at-least-once with a dedupe key | higher_better | 0.75 | Keys exist for deliveries, ingress and events. CLI re-runs are blocked by default. Rerunnable inference can still double-bill, which is accepted and declared. |
| galeed_reconstructability | A job's full history can be rebuilt from Galeed and store with no silent gaps | higher_better | 0.70 | Events are keyed and drops counted. Galeed is still lossy by design, which is acceptable because the store is authoritative. |
| guarantee_honesty | Ambiguity represented explicitly; no exactly-once claims | higher_better | 0.85 | `outcome=unknown` and `lease_lost` are explicit, and at-least-once and at-most-once are stated per class. |
| simplicity_compat | Fits SQLite/stub default, low deps, 5-value enum, existing API | higher_better | 0.62 | It adds an attempt table, outbox table, breaker and heartbeat_seq, and moves dependency checks into SQL. Merges and reuse of `requeue()` partly offset this. The surface is still substantial. |
| residual_high_risk | Unmitigated or under-specified high-severity risk remaining | lower_better | 0.30 | Remaining risk: N6-class executor and lock design is not yet concrete, the Kafka DLT on a store outage is untested, and the CLI per-backend classifiers depend on drifting upstream CLIs. |

## Changes From Previous
No previous iteration. The baseline is the Pass-1 draft. Against it:
- **CHANGED:** R1, R3, R4, R5, R6, R7, R10, R12, R13, R16, R17, R18, R22, R24
- **MERGED:** R2 into R1, R11 into R10, R19 into R7
- **ADDED:** R25
- **UNCHANGED:** R8, R9, R14, R15, R20, R21, R23
- The largest substantive changes are:
  - R5 (liveness observation, not a clock comparison)
  - R7 (commit retry, removing the pre-read, from C2)
  - R10 (sweeper lease)
  - R25 (explicit transition table)

## Uncertainty
- I verified C1–C4 by reading `client.py` and `store.py`. I did not check the Mongo store's equivalent fencing, or whether `_execute_with_retries` checks cancellation between retries.
- I did not check whether SQLite `synchronous` is overridden elsewhere. I assumed it is the default.
- N1, N2, N4, N5, N8, N12 and N14 are theoretical, not reproduced.
- The R6 claim that heartbeats need a dedicated connection is a design inference. A single SQLite connection behind one lock may make a "dedicated path" impossible without a second connection, which SQLite WAL permits.
- R9's dependency resolution in SQL may be hard for label-sequence waits. Paging is the fallback.

## Stop-Relevant Notes
- This pass was **not** convergent. It added material high-risk modes (N6, N9, N10 are code-backed) and one new requirement (R25), and it resolved state-transition ambiguity, which convergence counts as change.
- Next pass should attack:
  - R25 for completeness, especially the cancel versus retry-requeue race and the requeue of a COMPLETED job
  - the concrete design of the R6 heartbeat isolation
  - the R10 outbox transaction on Mongo, which has no multi-document transaction by default
  - the R12 store-outage pause interacting with R17
- New high-risk findings are expected to drop sharply after one more pass.
