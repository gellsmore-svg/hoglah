# Iteration 1

## Answer

### Code facts checked this pass (read-only, src/hoglah v0.13.0)
- **C5** (`models.py:382` `effective_retry_policy`): the retry policy is already persisted on the request (`retry_policy` or `max_retries`). `RetryPolicy()` code defaults apply only when neither is set. This mostly falsifies iteration-0's N7 ("a restart with a changed config changes in-flight limits"). Only the *code-default* case can drift, and only across a version upgrade.
- **C6**: `store.requeue` defaults to `from_statuses=(FAILED,)`. The client exposes only FAILED and CANCELLED. The store itself accepts any status, including COMPLETED. Requeue clears `result_json`, `error`, the lease and `result_published`, which erases the only failure evidence on the row.
- **C7**: Mongo `set_result`, `heartbeat` and `claim` are single-document conditional updates. `result_published` already lives *on the job document*. Mongo reclaim compares against the local wall-clock `_now_iso()`.
- **C8**: `_eval_depends_on` returns `blocked` as soon as a dependency is FAILED or CANCELLED. The poll then fails the child, via `_fail_blocked_dependency`, which claims and then writes FAILED. Cancel also cascades through `_fail_dependents`, so this logic is duplicated.
- **C9**: the in-claim retry loop sleeps with `asyncio.sleep` while it holds the lease and the slot. Nothing checks for cancellation between attempts apart from task cancellation of the local task.
- **C10**: SQLite runs one connection behind a `threading.Lock`, with WAL. Each SQLite statement holds the lock for milliseconds, so heartbeat starvation (E03, E05, E06) is caused by the *event loop*, not the store lock. N6 matters for Mongo, where a call can block for up to 30s.
- **E23**: confirms C2 and N10 empirically.

### Pass findings

**Pass 2 (attack recovery mechanisms): material.**
- R6, in its concrete form, uses a dedicated heartbeat thread. That thread can keep renewing leases after the event loop is wedged forever, so a job is never reclaimed. This is a new silent hold (N17) and needs a loop-liveness tick.
- R10's separate outbox table cannot be atomic with `set_result` on Mongo without multi-document transactions. Storing per-sink state on the job document fixes this, because single-document atomicity is enough.
- R10 deliveries racing an operator requeue could publish a stale attempt's result, so deliveries must be fenced by `attempt_no`.
- R12's DLT-to-store is circular during a store outage. If the DLT write fails, the message must not be acked.
- R17's breaker has no direct evidence. E20 only shows that the budget is too short.

**Pass 3 (propagation): material.**
- N16: a FAILED `outcome_unknown` parent (R3) makes C8 immediately fail all of its dependents. A later operator `requeue` of the parent leaves the children dead.
- A store outage longer than a lease makes R4 abort *all* in-flight work. That is correct fail-safe behaviour, but a result computed during the outage cannot be persisted (not even as `orphan_output`), so it is lost. This must be declared.
- The breaker must not count `store_unavailable` failures against a backend. That rule became moot once R17 was merged.

**Pass 4 (total death): material, mostly corrective.**
- N7 is withdrawn (C5).
- N15: a requeue of a CANCELLED or FAILED job whose last attempt is still physically running (a remote worker that has not yet seen the cancel, which takes up to one heartbeat of about 10s, or an orphaned CLI from E12/E14) starts a concurrent second execution. The fenced token stops *writes* but not side effects.
- The pseudo-claim in `_fail_blocked_dependency` would consume an `attempt_no` under R1. It must be recorded as `phase=claimed`, never dispatched.

**Pass 5 (silent failures): material.**
- N17 (above).
- N18: requeue wipes `error` and `result` (C6), so prior-attempt evidence is silently lost. R1's append-only attempts cover this.
- In-claim retry sleeps (C9) look like PROCESSING with heartbeats. They are not silent, but they hide that the job is waiting. R16 covers this.

**Pass 6 (idempotency of every retry): material, one item.**
- The same job can legitimately produce several results after an operator requeue, each with a different `delivery_id`. Consumers that dedupe by `job_id` alone will drop the newer result, and consumers that dedupe by `delivery_id` will see two. The guarantee must state this.
- The other paths were re-checked against current keys: ingress by `correlation_id`, deliveries by `job:attempt:sink`, events by `event_id`, and retries by the rerunnable class. No other gap was found.

**Pass 7 (Galeed): minor.**
- Add `loop_stalled` and `requeue` events, the latter carrying the prior attempt summary.
- No new gap remains once the attempt table is authoritative.

**Pass 8 (simplification): material.**
- R1: the policy snapshot already exists (C5). The new requirement is only "resolve defaults at enqueue and persist them".
- R10: the separate outbox table is replaced by per-sink state on the job row or document, generalising `result_published`.
- R6: "all store calls via executor" is replaced by "heartbeat thread plus non-blocking Galeed plus `to_thread` for OpenAI plus bounded Mongo timeouts".
- R17: the breaker is merged into R16 as per-category elapsed budgets, with auth, billing and model errors failing fast. Defer-without-attempt is removed. The R12-vs-R17 interaction disappears as a result.
- R20: the "mark dispatched" sentence is dropped as redundant with R3.

### Requirement set (job state = 5-value `status`; cause = `failure.category`; action = `decision.action` ∈ {retry, fail, requeue_manual, none}; ambiguity lives in `attempt.outcome`)

**R1 ROB-STATE-001 — CHANGED (snapshot reuses existing persisted policy; requeue history)**
- Each job has an append-only attempt log with these fields:
  - `attempt_no`
  - `phase` ∈ claimed|dispatched|returned|committed
  - `host_id`, `boot_id`
  - `started_at`/`ended_at`
  - `outcome` ∈ succeeded|failed|unknown|cancelled|lease_lost|in_progress
  - `failure{category, retryable, provider_code, exit_code, signal, http_status, exception_class, excerpt}`
  - `decision{action, delay, reason}`
- `attempt_no` is incremented atomically with the claim, and the lease token encodes it.
- The effective retry policy is resolved at enqueue and written to the request. This is an extension of C5, not a new mechanism.
- A reclaim from phase ≥ dispatched counts as an attempt. When `attempt_no > max_attempts`, the job goes to FAILED `attempts_exhausted` (E12, E13).
- A requeue appends an attempt-log marker and never deletes attempts (N18).

**R2 — MERGED (iteration 0, into R1).**

**R3 ROB-AMBIG-001 — CHANGED (requeue precondition)**
- `phase=dispatched` is committed before any side effect. If that commit fails, the worker does not dispatch and releases the claim.
- A reclaim from dispatched or returned sets `outcome=unknown`. The worker auto-reruns only the rerunnable class (stub, ollama and openai inference by default; CLI agents are not rerunnable). Otherwise the job goes to FAILED `outcome_unknown`, with `decision=requeue_manual` through the existing `requeue()`.
- `requeue()` MUST refuse while the last attempt is `in_progress`, or is `unknown` and not yet reaped (R13). It MAY proceed with an explicit `force=True` that is recorded in the attempt log (N15).
- **Guarantee: at-least-once dispatch for rerunnable jobs, at-most-once automatic dispatch otherwise. No exactly-once.**

**R4 ROB-LEASE-001 — CHANGED (store-outage loss declared)**
- Renewal retries within the remaining lease. Each store call is bounded below `lease − margin`.
- If renewal fails before the monotonic expiry minus the margin, the worker aborts provider work: SIGTERM, then SIGKILL to the CLI *group*. The attempt is recorded as `lease_lost`, or `unknown` if it was dispatched.
- A lost-lease completion (C1) is logged at WARNING. The output is kept as `orphan_output` *when the store is reachable*.
- Documented: during a store outage longer than one lease, completed results may be lost and are re-derived only by a rerun (rerunnable jobs) or by the operator.

**R5 ROB-LEASE-002 — UNCHANGED**
- A reclaim requires an unchanged `(lease_token, heartbeat_seq)` observed over at least `lease + margin` of the reclaimer's monotonic time (E17, WSL jumps).
- Mongo uses `$$NOW` for its hint arithmetic (C7).
- No reclaim happens until one observation window after startup.

**R6 ROB-LOOP-001 — CHANGED (concrete, narrower design)**
- (a) Lease renewal runs in one dedicated OS thread per worker, with its own store connection. SQLite WAL permits this. Each tick renews all owned leases in one batched conditional update and increments `heartbeat_seq`.
- (b) The thread renews only if the event loop's liveness tick (a timestamp a loop task updates every second) is fresher than `lease − margin`. Otherwise it stops renewing, logs `loop_stalled` at WARNING and lets R5 reclaim (N17).
- (c) Galeed emission is non-blocking (R18).
- (d) The OpenAI adapter uses `asyncio.to_thread` or an async client (E06).
- (e) Mongo store and Galeed clients have `serverSelectionTimeoutMS`/`socketTimeoutMS` set below the lease margin, validated at startup (N6).
- Remaining store calls stay on the loop.

**R7 ROB-CORE-001 — UNCHANGED (iteration-0 text; E23 now confirms it)**
- There is no pre-commit `get`. The fenced `set_result` is retried within the lease and is the only race guard (C2, E23).
- After True, nothing changes the outcome. Post-commit errors are delivery or observability failures (E04, C3).
- After False, there is no delivery and R4 applies.
- Stream errors disable streaming and never fail the job (E16).

**R8 ROB-CORE-002 — UNCHANGED**
- Cancel is one conditional write, `IN (QUEUED, PROCESSING)`, that clears the token. It returns False if it lost (E02).
- Cancelling a dispatched attempt gives `outcome=unknown`.

**R9 ROB-SCHED-001 — UNCHANGED**
- Waiting rows never fill the claim window: they are excluded in SQL or paged (E01).
- A stuck gauge and a `job_stuck` event cover jobs QUEUED-and-due beyond a threshold.

**R10 ROB-DELIV-001 — CHANGED (no new table; Mongo-atomic; attempt-fenced)**
- Per-sink delivery state (output_file, http_callback, named_callback, broker) is a field on the job row or document. It generalises `result_published` and is written in the *same single-row or single-document update* as the terminal `set_result`, so Mongo needs no transactions.
- `delivery_id = job_id:attempt_no:sink`.
- Sweepers claim with a delivery lease on the row, run periodically and only in worker instances, and fence every delivery on the current `attempt_no`. A stale attempt's result is never delivered after a requeue.
- HTTP sends an Idempotency-Key, and the broker sends a message id.
- File writes use a temp file, fsync of the file and directory, then rename.
- Direct callables are registered before the enqueue commit (E05).
- A named callback is never re-fired once delivered (E15).
- **Guarantee: at-least-once per (job, attempt, sink). A requeued job may legitimately deliver more than one result, so consumers dedupe by `delivery_id` and take the highest `attempt_no` as current.**

**R11 — MERGED (iteration 0, into R10).**

**R12 ROB-BRIDGE-001 — CHANGED (error split; DLT never acked unless durable)**
- Ingress never skips (E18):
  - Kafka: seek back to the failed offset.
  - Rabbit: nack with requeue, and reconnect.
  - Redis: periodic XAUTOCLAIM.
- Enqueue exceptions are split into two groups:
  - Store-unavailable errors (driver connection, timeout and lock classes) pause the consumer with backoff and are not counted against the message.
  - All others count toward a per-message poison budget. When it is exhausted the message goes to the DLT and is acked *only after* the DLT write is durable.
- The DLT write may go to the store as FAILED `ingress_poison`. If that write fails, the error is treated as store-unavailable.
- Ingress is idempotent through `correlation_id`.

**R13 ROB-CLI-001 — UNCHANGED**
- `stdin=DEVNULL`, or the prompt goes by stdin or a file (E09). Use `--` or a prompt file (E08). A file is used above the argv limit (E21).
- Per-backend default timeout. Group SIGTERM, then SIGKILL.
- Persist `pid`, `pgid`, `host_id`, `boot_id` and `start_time`. Reap only on a matching host and boot. Otherwise the attempt is marked `orphan_unreapable` (N8). This provides the "reaped" precondition that R3's requeue needs.

**R14 ROB-CLI-002 — UNCHANGED**
- Structured output modes with per-backend classifiers.
- Exit 0 with an empty or invalid payload is `protocol_error`.
- Excerpts come from both streams, are sanitised and keep the tail. Signals are named.
- Streaming uses an incremental UTF-8 decoder (E07, E08, E10).

**R15 ROB-CLASS-001 — UNCHANGED**
- Structured signals come first. Regex is used only on provider-originated text (E11).
- `unknown` allows at most 1 retry.
- `store_unavailable` and `lease_lost` are categories distinct from provider categories.

**R16 ROB-RETRY-001 — CHANGED (absorbs R17's essential part)**
- A retry is a fenced PROCESSING→QUEUED write with `run_at = next_attempt_at`, never an in-claim sleep (C9).
- Per-category defaults:
  - `provider_unavailable` and `network`: an elapsed budget sized to daemon restart time (minutes, not seconds; E20)
  - `rate_limit`: honours Retry-After or reset time
  - `auth`, `billing`, `model_not_found` and `invalid_request`: non-retryable, failing fast with a clear category (E10, E19)
- Jitter is > 0 by default.

**R17 ROB-HEALTH-001 — MERGED into R16**
- The breaker and deferral had no direct evidence beyond E20, which R16's elapsed budget covers.
- A per-backend health *gauge* (consecutive failures by category) is kept as a metric only.
- A breaker MAY be added later if load evidence warrants one.

**R18 ROB-LOG-001 — CHANGED (adds events)**
- The store is authoritative. Galeed events carry `(job_id, attempt_no, seq, event_id)` and cover:
  - attempt start/end
  - retry scheduled
  - lease reclaimed/lost/discarded
  - outcome_unknown
  - deliveries
  - ingress poison and pause
  - job_stuck
  - `loop_stalled`
  - `requeue` (with the prior attempt summary and the force flag)
  - startup recovery
- The emitter uses a bounded non-blocking queue. Drops are counted in a metric and on the next event.
- Failures are logged at WARNING with rate limiting.
- The spool is SHOULD, with replay deduped by `event_id`.

**R19 — MERGED (iteration 0, into R7).**

**R20 ROB-SHUT-001 — CHANGED (simplified)**
- SIGTERM and SIGINT stop claiming, drain within a bounded time, terminate CLI groups and close (E14).
- `hoglah run` exits non-zero if the worker thread *or* the R6 heartbeat thread dies.
- The redundant "mark dispatched" clause was dropped, because R3 already makes it true.

**R21 ROB-OLLAMA-001 — UNCHANGED**
- Default timeouts.
- `input_truncated` is detected from `prompt_eval_count`.
- An empty response, or `done_reason=load`, is not success.
- Auto-pull is opt-in and outside the attempt.
- A 5xx from bad options is retried at most once (E19, E20).

**R22 ROB-SEC-001 — UNCHANGED**
- Redaction covers results, files, callbacks, delivery payloads, excerpts, Galeed and `record_io`.
- `record_io` is opt-out when secrets are present.
- Claude runs with `--bare`. Broker-selected CLI backends go through an allow-list.

**R23 ROB-CONF-001 — CHANGED (minor)**
- Accept the documented env formats (E22).
- Startup validation covers the Mongo timeouts (R6) and a `lease_seconds` large enough relative to the heartbeat interval.

**R24 ROB-TEST-001 — CHANGED (adds scenarios)**
- E01–E23, plus iteration-0 items (a)–(g), plus:
  - (h) a wedged event loop with a live heartbeat thread, which must be reclaimed
  - (i) requeue while the prior attempt is still running, which must be refused
  - (j) requeue of an outcome_unknown parent that has dependents
  - (k) a delivery sweeper racing a requeue, where no stale attempt may be delivered
  - (l) a Mongo terminal write together with sink state under a kill between operations
  - (m) a DLT write failure during a store outage, where the message must not be acked
- Item (g) is changed to "upgrade with changed `RetryPolicy()` defaults".

**R25 ROB-STATE-003 — CHANGED (completed table)**
- Allowed job transitions (every write is conditional and fails closed):
  - QUEUED→PROCESSING: claim (attempt+1)
  - PROCESSING→QUEUED: retry (fenced by token) or reclaim (R5, and only if rerunnable or phase < dispatched)
  - PROCESSING→COMPLETED|FAILED: fenced commit
  - QUEUED|PROCESSING→CANCELLED: conditional cancel
  - QUEUED→FAILED: dependency_blocked, recorded as an attempt with `phase=claimed`, `outcome=failed`, and not counted toward max_attempts
  - FAILED|CANCELLED→QUEUED: operator requeue only, under the R3 precondition
- **COMPLETED is absorbing.** `store.requeue` MUST reject COMPLETED even when it is passed in `from_statuses`; a fresh job must be submitted instead (C6).
- Defined races:
  - cancel vs retry-requeue: whichever write lands first wins, and the loser's conditional write is a no-op. A retry that loses to cancel never dispatches.
  - cancel vs claim: if cancel wins after the claim, the remote worker sees a token mismatch at its next renewal and aborts, and the attempt becomes `unknown`.
  - reclaim vs commit: whichever lands first wins, by fencing.
- `requeue(cascade_dependents=True)` (default False) also requeues dependents that are FAILED with `category=dependency_blocked` and that point at this job (N16).
- The duplicated cancel cascade (C8) stays as a fast path. The poll-time evaluation is authoritative.

### New failure modes this iteration
| ID | Failure mode | Pass | Basis | Covered by |
|---|---|---|---|---|
| N15 | Requeue while the prior attempt is still physically running (remote cancel not yet seen, or an orphan CLI) causes concurrent duplicate side effects | 4 | code-backed (C6 + cancel docstring + E12/E14) | R3, R13, R25 |
| N16 | An outcome_unknown or FAILED parent fails all dependents, and requeuing the parent does not revive them | 3 | code-backed (C8) | R25 |
| N17 | A dedicated heartbeat thread keeps renewing leases while the event loop is wedged, so the job is held forever (introduced by the R6 fix) | 2 | design attack | R6(b), R20 |
| N18 | Requeue wipes error and result, so earlier failure evidence is lost | 5 | code-backed (C6) | R1 |
| N19 | A separate outbox table is non-atomic with the terminal write on Mongo, so a crash between the two loses or double-sends | 2 | code-backed (C7, no transactions used) | R10 |
| N20 | The delivery sweeper delivers a stale attempt's result after an operator requeue | 2 | theoretical | R10 |
| N21 | DLT-to-store fails during a store outage. If the message is acked anyway it is lost (the E18 shape) | 2/3 | theoretical (E18 mechanism) | R12 |
| N22 | A result computed during a store outage longer than one lease cannot be persisted as an orphan | 3 | theoretical | R4 (declared) |
| N7 | WITHDRAWN: the policy is already persisted per request (C5) | 4 | code-backed | — |

**Per pass:**

| Pass | Material? | Findings |
|---|---|---|
| 2 | yes | N17, N19, N20, N21 |
| 3 | yes | N16, N22 |
| 4 | yes | N15; N7 withdrawn |
| 5 | yes | N17, N18 |
| 6 | yes | multi-result-per-job guarantee wording |
| 7 | minor | 2 events |
| 8 | yes | 4 mechanisms removed or narrowed |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.87 | All of E01–E23 are mapped. Requeue, dependency and Mongo-atomicity modes have been added. Gemini and Kiro exit-code handling is still only under the generic R14. |
| restart_reconstructability | 0.82 | The transition table now has COMPLETED as absorbing, defined races and the requeue precondition. Policy persistence is verified in code. The lost-in-outage result is declared rather than solved. |
| retry_idempotency_safety | 0.79 | Deliveries are fenced by attempt, and requeue is refused while the prior attempt is live. Rerunnable inference can still double-bill, which is declared. `force` requeue is an explicit escape hatch. |
| galeed_reconstructability | 0.72 | The store is authoritative and the events are complete. Galeed is lossy by design, and drops are counted. |
| guarantee_honesty | 0.89 | Multi-result-per-job delivery, store-outage loss and the force-requeue risk are all stated explicitly. There is no exactly-once claim. |
| simplicity_compat | 0.71 | The outbox table, breaker and full executor are gone, and policy persistence and `result_published` are reused. Additions remain: attempt log, heartbeat thread, `heartbeat_seq`, SQL dependency filter. |
| residual_high_risk | 0.23 | Remaining: the R5 observation rule and the R6 heartbeat thread are unbuilt and untested. CLI classifiers depend on drifting upstream CLIs. The SQL dependency filter for label sequences is unproven. |

## Changes From Previous
- **CHANGED:**
  - R1 (reuses the persisted policy; requeue markers)
  - R3 (requeue precondition, force)
  - R4 (outage loss declared)
  - R6 (concrete heartbeat thread plus loop-liveness tick; executor dropped)
  - R10 (job-row sink state, Mongo-atomic, attempt-fenced; no table)
  - R12 (error split; DLT-durable-before-ack)
  - R16 (absorbs R17; fail-fast categories)
  - R18 (`loop_stalled` and requeue events)
  - R20 (heartbeat-thread liveness; redundant clause removed)
  - R23 (lease validation)
  - R24 (h–m)
  - R25 (complete table, COMPLETED absorbing, races, cascade requeue)
- **MERGED:** R17 into R16. The breaker was not evidence-justified, which also removes the R12/R17 interaction.
- **UNCHANGED:** R5, R7, R8, R9, R13, R14, R15, R21, R22.
- **Still merged from iteration 0:** R2, R11, R19.
- **ADDED:** none.
- **DELETED:** none, except that N7 is withdrawn as a failure mode.

## Uncertainty
- I did not verify whether `asyncio.wait_for` timeout cancellation kills the CLI process group or only the leader. The 2s leader-only SIGKILL fact suggests the leader only, and R13 covers it either way.
- Whether a second SQLite connection for heartbeats contends badly with the main connection's writes under `busy_timeout=5000` is unmeasured.
- The batched heartbeat (one UPDATE over many tokens) on Mongo needs `updateMany` with per-token `heartbeat_seq` increments. This is feasible but unchecked.
- N17, N19, N20, N21 and N22 are not reproduced.
- Demoting the breaker is a judgement call. Under heavy load against a dead backend, per-job persisted backoff produces N independent probes. This is cheap locally but noisy remotely.

## Stop-Relevant Notes
- **Not yet convergent.** Two code-backed modes were added (N15, N16), one a design-induced mode (N17), and R25 gained new transitions and preconditions. These are state-transition changes, which count against convergence. No new *requirement IDs* were needed, and the net mechanism count fell, so the set is tightening.
- **Next pass should attack:**
  - the R6 heartbeat thread under SQLite write contention and under Mongo `updateMany` partial failure
  - whether `requeue(force)` and `cascade_dependents` reintroduce unbounded loops
  - whether R9's SQL exclusion can express label-sequence waits, and if not, whether paging bounds the poll cost
  - one more check of whether R5's observation window and R6's liveness tick can be unified into a single mechanism (a likely simplification)
- I expect the next pass to add at most low or medium-severity items, and convergence is plausible at iteration 2.
