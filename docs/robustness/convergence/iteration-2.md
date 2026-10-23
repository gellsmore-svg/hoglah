# Iteration 2

## Answer

### Code facts checked this pass (read-only, src/hoglah v0.13.0)
- **C11** (`client.py:1530` `_eval_label_sequence`): when the store `list` call fails, the method catches `except Exception`, logs, and returns `"ready"`. **A store error therefore lets a later label run before an earlier one finishes (fail-open).**
  - It lists the *entire* label group (`limit=None`) once for every QUEUED row in the poll window.
  - Ordering is computed from a per-request `label_sequence` JSON. Plain SQL cannot express it without a materialised rank.
- **C12** (`labels.py:44`): an earlier label that is FAILED or CANCELLED returns `blocked`, and the poll then fails the later rows. This is the same pattern as C8, but **for label groups**. Iteration 1's `cascade_dependents` only covers `depends_on`.
- **C13** (`client.py:764` `submit_batch`): items are persisted one at a time, in topological order. If one fails, compensation is `remove(cancel=True)` on the items already enqueued, running *in-process*.
  - A crash mid-loop leaves a committed prefix of the batch.
  - A worker can already have claimed and dispatched prefix items, so compensation cancels a dispatched job.
  - `remove` deletes rows. `_eval_depends_on` treats a missing dependency as `blocked`, so deleting even a COMPLETED parent fails its waiting children.
- **C14** (`client.py:971` `_fail_dependents`): the cancel cascade is recursive, not transactional, and does not cover label groups. A store error part-way through leaves some children cascaded and others not.
- **C15** (`cli.py:588`): `hoglah requeue --all-failed --limit N` bulk-requeues FAILED jobs, and `allow_cancelled` adds CANCELLED ones. Nothing distinguishes a job that failed because its outcome was unknown.
- **C16** (`client.py:1393`): the heartbeat is a *per-job asyncio task on the worker loop*. Iteration 1's batched-thread design would have replaced this.
- **C17** (`cli_adapters.py`): how each CLI receives the prompt.
  - gemini passes the prompt as `-p <text>` with `--output-format text`.
  - kiro uses `chat --no-interactive` with the prompt as the last positional argument and no structured output mode.
  - No code maps gemini exit codes 41/42/44/52/53 or kiro exit code 3. Every non-zero exit is treated the same.
  - Nothing records the CLI version.

### Pass findings

**Pass 2 (attack recovery mechanisms): material. The main result is a simplification.**
- **R6's heartbeat thread.**
  - Under SQLite WAL, a second connection waits for the writer lock for up to `busy_timeout`, and the batched heartbeat on Mongo needs `updateMany`, which can partly fail. Both problems exist only because of the thread, and so does N17.
  - All the evidence of loop stalls (E03, E05, E06) traces to *specific* blocking calls: the dead heartbeat task, synchronous Galeed, synchronous urllib, and 30s Mongo server selection. Once each is bounded, a heartbeat on the loop is itself a correct liveness signal: a wedged loop stops renewing, and R5 reclaims.
  - **The thread is dropped.** The per-job heartbeat task (C16) stays, and R4 hardens it. R6 becomes a rule bounding blocking per call and per poll, plus a loop-lag watchdog.
  - With the thread gone, N17, the SQLite-contention question and the `updateMany` question all disappear.
- **R5 and R6 unification (the iteration-1 question).** The liveness tick is no longer needed, because the heartbeat sits on the loop. R5's monotonic observation window is still needed for clock skew (E17). Its `heartbeat_seq` is redundant: every heartbeat rewrites `lease_expires_at`, so an unchanged `(lease_token, lease_expires_at)` pair seen over the window is enough evidence that no heartbeat happened. **`heartbeat_seq` is dropped.**
- **`requeue(force)` and `cascade_dependents` loops.**
  - Cascade terminates. Every requeue is conditional on the job being FAILED, so each job is requeued at most once per call. A dependency cycle is re-blocked at the next poll, not looped.
  - Automatic loops are impossible because only operators requeue. However, `--all-failed` (C15) is *bulk operator automation*. A cron'd bulk requeue of an `outcome_unknown` CLI job re-runs a non-rerunnable agent on every cycle. This is new (N25).
- **R9.** SQL cannot express label rank directly (C11). Paging alone does not bound cost, because each row costs a full group listing.
  - Fix: evaluate readiness *per group once per poll*. One query per distinct `label_group` and `depends_on` set in the window gives a frontier rank and dependency statuses, which are cached for that poll.
  - Paging continues until `claim_window` ready rows are found or `max_scan` is reached. If `max_scan` is reached, a `job_stuck`/`scan_exhausted` metric is emitted.
- **R10 sweeper.** The iteration-1 text had no bound on delivery retries. A permanent ENOSPC or an unwritable output dir (E16 class) would make the sweeper retry forever. Bounded attempts are now required, then sink state `failed`.

**Pass 3 (propagation): material.**
- **N23 (C11).** A transient store error during label evaluation fails open: later labels dispatch before earlier ones complete. The ordering contract breaks, and so do the side effects that depend on it. The fix is that every readiness evaluator fails *closed*: on error the result is `wait`, never `ready`.
- **N24 (C12).** A FAILED or `outcome_unknown` earlier label permanently fails all later labels. This is the N16 pattern for label groups, so `cascade_dependents` must cover label groups too.
- **N26 (C13).** A partial batch after a crash or failed compensation leaves a prefix running whose children were never persisted. Compensation that cancels dispatched prefix jobs produces `outcome=unknown`.
- **N27 (C13).** `remove()` of a COMPLETED parent fails its children with "not found". This is cross-component deletion propagation.

**Pass 4 (total death): material, one item.**
- With N26, there is no durable trace that a batch was meant to have K items.
- Fix: the batch is persisted atomically where the store allows it (one SQLite transaction). On Mongo, items are inserted with `batch_sealed=false` and the flag is flipped by one `updateMany` after the last insert.
  - The claim query excludes unsealed batch rows. An unsealed batch older than a threshold is reported, and the operator can cancel it.
  - The flip is idempotent, and a partly applied flip only releases complete items.
- Everything else re-checked: attempt log, sink state on the row, and reclaim through R5. No other gap was found.

**Pass 5 (silent failures): material.**
- N23 is logged at ERROR but has no job-level trace, so it is effectively silent at the ordering level.
- The C17 problems:
  - kiro 2.25.0 can print a "Trust All Tools active" footer instead of a reply with exit 0, which today is COMPLETED.
  - gemini can print 429 text during retries that succeed (#17906).
  - kiro `--no-interactive` without `--trust-all-tools` silently denies tool calls (#11594).
  - All three are E07/E08-class false successes, and none has a structured guard for gemini or kiro.
- **CLI version drift** is silent: a CLI upgrade changes output or exit contracts with no signal.

**Pass 6 (idempotency of every retry): material, one item.**
- N25: bulk requeue must apply R3's precondition per job and *exclude* `outcome_unknown` and `orphan_unreapable` jobs unless the caller passes an explicit flag. The per-job `force` is not implied by `--all-failed`.
- Batch sealing is idempotent.
- Cancel-cascade retries are idempotent because they are conditional writes.
- `run_at` retries on wall clock: a peer whose clock is fast claims a backoff early. Backoff is shortened but there is no duplication, so this is declared, not fixed.

**Pass 7 (Galeed): minor.**
- Add events: `readiness_eval_error`, `batch_sealed`/`batch_unsealed_stale`, `delivery_failed_final` and `cli_version_drift`.
- No structural gap.

**Pass 8 (simplification): material.**
Removed:
- the R6 heartbeat thread, liveness tick and batched heartbeat
- R5's `heartbeat_seq`
- the R25 pseudo-claim for `dependency_blocked`, which becomes a direct conditional `QUEUED→FAILED` write with an attempt-log *marker*, not an attempt, so there is no "not counted" special case

Evidence check:
- R22's allow-list and `--bare` are doc-backed, so they stay.
- The R18 spool is already SHOULD.
- Nothing else in the set lacks a basis in evidence or code.

### Requirement set
Job state is the 5-value `status`. Cause is `failure.category`. Action is `decision.action` ∈ {retry, fail, requeue_manual, none}. Ambiguity lives in `attempt.outcome`.

**R1 ROB-STATE-001 — CHANGED (requeue epoch).**
- Each job has an append-only attempt log with these fields:
  - `attempt_no`
  - `phase` ∈ claimed|dispatched|returned|committed
  - `host_id`, `boot_id`
  - `started_at`, `ended_at`
  - `outcome` ∈ succeeded|failed|unknown|cancelled|lease_lost|in_progress
  - `failure{category, retryable, provider_code, exit_code, signal, http_status, exception_class, excerpt, cli_version}`
  - `decision{action, delay, reason}`
- `attempt_no` is incremented atomically with the claim, and the token encodes it.
- The effective policy is resolved at enqueue and persisted (C5).
- A reclaim from phase ≥ dispatched counts as an attempt.
- When the attempts *in the current requeue epoch* exceed `max_attempts`, the job goes to FAILED `attempts_exhausted` (E12, E13).
- A requeue appends a marker that starts a new epoch and never deletes anything (N18).

**R3 ROB-AMBIG-001 — CHANGED (bulk requeue).**
- `phase=dispatched` is committed before any side effect. If that commit fails, the worker does not dispatch.
- A reclaim from dispatched or returned sets `outcome=unknown`. The worker auto-reruns only the rerunnable class: stub, ollama and openai inference. CLI agents are not rerunnable. Otherwise the job goes to FAILED `outcome_unknown` with `decision=requeue_manual`.
- `requeue()` refuses while the last attempt is `in_progress`, or is unknown and not yet reaped. Explicit `force` is recorded (N15).
- Bulk requeue (`--all-failed`, `requeue_failed`) skips `outcome_unknown` and `orphan_unreapable` jobs unless they are explicitly included, and reports the skipped ids (N25).
- **Guarantee: at-least-once dispatch for rerunnable jobs, at-most-once automatic dispatch otherwise. No exactly-once.**

**R4 ROB-LEASE-001 — CHANGED (applies to the loop heartbeat task).**
- The per-job heartbeat task (C16) survives store errors and retries within the remaining lease (E03). Each store call is bounded below `lease − margin`.
- If renewal fails before the monotonic expiry minus the margin, the worker aborts provider work (group SIGTERM, then SIGKILL) and records `lease_lost`, or `unknown` if dispatched.
- A lost-lease completion is logged at WARNING. The output becomes `orphan_output` when the store is reachable.
- A store outage longer than one lease may lose results. This is declared.

**R5 ROB-LEASE-002 — CHANGED (`heartbeat_seq` dropped).**
- A reclaim requires an unchanged `(lease_token, lease_expires_at)` observed over at least `lease + margin` of the reclaimer's monotonic time (E17, WSL).
- Wall-clock expiry is only a prefilter. Mongo uses `$$NOW` for that prefilter.
- No reclaim happens until one window after startup.

**R6 ROB-LOOP-001 — CHANGED (thread removed; blocking budget).**
- Liveness is the loop heartbeat itself. A wedged loop stops renewing, and R5 reclaims.
- Every synchronous call on the loop has a bound below `(lease − margin)/k`. The cumulative store work per poll is bounded by R9.
- Specific measures:
  - Galeed emission is non-blocking (R18), for E05.
  - OpenAI uses `asyncio.to_thread` or an async client (E06).
  - Mongo store and Galeed `serverSelectionTimeoutMS`/`socketTimeoutMS` are set below the budget and validated at startup (R23).
  - SQLite `busy_timeout` is set below the budget.
- A loop-lag watchdog logs `loop_stalled` at WARNING and emits a Galeed event when lag exceeds `lease/3`.

**R7 ROB-CORE-001 — UNCHANGED.**
- There is no pre-commit `get`. The fenced `set_result` is retried within the lease and is the only race guard (C2, E23).
- After True, nothing changes the outcome (E04, C3).
- After False, there is no delivery and R4 applies.
- Stream errors disable the stream and never fail the job (E16).

**R8 ROB-CORE-002 — CHANGED (cascade robustness).**
- Cancel is one conditional write, `IN (QUEUED, PROCESSING)`, that clears the token. It returns False if it lost (E02).
- A dispatched attempt that is cancelled gets `outcome=unknown`.
- The cascade (C14) is an iterative best-effort fast path. The poll-time readiness evaluation is authoritative, so a cascade that is interrupted converges at the next poll.

**R9 ROB-SCHED-001 — CHANGED (label groups; fail-closed; cost bound).**
- Readiness for `depends_on` *and label groups* is evaluated once per distinct group or dependency set per poll, and cached for that poll.
- The poll pages until `claim_window` ready rows are found or `max_scan` is reached. Rows that are waiting never consume the window (E01, and the label-group equivalent).
- **Any evaluation error yields `wait`, never `ready`** (N23), and emits `readiness_eval_error`.
- A stuck gauge and `job_stuck` are emitted for jobs that are QUEUED and due past a threshold, or when the scan is exhausted.
- A missing dependency is `blocked` only if it never existed. Deletion of a COMPLETED job that live dependents point to is refused by `remove()` unless cascade is requested (N27).

**R10 ROB-DELIV-001 — CHANGED (bounded sink retries).**
- Per-sink state is kept on the job row or document and written in the same single-document update as the terminal `set_result`.
- `delivery_id = job:attempt:sink`.
- Sweepers run periodically under a delivery lease and are fenced on `attempt_no`.
- HTTP sends an Idempotency-Key, and the broker sends a message id.
- Files use temp, fsync, rename.
- Direct callables are registered before enqueue (E05). Named callbacks are never re-fired once delivered (E15).
- Each sink gets bounded attempts with backoff. After that it is set to `failed`, with a `delivery_failed_final` event and a metric (E16 class: ENOSPC, unwritable dir).
- **Guarantee: at-least-once per (job, attempt, sink). Consumers dedupe on `delivery_id` and treat the highest `attempt_no` as current.**

**R12 ROB-BRIDGE-001 — UNCHANGED.**
- Ingress never skips (E18): Kafka seeks back, Rabbit nacks with requeue and reconnects, Redis runs periodic XAUTOCLAIM.
- A store-unavailable error pauses the consumer. Other errors count toward a poison budget, after which the message goes to the DLT and is acked only once that is durable.
- `correlation_id` makes ingress idempotent.

**R13 ROB-CLI-001 — CHANGED (gemini and kiro argv).**
- `stdin=DEVNULL` or a prompt file (E09). The prompt must never be parsed as an option (E08): use `--`, a prompt file, or stdin. This applies to gemini's `-p <text>` and kiro's trailing positional argument too.
- A file is used above the argv limit (E21).
- Per-backend default timeout, which also bounds the gemini quota hang (#10925).
- Group SIGTERM, then SIGKILL.
- Persist `pid`, `pgid`, `host_id`, `boot_id` and start time. Reap only on the same host and boot. Otherwise the attempt is `orphan_unreapable`.

**R14 ROB-CLI-002 — CHANGED (per-backend exit contracts; version drift).**
- Each backend declares an output contract:
  - claude: json/stream-json
  - codex: `--json`
  - gemini: json plus exit codes 41→auth, 42→invalid_request, 44→sandbox (config), 52→config, 53→turn_limit, all non-retryable
  - kiro: exit 3→mcp_config; with no structured mode, a known-noise denylist ("Trust All Tools active" footer, tool-denied text) means `protocol_error`
  - grok: json
- Exit 0 with an empty, invalid or noise-only payload is `protocol_error`.
- Excerpts come from both streams, are sanitised and keep the tail. Signals are named. Decoding is incremental UTF-8 (E07, E08, E10).
- At worker startup, each CLI's `--version` is recorded and compared with the tested range. Drift logs WARNING and emits `cli_version_drift`, but does not block.
- `cli_version` goes on the attempt.

**R15 ROB-CLASS-001 — UNCHANGED.**
- Structured signals come first. Regex is used only on provider-originated text (E11).
- `unknown` allows at most 1 retry.
- `store_unavailable` and `lease_lost` are distinct categories.

**R16 ROB-RETRY-001 — CHANGED (`run_at` clock honesty).**
- A retry is a fenced `PROCESSING→QUEUED` write with `run_at`, never an in-claim sleep (C9).
- Per-category elapsed budgets:
  - provider_unavailable and network: minutes (E20)
  - rate_limit: honours Retry-After
  - auth, billing, model_not_found and invalid_request: fail fast (E10, E19)
- Jitter is > 0.
- `run_at` (scheduled and retry) is a wall-clock *not-before hint*. Under skew or WSL jumps, a job may run early or late by the skew. This is documented, and Mongo compares against `$$NOW`.

**R18 ROB-LOG-001 — CHANGED (events added).**
- The store is authoritative. Events carry `(job_id, attempt_no, seq, event_id)`.
- The iteration-1 set is kept, plus `readiness_eval_error`, `batch_sealed`/`batch_unsealed_stale`, `delivery_failed_final` and `cli_version_drift`.
- Emission goes through a bounded non-blocking queue. Drops are counted. Failures are logged at WARNING with rate limiting. The spool is SHOULD.

**R20 ROB-SHUT-001 — CHANGED.**
- SIGTERM and SIGINT stop claiming, drain within a bound, terminate CLI groups and close (E14).
- `hoglah run` exits non-zero if the worker thread dies. The heartbeat-thread clause is removed along with the thread.

**R21 ROB-OLLAMA-001 — UNCHANGED.**
- Default timeouts.
- `input_truncated` is detected from `prompt_eval_count`.
- An empty response, or `done_reason=load`, is not success.
- Auto-pull is opt-in and outside the attempt.
- A 5xx caused by bad options is retried at most once (E19, E20).

**R22 ROB-SEC-001 — UNCHANGED.**
- Redaction covers all sinks, excerpts and `record_io`.
- Claude runs with `--bare`.
- Broker-selected CLI backends go through an allow-list.

**R23 ROB-CONF-001 — CHANGED.**
- Accept the documented env formats (E22).
- Startup validation checks that the store and Galeed timeouts and the SQLite `busy_timeout` fit within the R6 budget, and that the lease is large enough relative to the heartbeat interval.

**R24 ROB-TEST-001 — CHANGED.**
- E01–E23, (a)–(f), (g) as revised, and (i)–(m). Iteration-1 test (h) is replaced: a wedged loop must cause a reclaim *without* a thread.
- New tests:
  - (n) a store error during label evaluation, where no out-of-order dispatch may happen
  - (o) a label-group head-of-line block equivalent to E01
  - (p) a kill mid-`submit_batch`, where no unsealed item may be claimed
  - (q) bulk requeue that must skip `outcome_unknown` jobs
  - (r) a gemini or kiro fake that exits with the documented codes and footer noise
  - (s) a sink with permanent ENOSPC, where retries must be bounded

**R25 ROB-STATE-003 — CHANGED.**
Allowed transitions (all writes are conditional and fail closed):

| Transition | Cause |
|---|---|
| QUEUED→PROCESSING | claim (attempt+1, batch sealed) |
| PROCESSING→QUEUED | retry (fenced) or reclaim (R5; only if rerunnable or phase < dispatched) |
| PROCESSING→COMPLETED or FAILED | fenced commit |
| QUEUED or PROCESSING→CANCELLED | conditional cancel |
| QUEUED→FAILED | `dependency_blocked`/`label_blocked`: a direct conditional write plus an attempt-log marker, with no claim |
| FAILED or CANCELLED→QUEUED | operator requeue under R3, which opens a new epoch |

- COMPLETED is absorbing: `store.requeue` rejects it (C6).
- Defined races:
  - cancel vs retry: the first write wins
  - cancel vs claim: the remote worker aborts and the attempt is unknown
  - reclaim vs commit: decided by fencing
- `requeue(cascade_dependents=True)` requeues FAILED `dependency_blocked` *and `label_blocked`* rows that point at this job or its label group. It terminates because each requeue is conditional on FAILED (N16, N24).

**R26 ROB-BATCH-001 — ADDED.**
- A batch becomes claimable only once it is complete. On SQLite, all items go in one transaction (chunked to stay within R6's busy budget). On Mongo, items carry `batch_sealed=false` until one final flip.
- Compensation for a failed submit cancels only unsealed items, which have never been dispatched.
- An unsealed batch older than a threshold is reported and can be cancelled (N26).
- **Guarantee: a batch is all-or-nothing *with respect to dispatch*, not storage.**

R2, R11, R17 and R19 remain merged, as in earlier iterations.

### Unmapped SOURCE areas checked
| Area | Disposition |
|---|---|
| Gemini/Kiro exit-code contracts | R14 |
| CLI version drift | R14, R18 |
| Redis PEL and Rabbit reconnect | R12 (unchanged; already specific) |
| Egress startup-only sweep | R10 |
| Output-file and stream dir | R7, R10 |
| Cancel cascade | R8, R25 |
| Partial batch | R26 |
| `run_at` clock | R16 |
| Unknown-error policy | R15 (≤1 retry) |
| Ollama overload 503 / memory queueing | R16, R21 |

Nothing in SOURCE is left unmapped.

### New failure modes
| ID | Failure mode | Pass | Basis | Covered by |
|---|---|---|---|---|
| N23 | A store error during label evaluation fails open, so later labels dispatch out of order | 3/5 | **code-backed (C11)** | R9 |
| N24 | A FAILED earlier label fails all later labels permanently, and requeue does not revive them | 3 | code-backed (C12) | R25 |
| N25 | Bulk `requeue --all-failed` re-runs outcome_unknown CLI agents, and a cron'd version repeats this every cycle | 2/6 | code-backed (C15) | R3 |
| N26 | A crash or failed compensation in `submit_batch` leaves a running prefix with missing children, and compensation cancels dispatched work | 3/4 | code-backed (C13) | R26 |
| N27 | `remove()` of a COMPLETED parent fails its live dependents with "not found" | 3 | code-backed (C13 + `_eval_depends_on`) | R9 |
| N28 | Gemini/kiro false success: footer or noise with exit 0, denied tools, 429 text; exit codes unmapped | 5 | doc-backed (#11594, cao#832, #17906) + C17 | R14 |
| N29 | A CLI upgrade silently changes the output or exit contract | 5 | doc-backed (E08 versions, cao#832) | R14 |
| N30 | Delivery sweeper retries a permanently failing sink forever | 2 | theoretical (E16 mechanism) | R10 |
| N31 | Label-group evaluation costs O(window × group) synchronous reads per poll and stalls the loop | 2 | code-backed (C11) | R9, R6 |
| N17 | WITHDRAWN: the thread is removed and the heartbeat stays on the loop | 2 | design | — |

**Per pass:**

| Pass | Material? | Findings |
|---|---|---|
| 2 | yes | thread removed, N25, N30, N31 |
| 3 | yes | N23, N24, N26, N27 |
| 4 | yes | R26 batch sealing |
| 5 | yes | N28, N29 |
| 6 | yes | N25 |
| 7 | minor | 4 events |
| 8 | yes | thread, `heartbeat_seq` and pseudo-claim removed |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.91 | Every SOURCE area is now mapped, including gemini/kiro exit contracts, version drift, batch partiality, label groups and the `run_at` clock. The remaining unmapped items are only speculative interactions. |
| restart_reconstructability | 0.85 | Batch sealing and requeue epochs close the last durable-intent gap. Fail-closed readiness removes the ordering ambiguity. Outage-period result loss is still declared rather than solved. |
| retry_idempotency_safety | 0.82 | Bulk requeue is guarded, sink retries are bounded, and cascade termination is argued. Rerunnable inference can still double-bill (declared), and early `run_at` under skew is declared. |
| galeed_reconstructability | 0.73 | Events are complete for the new paths. Galeed is still lossy by design, and the store remains authoritative. |
| guarantee_honesty | 0.90 | A batch guarantee scoped to dispatch and a `run_at` not-before-hint statement were added. There is no exactly-once claim anywhere. |
| simplicity_compat | 0.76 | The heartbeat thread, liveness tick, `heartbeat_seq` and pseudo-claim are gone, and the existing per-job heartbeat is kept. One field (`batch_sealed`) and one requirement (R26) were added. |
| residual_high_risk | 0.20 | Remaining: the R5 window is unbuilt; the R6 budget relies on an audit of sync calls on the loop that was not done exhaustively (preload/pool paths unchecked); CLI contracts drift (now detected, not prevented); and the gemini/kiro contracts are doc-only because those CLIs are not installed. |

## Changes From Previous
- **ADDED:** R26 (batch sealing; N26, code-backed).
- **CHANGED:**
  - R1 (requeue epoch)
  - R3 (bulk requeue guard)
  - R4 (loop heartbeat task)
  - R5 (`heartbeat_seq` dropped)
  - R6 (thread replaced by blocking budget and watchdog)
  - R8 (cascade best-effort, poll authoritative)
  - R9 (label groups, fail-closed, per-group evaluation, `max_scan`, `remove` guard)
  - R10 (bounded sink retries)
  - R13 (gemini/kiro argv)
  - R14 (exit contracts, version drift)
  - R16 (`run_at` honesty)
  - R18 (events)
  - R20 (thread clause removed)
  - R23 (budget validation)
  - R24 (n–s; h revised)
  - R25 (`label_blocked`, direct write, cascade covers labels)
- **UNCHANGED:** R7, R12, R15, R21, R22.
- **MERGED (carried):** R2, R11, R17, R19.
- **DELETED:** none as IDs. Mechanisms removed: the heartbeat thread, the loop-liveness tick, `heartbeat_seq` and the dependency pseudo-claim. N17 is withdrawn.

## Uncertainty
- I did not audit every synchronous call on the worker loop: the preload/pool `warm()`, affinity and metrics-server paths are unchecked. R6's budget claim assumes no other long blocking call exists. If one does, the loop-heartbeat design is wrong for that path, and the fix is a narrow to_thread for it, not the thread.
- Whether gemini's yargs would parse `-p --version` as an option value or a flag is unverified (gemini is not installed).
- The kiro footer denylist is brittle by nature. Without a structured output mode, kiro results can only be weakly validated, and the requirement should say so.
- The Mongo `batch_sealed` flip via `updateMany` can partly apply. Partial release of a complete batch's items is safe, but I have not proved that ordering is preserved for children released before their parents. Dependency waits still hold them, so this is probably fine.
- N23–N31 are code- or doc-backed but not reproduced.

## Stop-Relevant Notes
- **Not convergent.** One requirement was added (R26), and two code-backed medium-to-high modes were found (N23 fail-open ordering, N26 partial batch). R25 gained a `label_blocked` transition, which is a state-transition change. Most of the remaining changes were simplifications (net mechanisms went down again), and coverage of SOURCE areas is now complete.
- **Next pass should:**
  - audit the remaining synchronous calls on the worker loop (preload, pool, affinity, `_processing_snapshot`) against R6's budget
  - attack R26's interaction with `depends_on` that points at jobs outside the batch, and with `idempotency_key` re-submission of a half-sealed batch
  - check whether the `remove()` guard (R9/N27) conflicts with retention or clear commands (`hoglah clear`)
  - verify that fail-closed readiness cannot itself deadlock under a persistent store error. It should surface as `job_stuck`; confirm that the gauge does not depend on the same failing call.
- I expect at most low-severity additions next. Convergence is plausible at iteration 3.
