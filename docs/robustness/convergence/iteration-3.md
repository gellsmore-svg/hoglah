# Iteration 3

## Answer

### Code facts checked this pass (read-only, src/hoglah v0.13.0)

**Audit of synchronous calls on the worker loop (`client.py`)**

| Call | Where | Finding |
|---|---|---|
| poll `list(QUEUED, 10/50)` | `_worker_loop` 1125 | An exception aborts the whole poll and the loop logs "Error in worker loop" at ERROR, every 1s, with no rate limit. **`_row_to_dict` json.loads has no per-row guard, so one undecodable row in the window aborts every poll for good (C18).** |
| `_eval_depends_on` / `_depends_on_cycle` | 1462 / 1442 | Neither catches store exceptions, so an error in one row's dependency `get` aborts the *whole* poll (C19). Without that error, this path fails closed by accident, but the blast radius is the whole worker. The cycle walk makes O(depth) `get` calls per row per poll, every poll. |
| `_processing_snapshot` / `_dim_usage` | 1787 / 1827 | An error returns `[]` or counts 0 peers, so **model/session/tag slot limits fail OPEN** (C20). This is the same pattern as N23, but for admission gates. |
| `_preload_tick` → `_preload_demand` | 1597 | It runs as a task on the same loop and is re-spawned every 0.5s poll once the previous run finishes. Each tick repeats the whole readiness evaluation for up to `max(50, 4·lookahead)` rows, takes an *unlimited* PROCESSING snapshot, and runs `list(limit=500)`, all synchronously (C21). The adapters' `ps`, `preload` and `unload` calls are async and wrapped in `wait_for`; the HTTP adapter versions are no-ops. |
| `_cancel_watch_loop` | 1417 | It calls `store.get` every 0.25s for each in-flight job, which is 4·C sync reads per second. Only `CancelledError` is caught, so **one transient store error kills the watch silently** (the `finally` swallows it), and cross-process cancel stops working for that job (C22, the E03 class). A deleted row makes it return silently. |
| BackendPool / dispatch / affinity | dispatch.py, affinity.py | Pure in-memory work, no I/O. **Safe.** Minor issues: any exception, including caller errors, puts (model, backend) into a 30s cooldown; `_affinity_skips` only shrinks on claim. Neither is material. |
| metrics | metrics_server.py | Runs on its own thread, but `metrics_text()` = `render(self.stats())` calls `store.get_status_counts()`. **During a store outage the whole scrape fails, in-memory counters included** (C23). The SQLite `_lock` is shared with the loop. |
| SQLite `busy_timeout` | store.py 251 | 5000 ms per call. |

**Other findings**

- **C24 (`cli.py:499`, `client.py:2431`, `store.py:733`).** `hoglah clear` calls `h.clear()`, which **deletes first and asks "Delete N job(s)?" afterwards**. Answering "no" still deletes.
  - With no `--status`, `delete_jobs` deletes *all* rows, QUEUED and PROCESSING included.
  - `delete_jobs` bypasses `remove()`, so it also bypasses R9's guard against deleting a parent that live jobs depend on.
  - Deleted rows take their `idempotency_key` and `correlation_id` with them. The dedupe horizon is therefore the retention horizon.
  - Rows with `result_published=0` (the egress outbox) can be deleted before they are published.
- **C25 (`submit_batch` 764, `batch.py:108`).** `name_to_id` assigns new ids before any submit happens.
  - If an item carries an `idempotency_key` that matches an existing job, `submit()` returns the *existing* id. The batch result and its children's `depends_on` still point at the new id, which was never persisted, so those children are later blocked with "not found".
  - On a failure, compensation calls `remove(jid, cancel=True)` on every *returned* id. That **deletes pre-existing jobs the batch did not create**, including COMPLETED ones that other jobs depend on.
  - External `depends_on` ids pass through without an existence check, and a local name shadows an external id with the same string.
  - `batch_id` can be supplied by the caller and is not unique.

### Answers to iteration 2's four requests

**1. Loop audit.** BackendPool and affinity are clean. Four paths break R6's assumption that per-poll store work is bounded:
- the preload tick (C21)
- the cancel watch, at 4·C reads per second, which also fails silently (C22)
- the cycle walk (C19)
- poll-abort on a single row (C18, C19)

Fixes: the preload tick reuses the poll's readiness cache and snapshot. The cancel watch is folded into the heartbeat (pass 8). Cycles are checked at enqueue. Errors are isolated per row. R6 is otherwise sound: there is no remaining unbounded blocking call outside the store and Galeed, and those are bounded by timeouts.

**2. R26 attacks.**
- (a) **Reusing a `batch_id`.** If the seal flip is keyed by `batch_id`, a retried submission that reuses it releases the crashed attempt's orphan prefix *as well as* its own full set, so the prefix runs twice (N34). The fix is to seal by a per-submission `seal_token`.
- (b) **Idempotency inside a batch.** C25 shows that both the id mapping and compensation scope are wrong. Item keys are resolved before ids are assigned, and compensation touches only rows that carry this submission's `seal_token`.
- (c) **External dependencies.** These behave correctly under sealing: they wait, and a dead parent leads to a cascade. The pass-through gap is only a validation weakness. Validating at enqueue is SHOULD, and the poll stays authoritative.

**3. The R9/N27 guard vs `clear`.** The guard has a hole: `delete_jobs` bypasses it (C24). The guard moves into the store, as R27.

**4. Can fail-closed readiness deadlock silently?**
- **Today: a persistent store error is loud in logs but blind in metrics.** The poll aborts and logs at ERROR every second. Any store-derived stuck gauge cannot fire, and the metrics endpoint itself fails because of C23. A persistent *per-row* error (C18) is worse: a single row stalls every job in the worker.
- Resolution:
  - Readiness errors are isolated per row (wait).
  - An undecodable row is quarantined.
  - The stuck signal must come from **store-independent in-memory state**: `last_successful_poll_age`, `consecutive_store_errors` and loop lag. It is rendered even when `stats()` fails, and it drives a `degraded` health state.

### Pass findings

**Pass 2 (attack recovery mechanisms): material.**
- R26 is keyed wrongly (N34).
- R9's guard can be bypassed (C24).
- The cancel watch dies silently (N35).
- The "stuck gauge" in R9/R18 depends on the failing call (N38).

**Pass 3 (propagation): material.**
- **N33:** batch compensation deletes pre-existing jobs, which spreads to *their* dependents.
- **N32:** clearing deletes PROCESSING rows and parents, which spreads to live workers (heartbeat lost → interrupt, no terminal record) and to children (blocked).
- **N36:** a snapshot error leads to slot oversubscription, which can lead to GPU OOM, which then fails *other* jobs.

**Pass 4 (total death): material, one item.**
- Rows deleted by `clear` take the R1 attempt log with them. After a restart, nothing records that a dispatched job ever existed.
- Fix: deletion is allowed only for terminal rows whose sinks are settled.
- Everything else re-checked. Sealing by token makes a crashed submission identifiable, because its rows carry an unsealed `seal_token`.

**Pass 5 (silent failures): material.**
- N35 (the cancel watch dies).
- N36 (fairness fails open, logged only at ERROR with no job-level signal).
- N38 (metrics blind during an outage).
- C24 (a "cancelled" `clear` still deleted rows; the user is told "Clear cancelled.").

**Pass 6 (idempotency of every retry): material.**
- N33 and N34.
- N41: dedupe on `idempotency_key` and `correlation_id` lasts only as long as the row exists. A resubmission after `clear` creates a duplicate.
  - The window must be declared, and `clear` must keep a key tombstone or refuse to delete rows inside a configurable dedupe window.
  - Kafka is unaffected because committed offsets are not redelivered. Rabbit and Redis redeliveries after a long outage are affected.

**Pass 7 (Galeed): minor.**
- New events: `rows_deleted` (with counts and filter), `row_quarantined`, `worker_degraded`/`recovered`, and `batch_seal_conflict`.
- Galeed may record `job_completed` for a job whose row was later cleared. The store is authoritative only for retained rows, which is now stated.

**Pass 8 (simplification): material.**
- **The cancel watch is removed.** Under R8, cancel is a conditional write that clears the lease token, so the next fenced heartbeat already returns False and interrupts the job.
  - Cross-process cancel latency becomes ≤ the heartbeat interval. That is declared, and `heartbeat_interval` can be configured (default 10s; 2–3s is cheap on SQLite WAL).
  - Same-process cancel stays immediate through `_interrupt_local_job`.
  - This removes 4·C reads per second and N35 together.
- The preload tick reuses the poll's readiness cache and snapshot instead of evaluating again.
- The per-poll cycle walk is removed and replaced by an enqueue-time check plus R9's per-poll cache.

### Requirement set (vs iteration 2)

Job state is the 5-value `status`. Cause is `failure.category`. Action is `decision.action` ∈ {retry, fail, requeue_manual, none}. Ambiguity lives in `attempt.outcome`.

**R1 ROB-STATE-001 — UNCHANGED.**
- The attempt log is append-only. Each entry has `attempt_no`, phase, host and boot, outcome (including unknown and lease_lost), failure fields, `cli_version` and decision.
- `attempt_no` is incremented atomically with the claim.
- The policy is resolved and persisted at enqueue.
- Each requeue starts a new epoch, and `attempts_exhausted` is counted per epoch (E12, E13).

**R3 ROB-AMBIG-001 — UNCHANGED.**
- `dispatched` is committed before any side effect.
- A reclaim from dispatched or returned sets `outcome=unknown`. Only the rerunnable class auto-reruns.
- Guarded requeue, with `force` recorded.
- Bulk requeue skips unknown and unreapable jobs.
- **At-least-once for rerunnable jobs, at-most-once automatic dispatch otherwise. No exactly-once.**

**R4 ROB-LEASE-001 — CHANGED (the heartbeat is also the cancel detector).**
- The per-job loop heartbeat survives store errors and retries within the lease.
- If renewal fails before the monotonic expiry minus the margin, the worker aborts and records `lease_lost` or `unknown`.
- A False renewal is classified by a fenced read where possible: the row is CANCELLED, the row is gone, or the token moved. This replaces the cancel watch.
- A store outage longer than one lease may lose results. This is declared.

**R5 ROB-LEASE-002 — UNCHANGED.**
- A reclaim requires an unchanged `(lease_token, lease_expires_at)` over at least `lease + margin` of monotonic time.
- Wall-clock expiry (`$$NOW` on Mongo) is only a prefilter.
- No reclaim within the first window after startup.

**R6 ROB-LOOP-001 — CHANGED (audit result; metrics decoupled).**
- Every sync call on the loop is bounded below `(lease − margin)/k`. The cumulative store work per poll is bounded by R9, including the **preload tick**, which must reuse the poll's readiness cache and PROCESSING snapshot and must not run its own unlimited scan.
- There is no per-job periodic store polling apart from the heartbeat, so the cancel watch is removed (pass 8).
- Galeed emission is non-blocking. OpenAI calls go through `to_thread` or an async client. Store and Galeed timeouts and the SQLite `busy_timeout` fit the budget. A loop-lag watchdog is required.
- **Liveness gauges are in-memory and independent of the store:** `last_successful_poll_age_s`, `consecutive_store_errors` and `loop_lag_s`.
  - They are rendered even when `stats()` fails. The store gauges degrade to absent rather than failing the scrape (C23).
  - Worker-loop error logs are rate-limited.

**R7 ROB-CORE-001 — UNCHANGED.**
- There is no pre-commit `get`. The fenced commit is the only race guard (E23).
- Nothing after commit changes the outcome (E04).
- Stream errors never fail the job (E16).

**R8 ROB-CORE-002 — CHANGED.**
- Cancel is one conditional write `IN (QUEUED, PROCESSING)` that clears the token, and returns False if it lost (E02).
- A remote worker learns of the cancel through its next heartbeat (R4), so latency is ≤ the heartbeat interval. This is declared.
- A dispatched attempt that is cancelled gets `outcome=unknown`.
- The cascade is best-effort, and the poll is authoritative.

**R9 ROB-SCHED-001 — CHANGED (row isolation; all gates fail closed; independent stuck signal).**
- Readiness for `depends_on` and label groups is evaluated once per group or dependency set per poll and cached. The poll pages up to `max_scan`, and rows that are waiting never consume the window (E01).
- **Every admission gate fails closed**: readiness, slot and session/tag limits, and rate limits. If the PROCESSING snapshot cannot be read, nothing is admitted on any capped dimension that poll (C20, N36).
- **Errors are isolated per row**: an evaluation error affects only that row (`wait`) and does not abort the poll (C19). The store's `list` tolerates undecodable rows by returning `{id, decode_error}`. Such rows are quarantined to FAILED `corrupt_row` with a status-only conditional write and are never claimed (C18, N37).
- Cycles are rejected at enqueue when they are detectable. At poll time, cycle detection uses only the per-poll dependency cache.
- The stuck signal is `job_stuck` from successful scans **plus** R6's store-independent gauges. A persistent store error therefore surfaces as `degraded`, not silence (N38).
- A missing dependency is `blocked` only if it never existed. Deletion guards are covered by R27.

**R10 ROB-DELIV-001 — UNCHANGED.**
- Per-sink state is written with the terminal `set_result`, using `delivery_id = job:attempt:sink`.
- Sweepers run periodically and are fenced.
- Idempotency keys and message ids are sent. Files use temp, fsync, rename. Direct callables are registered before enqueue.
- Each sink gets bounded attempts, then `failed` with a final event.
- **At-least-once per (job, attempt, sink).**

**R12 ROB-BRIDGE-001 — UNCHANGED.**
- Ingress never skips (E18).
- A store outage pauses the consumer. A poison message goes to the DLT, then is acked.
- `correlation_id` provides idempotency *within the retention horizon* (see R27).

**R13 ROB-CLI-001 — UNCHANGED.**
- `stdin=DEVNULL` or a prompt file, and the prompt is never parsed as an option (gemini and kiro included).
- A file is used above the argv limit.
- Default timeouts. Group SIGTERM, then SIGKILL.
- pid, pgid, host and boot are persisted, and reaping happens only on the same boot.

**R14 ROB-CLI-002 — UNCHANGED.**
- Per-backend output and exit contracts (claude, codex, gemini 41/42/44/52/53, kiro 3 and a noise denylist (weak, as declared), grok).
- Exit 0 with an empty or noise-only payload is `protocol_error`.
- Incremental UTF-8 decoding. Both streams are excerpted.
- Version drift is detected.

**R15 ROB-CLASS-001 — UNCHANGED.**
- Structured signals first. Regex only on provider text (E11).
- `unknown` gets at most 1 retry.
- `store_unavailable`, `lease_lost` and `corrupt_row` are distinct categories.

**R16 ROB-RETRY-001 — UNCHANGED.**
- A retry is a fenced requeue with `run_at`.
- Per-category budgets: minutes for unavailable, Retry-After honoured, fail fast for auth, model and invalid errors.
- Jitter is > 0.
- `run_at` is a wall-clock not-before hint.

**R18 ROB-LOG-001 — CHANGED.**
- The previous event set is kept, plus `rows_deleted`, `row_quarantined`, `worker_degraded`/`recovered` and `batch_seal_conflict`.
- Bounded non-blocking emission. Failures are logged at WARNING with rate limiting. The spool is SHOULD.
- **The store is authoritative only for retained rows.** After a `clear`, Galeed is the sole history.

**R20 ROB-SHUT-001 — UNCHANGED.**
- SIGTERM and SIGINT stop claiming, drain within a bound, terminate CLI groups and close (E14).
- `hoglah run` exits non-zero if the worker thread dies.

**R21 ROB-OLLAMA-001 — UNCHANGED.**
Timeouts, truncation detection, empty or `load` responses not treated as success, auto-pull opt-in and outside the attempt (E19, E20).

**R22 ROB-SEC-001 — UNCHANGED.**
Redaction everywhere, claude with `--bare`, and a broker CLI allow-list.

**R23 ROB-CONF-001 — UNCHANGED.**
Env formats (E22) and startup validation of the R6 budget.

**R24 ROB-TEST-001 — CHANGED.**
- The previous tests are kept: E01–E23, (a)–(g), (i)–(s), and the revised (h).
- New tests:
  - (t) an undecodable row in the poll window, where other jobs must still run
  - (u) a dependency `get` that raises for one row, where other rows must still be claimed
  - (v) a PROCESSING snapshot error, where no slot oversubscription may happen
  - (w) a store down for longer than N polls, where the metrics scrape must still succeed and show `degraded`
  - (x) answering "no" to `hoglah clear`, where nothing may be deleted; and `clear` with no filter, where non-terminal rows survive
  - (y) a batch whose idempotent item matches an existing job, where compensation must not delete it and children must point at the existing id
  - (z) a retried `submit_batch` with the same `batch_id` after a crash, where no orphan prefix may be released
  - (aa) cross-process cancel latency of at most the heartbeat interval with no cancel-watch task

**R25 ROB-STATE-003 — CHANGED (deletion and quarantine are explicit transitions).**

| Transition | Cause |
|---|---|
| QUEUED→PROCESSING | claim (attempt+1, sealed) |
| PROCESSING→QUEUED | fenced retry, or reclaim under R5/R3 |
| PROCESSING→COMPLETED or FAILED | fenced commit |
| QUEUED or PROCESSING→CANCELLED | conditional cancel |
| QUEUED→FAILED | conditional direct write for `dependency_blocked`, `label_blocked` or `corrupt_row`, with a log marker |
| FAILED or CANCELLED→QUEUED | operator requeue under R3 (new epoch) |
| **terminal→deleted** | only under R27 |

- COMPLETED is absorbing.
- **QUEUED, PROCESSING and unsealed rows can be deleted only by an explicit `--force-nonterminal`**, which cancels them first (R8) and records `rows_deleted`.
- Defined races:
  - cancel vs retry: the first write wins
  - cancel vs claim: remote abort, outcome unknown
  - reclaim vs commit: decided by fencing
  - **delete vs claim: deletion is conditional on terminal status, so it loses to the claim**
- `cascade_dependents` covers `depends_on` and label groups, and terminates.

**R26 ROB-BATCH-001 — CHANGED (seal token; idempotency; compensation scope).**
- Every `submit_batch` call gets a fresh `seal_token`.
  - SQLite inserts all items in one transaction.
  - Mongo inserts items with `seal_token` unsealed and seals them with one `updateMany({seal_token})`. **Sealing is never keyed by `batch_id`** (N34).
  - Reusing a caller-supplied `batch_id` while an earlier submission is unsealed raises `batch_seal_conflict`, unless `resume=True` re-seals the earlier token after completing its missing items.
- Item `idempotency_key`s are resolved *before* ids are assigned, so an existing job maps into `name_to_id` and its dependents point at it (C25).
- **Compensation deletes or cancels only rows carrying this `seal_token`**, which are never dispatched. It never touches pre-existing jobs (N33).
- External `depends_on` ids are validated at enqueue (SHOULD: warn or raise on not-found). Local names may not shadow existing job ids.
- An unsealed token older than a threshold is reported and can be cancelled.
- **All-or-nothing with respect to dispatch, not storage.**

**R27 ROB-ADMIN-001 — ADDED (retention and deletion safety).**
- `delete_jobs` and `delete_job` enforce these guards in the store, not only in `remove()`. By default they skip:
  - non-terminal rows
  - unsealed rows
  - rows with any sink not in `delivered`/`failed`, including `result_published=0`
  - rows referenced by a non-terminal `depends_on` or label peer (N27, C24)
- `hoglah clear` counts with a dry-run, confirms, *then* deletes (C24).
- The `idempotency_key`/`correlation_id` dedupe window equals row retention. This is documented. `clear` SHOULD keep a key tombstone (id and key only) for a configurable `dedupe_window`, or refuse to delete rows younger than it (N41).
- Every deletion emits `rows_deleted`.
- Basis: C24, code-backed; not reproduced.

R2, R11, R17 and R19 remain merged, as in earlier iterations.

### New failure modes
| ID | Failure mode | Pass | Basis | Severity | Covered by |
|---|---|---|---|---|---|
| N32 | `hoglah clear` deletes before confirmation, and with no filter it deletes QUEUED and PROCESSING rows. Running jobs lose their lease, children are blocked, and attempt history disappears. | 3/4/5 | **code-backed (C24)** | high | R27, R25 |
| N33 | Batch compensation deletes pre-existing idempotent jobs, including COMPLETED parents of other jobs | 3/6 | code-backed (C25) | high | R26 |
| N34 | A seal flip keyed by `batch_id` releases a crashed submission's orphan prefix, so it runs twice | 2/6 | design attack on R26 + code (caller-supplied `batch_id`) | medium | R26 |
| N35 | The cancel watch dies silently on one store error, so cross-process cancel no longer interrupts that job | 2/5 | code-backed (C22) | medium | R4, R8 (watch removed) |
| N36 | A PROCESSING snapshot error makes the fairness slot gates fail open: oversubscription, possible GPU OOM | 3/5 | code-backed (C20) | medium | R9 |
| N37 | A single undecodable row, or a dependency `get` that raises, aborts every poll and stalls the whole worker | 2/5 | code-backed (C18, C19) | high | R9, R25 |
| N38 | The store-derived stuck gauge and the whole metrics scrape fail with the store, so monitoring is blind exactly when needed | 2/5 | code-backed (C23) | medium | R6, R9 |
| N39 | The preload tick repeats the readiness scan, an unlimited snapshot and `list(500)` on the loop every 0.5s | 2 | code-backed (C21) | medium | R6 |
| N40 | The per-poll cycle walk costs O(depth) store reads per row | 2 | code-backed (C19) | low | R9 |
| N41 | Dedupe on `idempotency_key`/`correlation_id` is lost after retention deletes the row | 6 | code-backed (C24) | medium | R27, R12 |

**Per pass:**

| Pass | Material? | Findings |
|---|---|---|
| 2 | yes | N34, N35, N37, N38, N39 |
| 3 | yes | N32, N33, N36 |
| 4 | yes | deletion of attempt history |
| 5 | yes | N35, N36, N38, the C24 confirmation bug |
| 6 | yes | N33, N34, N41 |
| 7 | minor | 4 events |
| 8 | yes | cancel watch, cycle walk and duplicate preload scan removed |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.93 | Every sync path on the loop is now audited. Admin and retention, batch idempotency and per-row poison were found and mapped. What remains unmapped is mixed-version workers (old worker, new request schema; FAILED per job, low) and Mongo `delete_jobs` parity, which I did not read. |
| restart_reconstructability | 0.87 | Deletion is now a guarded transition, so attempt history cannot vanish for non-terminal rows. A crashed batch submission is identifiable by its `seal_token`. Outage-period result loss is still declared. |
| retry_idempotency_safety | 0.84 | Batch compensation scope and seal keying are fixed, and the dedupe horizon is declared, with a tombstone as SHOULD. Rerunnable double-billing and early `run_at` under skew are still declared. |
| galeed_reconstructability | 0.74 | Deletion, quarantine and degraded events were added. Galeed is explicitly the only history after a `clear`, and it is still lossy. |
| guarantee_honesty | 0.91 | Cross-process cancel latency, the dedupe window and "store authoritative only for retained rows" are now declared. There is no exactly-once claim anywhere. |
| simplicity_compat | 0.77 | The cancel-watch task, the per-poll cycle walk and the duplicate preload scan were removed. One requirement (R27) and one field (`seal_token`, replacing `batch_sealed`) were added. The `clear` default changes behaviour (non-terminal rows are kept), which is a compatibility change but a safe one. |
| residual_high_risk | 0.18 | Remaining: all of this is unbuilt and N32–N41 are unreproduced. Per-row quarantine relies on a status-only write succeeding on a corrupt row. CLI contract drift is detected, not prevented. The gemini/kiro contracts are doc-only. |

## Changes From Previous
- **ADDED:** R27 (retention and deletion safety; N32, N41, and the N27 bypass).
- **CHANGED:**
  - R4 (the heartbeat detects cancel)
  - R6 (audit result, preload reuses the cache, no per-job polling, store-independent liveness gauges, metrics decoupled)
  - R8 (cancel latency declared, cancel watch removed)
  - R9 (row isolation, all admission gates fail closed, quarantine, enqueue-time cycle check, independent stuck signal)
  - R18 (events, retention honesty)
  - R24 (t–aa)
  - R25 (terminal→deleted, `corrupt_row`, delete-vs-claim race)
  - R26 (`seal_token`, idempotent items, compensation scope, `batch_id` reuse)
- **UNCHANGED:** R1, R3, R5, R7, R10, R12, R13, R14, R15, R16, R20, R21, R22, R23. R12's text gained only the retention qualifier and is otherwise unchanged.
- **MERGED (carried):** R2, R11, R17, R19.
- **DELETED:** none as IDs. Mechanisms removed: the cancel-watch polling task, the per-poll cycle walk and the preload duplicate scan. `batch_sealed` was replaced by `seal_token`.

## Uncertainty
- I did not read Mongo's `delete_jobs`/`delete_job` or the Mongo `list` decode path. I assume parity with SQLite, so C18 and C24 on Mongo are unverified.
- Quarantine of a corrupt row: whether a status-only conditional UPDATE works when `request_json` is corrupt holds for SQLite (no decode on write), but needs checking for Mongo (BSON is unlikely to be corrupt; the risk is mostly SQLite).
- Folding the cancel watch into the heartbeat raises cross-process cancel latency from about 0.25s to about 10s by default. Whether any caller depends on the sub-second cancel (for example the Mahlah UI) is unknown. A configurable short heartbeat mitigates it.
- That a "Clear cancelled." answer still deletes was read from code order, not reproduced. It is very likely, because `h.clear` runs before `typer.confirm`.
- N32–N41 are code-backed by reading only. None is reproduced.

## Stop-Relevant Notes
- **Not convergent.**
  - This pass found material new high-risk modes that are code-backed: N32 (the `clear` data loss and confirm-after-delete bug), N33 (batch compensation deletes pre-existing jobs) and N37 (a single poison row stalls the whole worker).
  - It added a material requirement (R27).
  - It revealed a new state-transition ambiguity: deleting non-terminal rows was unmodelled, and R25 now has terminal→deleted and the delete-vs-claim race.
- All four requested checks are done. In 3 of 4 the answer exposed a real gap: the loop audit found C20–C23, R26 was keyed wrongly, and the R9 guard was bypassed by `clear`.
- **Next pass should:**
  - read the `mongo_store` delete and decode paths to confirm Mongo parity
  - attack R27's tombstone and dedupe window, and R26's `resume=True`
  - check `cancel_batch`, `wait_batch` and `requeue_failed` against the new deletion and seal rules
  - sweep the remaining operator and web surfaces (`web.py`, `cli.py` requeue/remove/serve) for other destructive operations that bypass store guards. Those are the only unaudited surfaces left. If they are clean, iteration 4 can plausibly converge.
