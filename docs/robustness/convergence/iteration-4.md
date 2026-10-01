# Iteration 4

## Answer

### Requested checks (read-only, src/hoglah v0.13.0)

**1. Mongo parity for C18 and C24 (`mongo_store.py`)**

- **C24 is confirmed on Mongo.**
  - `delete_jobs` builds `{status?, updated_at<before?}` and calls `delete_many`. With no filter that is `delete_many({})`, which deletes every row, including QUEUED and PROCESSING ones and rows whose egress is unpublished.
  - `delete_job` is an unconditional `delete_one({_id})`.
  - Neither checks dependents, sinks or seal state. The store-level guard required by R27 is therefore needed on both backends.
- **C18 is refuted on Mongo as a poll-stall.**
  - `_doc_to_dict` reads native BSON sub-documents and builds `request_json` with `json.dumps(default=str)`. No decode step can raise, so one bad document cannot abort `list()`.
  - The remaining Mongo hazard is a schema-invalid `request`, for example from a newer-version producer (mixed versions) or a manual edit. That hazard is **per job**: `JobRequest(**row["request"])` (client.py:1260) runs after the claim, and the generic `except` turns it into FAILED "Unexpected worker error".
  - A non-dict `request` would still raise `AttributeError` in the poll's `(row.get("request") or {}).get(...)`. That needs a manual edit, so it is low.
  - **So C18 (a poll-wide stall) is SQLite-only.** On SQLite, `_redeliver_restart_callbacks` → `list(limit=200)` also runs in the constructor whenever named callbacks are configured. A corrupt row in the first 200 therefore stops `Hoglah()` from constructing at all. R9's tolerant `list` already covers this.
  - The iteration-3 worry about whether a status-only write can quarantine a corrupt Mongo row is moot: Mongo has no undecodable rows.

**2. Attacks on R27 (tombstone and dedupe window) and R26 (`resume=True`)**

- **The tombstone fails semantically.** A dedupe hit on a tombstone returns an id whose row no longer exists:
  - `get()` and `wait()` raise `KeyError` for a submission the caller was told succeeded.
  - A bridge acks it, and no reply is ever re-published.
  - It also needs a second keyed table with its own retention.
  - **Pass 8 replaces it:** `clear` refuses to delete rows that carry a dedupe key and are younger than `dedupe_window`, unless `--force`. No new storage is needed and the semantics are honest: inside the window, dedupe returns a live row.
  - What remains is declared: a Rabbit or Redis redelivery after the window creates a duplicate.
- **`resume=True` fails three ways.**
  - Two concurrent resumes of one token both insert the "missing" items, so the batch is duplicated. Preventing that needs a UNIQUE `(seal_token, item_name)`.
  - A resume with a different item list mixes two batches.
  - It needs the item names persisted per row.
  - It buys nothing: unsealed rows were never dispatched, so abandoning them and resubmitting is equivalent. **`resume` is deleted.**
- **New race found while attacking abandonment (N44, a state-transition ambiguity).**
  - On Mongo, sealing is `updateMany({seal_token})`. If an operator or stale-token cleanup cancels or deletes a *slow but live* submission's rows while that submission's seal is in flight, the seal releases whatever rows remain. The batch is then partially dispatched, which breaks "all-or-nothing w.r.t. dispatch".
  - Fix: seal and abandon become a **compare-and-set on one per-token header document** (`open→sealed` | `open→abandoned`). The poll admits rows only for tokens in a per-poll cached set of sealed tokens, alongside R9's readiness cache.
  - SQLite needs no header: its single transaction means rows are never visible unsealed.

**3. `cancel_batch`, `wait_batch` and `requeue_failed` against the deletion and seal rules**

- **`cancel_batch`** (client.py:825) lists by `batch_id` with no limit and cancels each row.
  - `batch_id` is caller-supplied and not unique, so it spans every submission that shares the id. This is declared as grouping semantics, not a bug.
  - Cancelling unsealed rows produces CANCELLED rows that are unsealed. Then `requeue --allow-cancelled` produces a QUEUED row that is never claimable, so it **sits forever, silently** (N45, low).
  - Fix: a requeue refuses rows whose token is not sealed, and abandoning a token deletes its rows instead of cancelling them.
- **`wait_batch`** (809) lists once, then calls `wait()` per row with a wall-clock deadline and no default timeout.
  - On Mongo, a list taken mid-insert sees a partial set.
  - An abandoned token's rows block forever if they are left in place, or raise `KeyError` if they are deleted.
  - Fix: `wait` and `wait_batch` resolve an abandoned token to an explicit error, and `wait_batch` resolves membership through the token header.
- **`requeue` and `requeue_failed`** (2327/2368; store 676 and mongo 374).
  - Both are conditional on status, and both clear the result, lease and `result_published`. Correct.
  - The bulk path, however, requeues *every* FAILED row:
    - `corrupt_row` rows, which would re-poison the worker
    - `outcome_unknown` rows (R3 already says bulk skips these)
    - attempts-exhausted poison jobs (E13), which would kill workers again
  - Requeueing a single parent does not revive children that were already cascade-failed with `dependency_blocked`. They stay FAILED silently. This is declared, and `--with-dependents` is a SHOULD.

**4. Sweep of operator surfaces**

| Surface | Finding |
|---|---|
| `hoglah rm` → `remove(cancel=True)` | It confirms *before* acting (correct), cancels, then calls `delete_job` immediately. An in-flight job loses its attempt record. Under R27 the delete is now refused while sinks are pending, and `rm` must report "deferred" rather than "Job not found". `remove(cancel=False)` is public API and deletes PROCESSING rows. The R27 store guard covers it. |
| `hoglah cancel` (CLI process, `start_worker=False`) | `_cancel_one` → `_deliver()` runs **in the short-lived CLI process**. The HTTP callback and file write are daemon threads that die when the CLI exits. There is no bridge for broker egress, so it waits for the startup-only sweep. The same applies to `_fail_dependents` → `_fail_blocked_dependency` → `_deliver` (**N43**, code-backed, medium). Fixed by extending R10's scope to every terminal writer. |
| `hoglah requeue`, `dlq` | Covered above. `dlq` is read-only. |
| `hoglah clear` | E25 is confirmed: it deletes before confirming. C24 holds on both stores. |
| `serve` / `web.py` | Every route is GET only, the binding is 127.0.0.1 by default, and there is no auth. `Hoglah(start_worker=False)` with no callbacks means no redelivery. `--host 0.0.0.0` exposes prompts and outputs, which falls under R22. `/metrics` falls under C23 and R6. **No state-changing surface.** |
| `messaging_submitter.py` | `submit()` mints a fresh `uuid4` correlation id on every call. A submitter timeout, or "not acknowledged within 10s" from Kafka, raises `MessagingSubmitError`, yet the job may already be queued or running. A caller that retries creates a **new** correlation id, so the job **runs twice**, and bridge dedupe cannot help (**N42**, medium; high for CLI-agent backends). `timeout_seconds=int(timeout)` couples the job's execution timeout to the client's wait. `int(0.5)=0` probably means no timeout at all. No cancel is sent on give-up. |
| Galeed (pass 7) | `_cancel_one` emits `JOB_CANCELLED` with `status="completed"` (client.py:960). A reconstruction that reads `status` misclassifies cancels. In E02 it also coexists with `JOB_COMPLETED` (N46, low). |

Named-callback redelivery runs only when callbacks are registered, so plain CLI and web instances do not re-fire. E15's scope is unchanged.

### Pass findings (passes 2–8 re-run over the whole set)

- **Pass 2 (attack recovery mechanisms): material, narrow.**
  - The R26 seal-versus-abandon race (N44).
  - The R27 tombstone semantics (resolved by deleting it).
  - `resume` races (resolved by deleting it).
  - R3's bulk-skip list was incomplete (`corrupt_row`, poison).
- **Pass 3 (propagation): minor.**
  - N43: a terminal write from a non-worker process means delivery is lost when that process exits.
  - Abandoned unsealed rows lead to a requeue that is stuck forever (N45).
- **Pass 4 (state after total process death): clean.**
  - With the header CAS, the state of a crashed submission (`open` header) is unambiguous after a restart.
  - Pending sinks written by a CLI-side cancel are swept by any worker (R10).
  - Nothing new.
- **Pass 5 (silent failures): minor.**
  - N45 (stuck forever, silently).
  - `rm` misreports a deferred delete as "not found".
  - Cancel-in-CLI loses its callback silently (N43).
- **Pass 6 (idempotency of every retry): material, one item.**
  - N42: the submitter-side retry is not idempotent and the ambiguous outcome is reported as failure.
  - Fix, folded into R12: a stable correlation id supplied by the caller, and a distinct `timed_out_unknown`.
- **Pass 7 (Galeed): minor.** N46: event status must equal the job status.
- **Pass 8 (simplification): yes.**
  - Deleted: the tombstone, `resume=True` and R24's enumerated test list (now a rule).
  - Merged: the "abandon" path into a header CAS that replaces both "report and cancel stale token" and resume.
  - **Is the set larger than the evidence justifies?**
    - **R26** rests on code-reading only. C25 is a real compensation and idempotency bug, but the Mongo partial-insert crash it guards against has never been observed. R26 is therefore split: the C25 fixes are MUST, and Mongo sealing is MUST only where `submit_batch` is used on Mongo, which is SHOULD-scoped.
    - **R14's kiro/gemini contracts** are doc-only and are downgraded to SHOULD, with detection.
    - **R5's two-observation rule** is backed by E17 and is kept.
    - **R6's in-memory gauges** are backed by C23 and are kept.
    - No other requirement lacks an E- or C-basis.

### Requirement set (vs iteration 3)

Job state is the 5-value `status`. Cause is `failure.category`. Action is `decision.action` ∈ {retry, fail, requeue_manual, none}. Ambiguity lives in `attempt.outcome`. **No exactly-once anywhere.**

**R1 ROB-STATE-001 — UNCHANGED.**
- The attempt log is append-only: `attempt_no`, phase, host and boot, outcome (succeeded/failed/unknown/lease_lost/cancelled), failure fields, `cli_version` and decision.
- `attempt_no` is incremented atomically with the claim.
- The policy is resolved and persisted at enqueue.
- Each requeue starts a new epoch, and `attempts_exhausted` is counted per epoch (E12, E13).

**R3 ROB-AMBIG-001 — CHANGED (bulk-skip list).**
- `dispatched` is committed before any side effect.
- A reclaim from dispatched or returned sets `outcome=unknown`. Only the rerunnable class auto-reruns.
- Single requeue is guarded, and `force` is recorded.
- **Bulk `requeue_failed` skips** `outcome_unknown`, unreapable, `corrupt_row` and attempts-exhausted-by-worker-death rows, and **any row whose token is not sealed**.
- A single requeue reports the dependents that are cascade-failed. `--with-dependents` is SHOULD.
- At-least-once for rerunnable jobs, at-most-once automatic dispatch otherwise.

**R4 ROB-LEASE-001 — UNCHANGED.**
- The heartbeat survives store errors and retries within the lease.
- If renewal fails before the monotonic expiry minus the margin, the worker aborts with `lease_lost`/`unknown`.
- A False renewal is classified by a fenced read (cancelled, gone or stolen). The heartbeat replaces the cancel watch.
- A store outage longer than one lease may lose results. This is declared.

**R5 ROB-LEASE-002 — UNCHANGED.**
- A reclaim needs `(token, expiry)` to stay unchanged for at least lease + margin of monotonic time.
- `$$NOW` on Mongo is only a prefilter.
- No reclaim in the first window after startup (E17).

**R6 ROB-LOOP-001 — UNCHANGED.**
- Sync calls on the loop are bounded per call and per poll. The preload tick reuses the poll cache. There is no per-job store polling apart from the heartbeat.
- Galeed emission is non-blocking. OpenAI calls go through `to_thread` or an async client.
- A loop-lag watchdog is required. The liveness gauges are store-independent, and the metrics scrape survives a store outage (E05, E06, C21, C23).

**R7 ROB-CORE-001 — UNCHANGED.**
- No pre-commit `get` (E23). The fenced commit is the only race guard.
- Nothing after commit changes the outcome (E04). Stream errors never fail the job (E16).

**R8 ROB-CORE-002 — UNCHANGED.**
- Cancel is one conditional write `IN (QUEUED, PROCESSING)` that clears the token, and returns False if it lost (E02).
- Remote cancel latency is ≤ the heartbeat interval. This is declared.
- A cancelled attempt that was dispatched gets `outcome=unknown`. The cascade is best-effort, and the poll is authoritative.

**R9 ROB-SCHED-001 — CHANGED (Mongo scope; sealed-token cache).**
- Readiness is cached per poll and the poll pages, so waiting rows never consume the window (E01).
- Every admission gate fails closed (C20).
- Errors are isolated per row (C19). The SQLite `list` tolerates undecodable rows, which are quarantined to FAILED `corrupt_row` (C18; SQLite only).
- A schema-invalid request on either store is classified `corrupt_row` rather than "Unexpected worker error".
- Admission also requires the row's token to be in the per-poll set of sealed tokens (R26).
- Cycles are rejected at enqueue.
- `job_stuck` combines with R6's gauges, so a persistent store error surfaces as `degraded`.

**R10 ROB-DELIV-001 — CHANGED (scope: every terminal writer).**
- Per-sink state (`pending|delivered|failed`, `delivery_id = job:attempt:sink`) is written **by every terminal write**: fenced commit, cancel, `dependency_blocked`/`label_blocked` and `corrupt_row`.
- A non-worker process, such as the CLI, the web app or an embedding caller with `start_worker=False`, may try delivery inline, but the record stays `pending` until it is confirmed. Any worker's periodic, fenced sweeper finishes it (N43).
- Idempotency keys and message ids are sent. Files use temp, fsync, rename. Direct callables are registered before enqueue.
- Each sink gets bounded attempts, then `failed` with a final event.
- At-least-once per (job, attempt, sink).

**R12 ROB-BRIDGE-001 — CHANGED (submitter side).**
- Ingress never skips (E18). A store outage pauses the consumer. A poison message goes to the DLT, then is acked.
- **`MessagingSubmitter.submit` accepts a caller-supplied `correlation_id`**, and the docs say to reuse it on retry. Failures are split:
  - a publish that is not acknowledged or a wait that expires raises `SubmitOutcomeUnknown`, which carries the correlation id
  - a definite publish error raises `MessagingSubmitError`
- The job timeout is separate from the wait timeout, and `int()` truncation to 0 is rejected (N42).
- Dedupe via `correlation_id` holds within R27's `dedupe_window`.

**R13 ROB-CLI-001 — UNCHANGED.**
- `stdin=DEVNULL` or a prompt file, never parsed as an option (E08, E09). A file is used above the argv limit (E21).
- Default timeouts. Group SIGTERM, then SIGKILL.
- pid, pgid, host and boot are persisted, and reaping happens only on the same boot (E12, E14).

**R14 ROB-CLI-002 — CHANGED (evidence-graded).**
- MUST, where the evidence comes from real runs: per-backend output and exit contracts for claude, codex and grok (E07, E08, E10).
- **SHOULD, where the evidence is doc-only:** the gemini exit codes and the kiro noise denylist.
- Exit 0 with an empty or noise-only payload is `protocol_error`. Incremental UTF-8 decoding (E07). Both streams are excerpted. Version drift is detected.

**R15 ROB-CLASS-001 — UNCHANGED.**
- Structured signals first. Regex only on provider text (E11).
- `unknown` gets at most 1 retry.
- `store_unavailable`, `lease_lost` and `corrupt_row` are distinct categories.

**R16 ROB-RETRY-001 — UNCHANGED.**
- A retry is a fenced requeue with `run_at`.
- Per-category budgets (E20). Retry-After is honoured. Jitter is > 0.
- `run_at` is a not-before hint.

**R18 ROB-LOG-001 — CHANGED (event status fidelity).**
- The previous event set is kept, plus `batch_abandoned`.
- **An event's `status` must equal the row's resulting job status.** For example, `JOB_CANCELLED` must not carry `"completed"` (N46).
- Bounded non-blocking emission. Failures are logged at WARNING with rate limiting. The spool is SHOULD.
- The store is authoritative only for retained rows.

**R20 ROB-SHUT-001 — UNCHANGED.**
- SIGTERM and SIGINT stop claiming, drain within a bound, terminate CLI groups and close (E14).
- `run` exits non-zero if the worker thread dies.

**R21 ROB-OLLAMA-001 — UNCHANGED.**
Timeouts, truncation detection, empty or `load` responses not treated as success, auto-pull opt-in and outside the attempt (E19, E20).

**R22 ROB-SEC-001 — UNCHANGED.**
- Redaction everywhere, claude with `--bare`, and a broker CLI allow-list.
- `serve` warns when it binds to a non-loopback address. This is a wording clarification.

**R23 ROB-CONF-001 — UNCHANGED.**
Env formats (E22) and startup validation of budgets.

**R24 ROB-TEST-001 — CHANGED (rule, not list).**
- Every E-scenario (E01–E25) and every code-backed N-mode in this set gets a deterministic fault-injection test that asserts the required state, cause, action and events.
- Restart tests use a real process kill. State-machine property tests cover R25, and gated real-provider smoke tests cover R14's MUST backends.
- The previous enumerations (a)–(aa) are kept as the starting index.

**R25 ROB-STATE-003 — CHANGED (abandoned-token rows).**

| Transition | Cause |
|---|---|
| QUEUED→PROCESSING | claim (attempt+1, token sealed) |
| PROCESSING→QUEUED | fenced retry, or reclaim under R5/R3 |
| PROCESSING→COMPLETED or FAILED | fenced commit |
| QUEUED or PROCESSING→CANCELLED | conditional cancel |
| QUEUED→FAILED | conditional write for `dependency_blocked`, `label_blocked` or `corrupt_row` |
| FAILED or CANCELLED→QUEUED | operator requeue (new epoch; sealed rows only) |
| terminal→deleted | only under R27 |
| **unsealed(abandoned)→deleted** | the token-scoped delete, R26's only deleter of unsealed rows |

- COMPLETED is absorbing.
- Non-terminal rows are deleted only via `--force-nonterminal`, which cancels them first.
- Defined races:
  - cancel vs retry: the first write wins
  - cancel vs claim: remote abort, outcome unknown
  - reclaim vs commit: decided by fencing
  - delete vs claim: the delete loses
  - **seal vs abandon: decided by the header CAS; the loser's operation is a no-op**

**R26 ROB-BATCH-001 — CHANGED (resume deleted; header CAS; evidence split).**
- **MUST (C25):**
  - Item `idempotency_key`s are resolved before ids are assigned, so an existing job maps into `name_to_id`.
  - Compensation touches only rows this call inserted, so pre-existing jobs are never deleted (N33).
  - Local names may not shadow existing ids.
  - External `depends_on` is validated at enqueue (SHOULD).
- **Atomic release:**
  - SQLite uses one transaction.
  - Mongo inserts rows with a fresh `seal_token`, then CASes the header document `open→sealed`. Abandonment, whether by the caller, an operator or a stale threshold, CASes `open→abandoned` and then deletes that token's rows (N34, N44).
  - Reusing a `batch_id` is allowed: it is a grouping label and is declared non-unique. **`resume` is removed**, and the retry for a crashed submission is a fresh call.
  - `wait`/`wait_batch` on an abandoned token raises an explicit `BatchAbandoned`. `cancel_batch` on an open token abandons it.
- All-or-nothing with respect to dispatch, not storage.

**R27 ROB-ADMIN-001 — CHANGED (tombstone replaced by retention floor).**
- The store-level `delete_job` and `delete_jobs` (on **both** SQLite and Mongo, C24 confirmed) skip by default:
  - non-terminal rows
  - unsealed rows
  - rows with pending sinks
  - rows referenced by a live dependent or label peer
  - **rows that carry an `idempotency_key`/`correlation_id` and are younger than `dedupe_window`**
- `--force` overrides the last rule only. `--force-nonterminal` follows R25.
- `clear` does a dry-run count, confirms, then deletes (E25).
- `rm` reports a deferred delete as "deferred: <reason>" rather than "not found".
- Every deletion emits `rows_deleted`.

**Merged (carried):** R2→R1/R3, R11→R10, R17→R16, R19→R7.

### New failure modes
| ID | Failure mode | Pass | Basis | Severity | Covered by |
|---|---|---|---|---|---|
| N42 | A submitter timeout or unacknowledged publish is reported as failure, and a retry mints a new correlation id, so the job is executed twice | 6 | code-backed (`messaging_submitter.py:120-140`) | medium (high for CLI agents) | R12 |
| N43 | A terminal write in a non-worker process (CLI cancel, cascade) delivers inline on daemon threads that die when the process exits, so callbacks and files are lost and broker egress waits for a worker restart | 3/5 | code-backed (`_cancel_one`→`_deliver`, `_get_hoglah(start_worker=False)`) | medium | R10 |
| N44 | Seal and abandon race on Mongo: a cancel or delete of a slow-but-live submission while its `updateMany` seal runs means partial dispatch | 2 | design attack on R26 | medium-low | R26, R25 |
| N45 | Cancelled unsealed rows that are requeued become QUEUED forever, unclaimable and silent | 3/5 | design + code (requeue is unconditional on seal) | low | R3, R26 |
| N46 | `JOB_CANCELLED` is emitted with `status="completed"`, so Galeed misreports cancels | 7 | code-backed (client.py:960) | low | R18 |
| N47 | Bulk `requeue_failed` re-queues `corrupt_row` and poison rows, re-poisoning workers | 2 | code-backed (`requeue_failed` lists all FAILED) | medium | R3 |

**Refuted or downgraded:** C18 as a poll-wide stall on Mongo (refuted; per-job only). The R27 tombstone (deleted as semantically wrong).

**Per pass:**

| Pass | Material? | Findings |
|---|---|---|
| 2 | yes, narrow | N44 (a new transition race), N47, tombstone and `resume` defects |
| 3 | minor | N43, N45 |
| 4 | no | — |
| 5 | minor | N43, N45, `rm` message |
| 6 | yes, one item | N42 |
| 7 | minor | N46 |
| 8 | yes (deletions) | tombstone, `resume` and the test list removed; R14 and R26 evidence-graded |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.95 | Every listed surface is now audited: the Mongo store, the CLI admin commands, web and the submitter. The remaining unmapped items are minor: mixed-version request schemas (now per-job `corrupt_row`) and `wait_batch` wall-clock deadlines. |
| restart_reconstructability | 0.88 | The header CAS makes a crashed batch's state unambiguous. Sinks from CLI-side terminal writes survive the process exiting. Results lost during an outage are still declared. |
| retry_idempotency_safety | 0.85 | The submitter-retry duplication was found and closed with a stable correlation id and an explicit outcome-unknown. The bulk requeue no longer re-poisons. Double-billing on rerunnable jobs and redelivery after the window are declared. |
| galeed_reconstructability | 0.75 | Event status fidelity was added. Galeed remains lossy and is the sole history after deletion. |
| guarantee_honesty | 0.92 | Submitter timeouts are now "unknown", not "failed". The dedupe window returns only live rows. `batch_id` is declared non-unique. Doc-only CLI contracts are graded SHOULD. |
| simplicity_compat | 0.80 | Net deletions: the tombstone table, `resume=True` and the enumerated tests. The header document is Mongo-only and only for `submit_batch`. No new requirement ID. Compatibility changes: `clear` keeps keyed and young rows, and `requeue_failed` skips some rows. |
| residual_high_risk | 0.17 | Nothing is built yet. N32–N47 are mostly unreproduced (only E25 is reproduced). The header CAS is a design, not a test. CLI contract drift is detected, not prevented. |

## Changes From Previous
- **CHANGED:**
  - R3 (bulk-skip `corrupt_row`, poison and unsealed rows; dependents reported)
  - R9 (C18 SQLite-only; schema-invalid → `corrupt_row`; sealed-token admission)
  - R10 (every terminal writer writes sink state; non-worker processes leave it to a sweeper)
  - R12 (submitter correlation id and unknown outcome)
  - R14 (MUST/SHOULD by evidence)
  - R18 (event status fidelity, `batch_abandoned`)
  - R24 (rule instead of list)
  - R25 (abandoned-row deletion; seal-vs-abandon race)
  - R26 (`resume` deleted; header CAS; C25 MUST split from the sealing scope)
  - R27 (tombstone replaced by retention floor; Mongo parity; `rm` deferral message)
- **UNCHANGED:** R1, R4, R5, R6, R7, R8, R13, R15, R16, R20, R21, R23. R22 gained only a wording clarification on `serve` binding.
- **MERGED (carried):** R2, R11, R17, R19.
- **ADDED:** none.
- **DELETED (mechanisms):**
  - the R27 tombstone (wrong semantics)
  - R26 `resume=True` (racy and redundant)
  - R26's "report-and-cancel stale token" (replaced by abandon-and-delete under CAS)
  - R24's enumeration as the normative text

## Uncertainty
- N42–N47 are code-read or design-derived and none is reproduced. N44 in particular is a design attack on a mechanism that does not exist yet.
- I did not verify whether Galeed consumers read `status` or `type` when reconstructing outcomes, so N46's impact may be only cosmetic.
- I did not verify whether callers (Tirzah) actually retry `MessagingSubmitter.submit` on timeout, so N42's likelihood is unknown. The mechanism is certain.
- Compatibility: a non-unique `batch_id` is declared rather than changed. Whether any caller relies on `requeue_failed` re-queueing `dependency_blocked` children is unknown.
- The `dedupe_window` default is unspecified. It should exceed the longest broker redelivery horizon in use.

## Stop-Relevant Notes
- **Not convergent, but close.**
  - No new **high**-severity failure mode was found, and no major risk judgement moved by more than one step.
  - Two conditions still fail:
    1. Attacking R26 exposed one new state-transition ambiguity: seal vs abandon on Mongo (N44, resolved by a header CAS in R25 and R26).
    2. N42 adds a material obligation to a newly audited surface, though it is folded into R12 and adds no new ID.
  - Everything else was a clarification, a low-severity fold, or a deletion.
- Every operator and state-changing surface named in the original request has now been read. Nothing unaudited remains in `src/hoglah`.
- **The next pass should:**
  - attack the header CAS: the poll's sealed-token cache going stale, a header write failing after the rows are inserted, and abandon-delete racing a claim. Under R9 a claim requires sealed, so it should lose.
  - attack R12's `SubmitOutcomeUnknown`, and R10's rule that non-worker processes leave delivery pending, for any new silent path
  - otherwise make no additions. If neither attack yields a new transition or a medium+ mode, iteration 5 should be declared convergent.
