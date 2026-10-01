# Iteration 5

## Answer

### Requested attacks (read-only, src/hoglah v0.13.0)

**A. R26 header CAS.**
- *Stale sealed-token cache in the poll* — safe **iff `sealed` is absorbing** (no sealed→abandoned; abandon is only `open→abandoned`). Then a stale cache can only *omit* a newly sealed token (admission delayed one poll), never admit an abandoned one. Made explicit (INV-9). `cancel_batch` on a sealed token is per-row cancel, never abandon.
- *Header write failing after rows inserted* — order fixes most of it: header `open` is inserted **before** any row (a failed header insert = definite error, nothing written). Crash mid-insert leaves `open` + partial rows → stale abandoner. Remaining hole: the seal CAS itself times out ambiguously (Mongo network error after apply). Fold: `submit_batch` raises `BatchOutcomeUnknown(seal_token)`; resolution is a header read (`batch_status(token)`), never a blind abandon (the CAS would decide anyway).
- *Slow live owner vs stale abandoner* — new wrinkle: after `open→abandoned` + token-delete, the still-running owner keeps inserting rows → orphan unsealed rows (never admitted, but counted as queued). Fold: owner's seal CAS fails → owner deletes its own token rows and raises `BatchAbandoned`; plus abandoned-token GC is **repeated** by the sweeper (not one-shot), and `stats()` excludes unsealed rows. Covered by the existing `unsealed(abandoned)→deleted` transition — no new transition.
- *Abandon-delete racing a claim* — claim needs the token in the sealed set; an abandoned token is never sealed (INV-9), so the claim cannot happen. No race.
- *Mixed-version rollout* — a v0.13 worker on the same Mongo ignores seals and can claim unsealed rows. Declared: Mongo `submit_batch` sealing requires all workers ≥ the sealing version (low; wording).

**B. R12 `SubmitOutcomeUnknown` — new material silent path (N48).** Following R12's own advice (retry with the same `correlation_id`) does **not** resolve on two of three transports:
- Ingress dedupe hit (`store.enqueue` returns the existing row; `kafka_bridge.py:560-588`) passes the H3 check and is acked silently. The row's stored `_kafka.reply_to` is the **first** submission's route; nothing is re-published.
- Rabbit: the reply queue is `exclusive, auto_delete` per transport (`messaging_submitter.py:227`) — the first route is gone once that submitter closes. A not-yet-finished job then publishes to a dead queue (UnroutableError → left unpublished); a finished one already published there.
- Redis: the retry captures the results-stream tail **before** re-adding (`:268-270`), so a result published before the retry is never read.
- Kafka: works only because the submitter reads from `earliest` in a fresh group (within retention).
- Net: the caller gets `SubmitOutcomeUnknown` forever for a job that has completed — silent, medium (high for CLI agents if the caller then gives up and resubmits under a new id, re-creating N42).
- Fix (into R12 + R10): an ingress dedupe hit **adds a broker sink for the new reply route** (append, `delivery_id = job:attempt:broker:route`) — `pending`, delivered immediately if the row is already terminal. Existing sink records are never flipped back (no new transition on old records; INV-11).

**C. R10 "non-worker processes leave delivery pending for a worker sweeper".**
- *No worker ever runs* (or none with the needed capability — e.g. no bridge configured): pending sinks sit forever, silently, and also block R27 deletion. Fold: `delivery_backlog{sink}` gauge (store-independent age of oldest pending); the CLI prints "delivery pending (no worker confirmed)"; a sweeper only claims sinks it can deliver and **never** marks an undeliverable-by-me sink failed/published (today `republish_unpublished` marks non-bridge rows published — fine — but a non-bridge worker must not touch broker sinks).
- *Direct callables (code-backed, `client.py:613,683,2196`)*: a callable passed to `submit()` on a `start_worker=False` instance never fires (the job finishes in another process); a callable registered on an idempotent dedupe hit of an already-terminal job never fires either. Both silent (N49, low-medium). Fold into R10: direct callables are process-local best-effort; on dedupe-hit of a terminal row fire immediately; on `start_worker=False` raise (or warn loudly) and point to named/HTTP callbacks.

### Pass 8 (delete / merge) over the whole set
- **Merged R21 → R14** (now "backend result contracts"): "success with empty / noise / `load` payload = `protocol_error`" is one rule for CLI, Ollama and OpenAI; Ollama timeouts move to R13's general "every backend has a default timeout" clause (R13 broadened to all adapters).
- **R25 kept** as the single normative transition table; the INV list below is derived from it, not a second source.
- Nothing deleted: every live requirement has an E- or code-backed C/N basis; R26's Mongo sealing stays SHOULD-scoped (only with Mongo + `submit_batch`), R18's spool stays SHOULD.
- Considered and rejected: replacing the header CAS by a `run_at` sentinel (crash between CAS and `updateMany` creates sealed-but-sentinel rows → needs its own repair sweep; not simpler).

### Pass 5 (whole-set silent-failure sweep)
New or residual silent paths, all folded: pending sinks with no capable sweeper (C above); direct callables never firing (N49); orphan rows of abandoned tokens (A above); sink `failed` after bounded attempts now also increments `delivery_failed` (previously event-only, and Galeed may be down). No others found: corrupt rows are quarantined (R9), stuck-due jobs surface via `job_stuck` (R9/R6), unknown outcomes land in FAILED `outcome_unknown` visible in `dlq` (R3), lost leases are logged at WARNING (R4/R18).

### Final requirement set

Conventions: JOB STATE = the 5-value `status`; CAUSE = `failure.category`; ACTION = `decision.action` ∈ {retry, fail, requeue_manual, none}; ambiguity = `attempt.outcome=unknown`. **No exactly-once anywhere**; guarantees are at-least-once (rerunnable) or at-most-once automatic dispatch (non-rerunnable), and at-least-once per delivery sink.

**R1 ROB-STATE-001 — P0 — Attempt log.** Every job has an append-only attempt log: `attempt_no`, phase (claimed|dispatched|returned|committed), host, boot_id, started/ended, outcome (succeeded|failed|unknown|lease_lost|cancelled), failure{category, retryable, provider_code, exit_code, signal, http_status, exception_class, sanitized excerpt}, `cli_version`, decision{action, delay}. `attempt_no` is incremented in the same atomic statement as the claim. The retry policy is resolved and persisted at enqueue. `max_retries` is enforced across process lifetimes per epoch (each operator requeue starts a new epoch); exceeding it → FAILED `attempts_exhausted` (poison bound). *Covers E12, E13, code fact (no persisted attempt counter); absorbs R2.*

**R3 ROB-AMBIG-001 — P0 — Ambiguous outcomes.** Phase `dispatched` is committed before any provider side effect. Reclaim of an attempt in dispatched/returned sets `outcome=unknown`; only the rerunnable class (stub/ollama/openai inference by default) auto-reruns; non-rerunnable (CLI agents by default) → FAILED `outcome_unknown`, action `requeue_manual`. Single requeue of such a row requires `force`, which is recorded. Bulk `requeue_failed` skips `outcome_unknown`, unreapable, `corrupt_row`, attempts-exhausted-by-worker-death rows and rows whose token is not sealed. Single requeue reports cascade-failed dependents; `--with-dependents` SHOULD. *Covers E12, E13, E14, N45, N47.*

**R4 ROB-LEASE-001 — P0 — Heartbeat survives.** The heartbeat retries transient store errors within the remaining lease; if renewal has not succeeded by monotonic expiry minus margin, the worker aborts provider work and records `lease_lost` (outcome unknown). A False renewal is classified by a fenced read (cancelled / gone / stolen). The heartbeat is the cross-process cancel watch (≤ one interval latency). A store outage longer than one lease can lose a result — declared. Failures logged at WARNING. *Covers E03, N35/C22.*

**R5 ROB-LEASE-002 — P0 — Clock-safe reclaim.** Reclaim requires the same `(lease_token, lease_expires_at)` observed unchanged across ≥ lease + margin of the reclaimer's monotonic time; Mongo `$$NOW` is only a prefilter; no reclaim in the first window after process start. *Covers E17, WSL clock-jump docs.*

**R6 ROB-LOOP-001 — P1 — Non-blocking loop.** No unbounded sync I/O on the worker event loop: store/Galeed calls bounded per call and per poll; preload reuses the poll's cache; no per-job store polling except the heartbeat; Galeed emission non-blocking; OpenAI adapter via `to_thread` or an async client. A loop-lag watchdog is required. Liveness gauges (loop lag, last successful poll, `job_stuck`, `delivery_backlog`, `delivery_failed`) are in-memory/store-independent and the metrics scrape survives a store outage. *Covers E05, E06, C21, C23, N38, N39.*

**R7 ROB-CORE-001 — P0 — Commit is final.** No pre-commit `get`; the fenced `set_result` is the only race guard. After a successful commit nothing may change the outcome; later exceptions are delivery/observability failures (logged, sink-recorded), never a FAILED delivery. Diagnostic stream errors disable the stream for that job and never fail it. *Covers E04, E16, E23; absorbs R19.*

**R8 ROB-CORE-002 — P0 — Conditional cancel.** Cancel is one conditional write `status IN (QUEUED, PROCESSING)` that clears the lease token and returns False if it lost; never overwrites a terminal row. A cancelled attempt that was dispatched gets `outcome=unknown`. Dependent cascade is best-effort; the poll's readiness check is authoritative. *Covers E02.*

**R9 ROB-SCHED-001 — P0 — Poll cannot stall.** Readiness (depends_on, label sequence, sealed token) is cached per poll and the poll pages, so waiting rows never consume the claim window. Every admission gate fails closed. Errors are isolated per row; SQLite `list` tolerates undecodable rows and quarantines them to FAILED `corrupt_row`; a schema-invalid request on either store is `corrupt_row`, not "Unexpected worker error". Dependency cycles are rejected at enqueue. `job_stuck` (QUEUED-and-due older than threshold) plus R6 gauges surface persistent store errors as `degraded`. *Covers E01, E26, C18 (SQLite), C19, C20, N36, N37, N40.*

**R10 ROB-DELIV-001 — P1 — Durable delivery sinks.** Every terminal write (fenced commit, cancel, `dependency_blocked`/`label_blocked`, `corrupt_row`) writes per-sink records (output_file, http_callback, named_callback, broker) with state pending|delivered|failed and `delivery_id = job:attempt:sink[:route]`. Sink records are append-only per id; terminal sink states are never reopened. A non-worker process may attempt inline delivery but the record stays `pending` until confirmed; a periodic, fenced sweeper in any worker completes it, claiming only sinks it is capable of delivering. Bounded attempts per sink, then `failed` + final event + `delivery_failed` metric. HTTP carries an Idempotency-Key; broker messages carry the delivery id; files: unique temp, fsync file+dir, rename. Named-callback redelivery is driven by sink state (no "first 200" scan). Direct callables are process-local, best-effort: registered before enqueue; fired immediately on a dedupe hit of a terminal row; refused (or loud warning) on a `start_worker=False` instance. `delivery_backlog` exposes pending age; CLI terminal writes print "delivery pending" when unconfirmed. At-least-once per (job, attempt, sink, route). *Covers E04, E05, E15, N43, N48 (sink side), N49; absorbs R11.*

**R12 ROB-BRIDGE-001 — P0 — Ingress never skips; submit resolvable.** Ingress never commits/acks past an unprocessed message: Kafka seeks back to the failed offset (or pauses), Rabbit nack-requeue with backoff + consumer reconnect, Redis periodic XAUTOCLAIM. A store outage pauses the consumer; a poison message goes to the DLT, then is acked. On an ingress dedupe hit the bridge appends a broker sink for the message's reply route (delivered at once if the row is terminal). `MessagingSubmitter.submit` accepts a caller-supplied `correlation_id` (docs: reuse it on retry); a publish not acknowledged or a wait that expires raises `SubmitOutcomeUnknown(correlation_id)`, a definite publish error raises `MessagingSubmitError`; job timeout is separate from wait timeout and `int()` truncation to 0 is rejected. Dedupe holds within R27's `dedupe_window`; after it a redelivery may duplicate — declared. *Covers E18, N41, N42, N48.*

**R13 ROB-CLI-001 — P1 — Subprocess and timeout hygiene.** CLI: stdin=DEVNULL or prompt via stdin/file; prompt never parsed as an option; prompt file above the argv limit; kill the process group SIGTERM→SIGKILL(group); persist pid, pgid, host, boot_id, start_time so a recovering worker reaps orphans on the same boot before any rerun. Every adapter (CLI, Ollama, OpenAI) has a default connect/read-idle/total timeout. *Covers E08, E09, E12, E14, E21, E06 (timeouts), Ollama `timeout=None` code fact.*

**R14 ROB-BACKEND-001 — P0 — Backend result contracts.** A "success" with an empty, noise-only or non-final payload is `protocol_error`, never COMPLETED (CLI exit 0 + empty/stderr-only, Ollama `done_reason: load`/empty). MUST (real-run evidence): per-backend structured-output and exit-code parsing for claude, codex, grok; Ollama truncation detection (`prompt_eval_count` vs `num_ctx`); incremental UTF-8 decoding of streamed output; error excerpts from both streams, sanitized, tail-preserving, signal named. SHOULD (doc-only): gemini exit codes, kiro noise denylist. CLI version drift detected and recorded. Ollama auto-pull is opt-in and outside the job attempt. *Covers E07, E08, E10, E19, E20; absorbs R21.*

**R15 ROB-CLASS-001 — P1 — Classification.** Canonical cause taxonomy (incl. auth, quota/rate_limit, provider_unavailable, network, invalid_request, model_not_found, protocol_error, timeout, store_unavailable, lease_lost, corrupt_row, outcome_unknown, attempts_exhausted, unknown) separate from action and state. Structured signals first; regex only on provider-originated text, never on text containing the prompt/model output; `unknown` gets at most one retry. *Covers E10, E11, E19.*

**R16 ROB-RETRY-001 — P2 — Retry scheduling.** A retry is a fenced requeue with `run_at` (not an in-claim sleep), so it survives restarts and frees slots. Per-category budgets and max_elapsed (sized beyond daemon restart time for provider_unavailable); Retry-After honoured; jitter > 0; `run_at` is a not-before hint. Per-backend breaker: repeated unavailable/auth failures defer that backend's queued jobs without consuming attempts; half-open single probe; state exported. *Covers E20; absorbs R17.*

**R18 ROB-LOG-001 — P1 — Galeed events.** Events for attempt start/end with category, retry scheduled, lease reclaimed/lost, outcome_unknown, delivery attempt/failure, breaker transitions, startup recovery, `batch_abandoned`, `rows_deleted`. Event `status` equals the row's resulting job status. Bounded non-blocking emitter, drops counted; Galeed failures logged at WARNING, rate-limited; local spool replay SHOULD. The store is authoritative only for retained rows. *Covers E03, E05, N46.*

**R20 ROB-SHUT-001 — P1 — Shutdown.** SIGTERM/SIGINT: stop claiming, bounded drain, terminate CLI groups, close. `hoglah run` exits non-zero if the worker thread dies. *Covers E14.*

**R22 ROB-SEC-001 — P1 — Secrets and trust.** Redact secrets (e.g. `metadata.api_key`) from results, files, callbacks, Galeed, excerpts. Claude CLI runs with `--bare` (no workdir hooks/MCP); broker-selected CLI backends need an allow-list; `serve` warns on non-loopback bind. *Covers metadata-merge code fact, Claude headless docs.*

**R23 ROB-CONF-001 — P2 — Config.** Documented env formats (k=v, comma lists) accepted or docs corrected; startup validation of budgets and timeouts. *Covers E22.*

**R24 ROB-TEST-001 — P2 — Fault-injection tests.** Every E-scenario (E01–E26) and every code-backed N-mode in this set has a deterministic test asserting state, cause, action and events; restart tests use a real process kill; state-machine property tests over R25; gated real-provider smoke tests for R14's MUST backends.

**R25 ROB-STATE-003 — P0 — Transition table (normative).**
| Transition | Only via |
|---|---|
| QUEUED→PROCESSING | claim (attempt+1; token sealed or no token) |
| PROCESSING→QUEUED | fenced retry, or reclaim under R5/R3 |
| PROCESSING→COMPLETED/FAILED | fenced commit |
| QUEUED/PROCESSING→CANCELLED | conditional cancel |
| QUEUED→FAILED | conditional write: dependency_blocked, label_blocked, corrupt_row, attempts_exhausted |
| FAILED/CANCELLED→QUEUED | operator requeue (new epoch; sealed rows only) |
| terminal→deleted | R27 only |
| unsealed(abandoned)→deleted | token-scoped delete / abandoned-token GC |
Header (Mongo batch): `open→sealed` or `open→abandoned`, CAS, both absorbing. Races: cancel vs retry/commit — first conditional write wins; cancel vs claim — remote abort, outcome unknown; reclaim vs commit — fencing; delete vs claim — delete loses; seal vs abandon — header CAS, loser no-op. Non-terminal rows are deleted only via `--force-nonterminal`, which cancels first. *Covers E02, E25, N32, N44.*

**R26 ROB-BATCH-001 — P0 (C25 part) / P2 (Mongo sealing).** MUST: item idempotency keys resolved before ids are assigned; compensation touches only rows this call inserted; local names may not shadow existing ids; external `depends_on` validated (SHOULD). Atomic release: SQLite one transaction; Mongo inserts the header `open` first, rows with that `seal_token` next, then CAS `open→sealed`. An ambiguous seal write raises `BatchOutcomeUnknown(token)`, resolved by reading the header. Abandonment (caller, operator, stale threshold) CASes `open→abandoned`, then deletes the token's rows; an owner whose seal CAS loses deletes its own rows and raises `BatchAbandoned`; the sweeper repeats abandoned-token GC. `wait`/`wait_batch` on an abandoned token raise `BatchAbandoned`; `wait_batch` resolves membership via the header; `stats()` excludes unsealed rows. `batch_id` is a non-unique grouping label; `resume` does not exist. Mongo sealing requires all workers at ≥ the sealing version — declared. All-or-nothing w.r.t. dispatch, not storage. *Covers C25, N33, N34, N44, N45.*

**R27 ROB-ADMIN-001 — P0 — Safe deletion.** Store-level `delete_job`/`delete_jobs` (SQLite and Mongo) skip by default: non-terminal, unsealed, pending-sink, live-dependent/label-peer, and dedupe-keyed rows younger than `dedupe_window` (`--force` overrides only the last). `clear` = dry-run count → confirm → delete. `rm` reports "deferred: <reason>". Every deletion emits `rows_deleted`. *Covers E25, C24, N32, N41.*

**Merged (carried):** R2→R1/R3, R11→R10, R17→R16, R19→R7, **R21→R14 (this pass)**.

### State invariants
- INV-1 Every status change is a conditional (compare-and-set) write; no unconditional status write exists.
- INV-2 COMPLETED is absorbing; FAILED/CANCELLED leave only via operator requeue (new epoch).
- INV-3 A PROCESSING row has exactly one live lease token; only its holder can commit, retry or renew.
- INV-4 `attempt_no` increases with every claim and is never reset within an epoch; attempts per epoch ≤ max_retries+1.
- INV-5 An attempt whose phase reached `dispatched` without a recorded end has outcome `unknown` after reclaim/cancel; it is never recorded as not-run.
- INV-6 A non-rerunnable job is never automatically dispatched twice for the same epoch.
- INV-7 After a successful fenced commit, the job's status and result never change except by operator requeue.
- INV-8 Every terminal row has one sink record per configured sink; a terminal sink state never reverts.
- INV-9 A row is claimable only if it has no seal token or its token is sealed; sealed and abandoned are absorbing and mutually exclusive.
- INV-10 A due QUEUED row whose dependencies are satisfied is admitted within a bounded number of polls, independent of other rows' readiness or decodability.
- INV-11 An accepted ingress message is acked/committed only after its job row (or a DLT copy) is durable; a dedupe hit yields a deliverable sink for the new reply route.
- INV-12 No row that is non-terminal, unsealed, has pending sinks, has live dependents, or is a dedupe key inside the window is deleted without an explicit force flag.
- INV-13 Every Galeed event's `status` equals the row's resulting status; Galeed loss never alters job state.
- INV-14 Failure cause, recovery action and job state are separate fields; none is inferred from free-text error strings when a structured signal exists.

### New failure modes this pass
| ID | Failure mode | Pass | Basis | Severity | Covered by |
|---|---|---|---|---|---|
| N48 | Retry with the same correlation id after `SubmitOutcomeUnknown` never resolves on Rabbit/Redis: dedupe hit acked silently, reply route frozen to the first (possibly deleted) queue, Redis retry reads only after its new tail | 2/5/6 | code-backed (`kafka_bridge.py:560-588`, `messaging_submitter.py:227,268`) | medium | R12, R10, INV-11 |
| N49 | Direct callable never fires on a `start_worker=False` instance or on a dedupe hit of a terminal job | 5 | code-backed (`client.py:613,683,2196`) | low-medium | R10 |
| N50 | A slow live batch owner keeps inserting after a stale abandon → orphan unsealed rows counted as queued | 2 | design attack on R26 | low | R26 (fold) |
| N51 | Ambiguous Mongo seal write leaves caller unsure whether the batch will run | 2 | design attack on R26 | low | R26 (fold) |
| N52 | No capable sweeper (no worker / no bridge) → pending sinks forever, blocking deletion | 5 | design + code (startup-only outbox) | low | R10, R6 gauge |

**Per pass:**
| Pass | Material? | Findings |
|---|---|---|
| 2 | yes (one) | N48 via attacking R12's own fix; header-CAS attacks yield only folds (N50, N51) |
| 3 | minor | broker sink needs bridge capability in the sweeping worker |
| 4 | no | header-first ordering makes every crash point unambiguous |
| 5 | minor | N49, N52, `delivery_failed` metric |
| 6 | yes (same item) | N48 is an idempotent-retry that is safe but non-resolving |
| 7 | no | — |
| 8 | yes (merge) | R21→R14; R13 timeout clause generalized; sentinel alternative rejected |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.96 | Submitter retry routing and process-local callables were the last unaudited paths; all are now mapped. |
| restart_reconstructability | 0.89 | Header-first ordering + absorbing seal states make every batch crash point resolvable; results lost in long store outages remain declared. |
| retry_idempotency_safety | 0.87 | Stable correlation retries now resolve on all transports; double-billing for rerunnable jobs and post-window redelivery remain declared. |
| galeed_reconstructability | 0.75 | Unchanged; Galeed is lossy and sole history after deletion. |
| guarantee_honesty | 0.93 | Direct callables declared process-local; mixed-version sealing limit and dedupe window declared. |
| simplicity_compat | 0.81 | One merge (R21→R14); no new ID; sink records append-only instead of a re-arm transition. Compat cost: `submit(callback=...)` on `start_worker=False` now refuses/warns. |
| residual_high_risk | 0.16 | Nothing implemented; N42–N52 unreproduced; N48's fix touches the bridge ingress hot path. |

## Changes From Previous
- CHANGED: R10 (append-only sinks per route, capability-scoped sweeper, direct-callable rules, `delivery_backlog`/`delivery_failed`), R12 (dedupe hit appends reply-route sink), R13 (default timeouts for every adapter), R14 (renamed ROB-BACKEND-001, absorbs R21), R25 (header states absorbing; attempts_exhausted listed), R26 (header-first order, `BatchOutcomeUnknown`, owner self-cleanup, repeated GC, stats exclude unsealed, mixed-version limit), R6 (two new gauges).
- MERGED: R21→R14.
- UNCHANGED: R1, R3, R4, R5, R7, R8, R9, R15, R16, R18, R20, R22, R23, R24 (range extended to E26), R27.
- ADDED / DELETED IDs: none. Added the INV-1…14 list.

## Uncertainty
- N48 is code-read, not reproduced; whether any caller uses Rabbit/Redis submitters with retry is unknown (Kafka path works within retention).
- R10's "refuse direct callable on `start_worker=False`" may break an embedding that relies on in-process manual `process_one`-style execution; "warn" may be the compatible choice.
- Header-CAS attacks are attacks on a design that does not exist yet; real Mongo write-concern behaviour on ambiguous errors is unverified.
- `dedupe_window` and sweeper intervals remain unspecified defaults.

## Stop-Relevant Notes
- **NOT CONVERGENT (narrowly).** The header-CAS attacks produced only folds and confirmed no new transition (sealed absorbing closes the stale-cache and claim races). But attacking R12's `SubmitOutcomeUnknown` found N48: R12's prescribed retry silently never resolves on Rabbit/Redis. Fixing it is a material new obligation (an ingress dedupe hit must append a reply-route sink). It is medium severity, not high. No risk judgement moved by more than one step, and the sink state machine stays transition-free because sinks are append-only.
- If another pass is run, attack only (a) the append-on-dedupe-hit sink (unbounded sinks if a client retries in a loop: cap per job) and (b) the capability-scoped sweeper. If neither yields a medium+ mode, declare convergence. The set is otherwise stable: three passes with no new high-severity mode.
