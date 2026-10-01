# SOURCE — Hoglah robustness requirements: recursive attack/convergence

## Original request (condensed, binding)
Produce an evidence-based robustness requirements set for **Hoglah** (local-first durable LLM job queue:
SQLite/Mongo store, asyncio worker in a daemon thread, leases+heartbeat, in-memory RetryPolicy, depends_on,
batches, cancel, callbacks (in-process / named / HTTP), output files, NDJSON token streams, Kafka/RabbitMQ/Redis
bridges with an egress outbox, Galeed trace spine (Mongo), adapters: Ollama, OpenAI-compatible HTTP, CLI agents
claude/codex/gemini/grok/kiro). Not a daemon/supervision design. Must: separate failure CAUSE / recovery ACTION /
JOB STATE; represent ambiguous outcomes honestly; never claim exactly-once where not provided; keep Hoglah
lightweight (SQLite, stub default, low deps, existing public API and 5-value status enum where practical).
Each loop pass must perform: (2) attack each recovery mechanism ("how does this fix fail?"), (3) cross-component
propagation, (4) state reconstruction after total process death, (5) silent-failure hunt, (6) idempotency of every
retry, (7) Galeed reconstructability, (8) simplification (remove needless mechanisms / duplicates of what exists),
(9) re-score. Convergence = a pass adding no material new high-risk failure mode, no material new requirement,
no new state-transition ambiguity.

## Code facts (verified by reading src/hoglah, v0.13.0)
- Status enum: QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED. No attempt counter persisted. Retry loop runs
  in-memory inside one claim (`_execute_with_retries`). Error stored as `str(exc)`.
- Claim: atomic UPDATE QUEUED->PROCESSING with lease_token + lease_expires_at (wall-clock ISO). Heartbeat every
  10s extends 30s lease. Reclaim (`reclaim_stale_leases`) requeues PROCESSING with expired lease -> QUEUED,
  error="Recovered from expired processing lease", no attempt increment, no record of prior dispatch.
- Completion: `set_result(..., lease_token)` fenced on status=PROCESSING AND token. After it: store.get,
  metrics, Galeed emit + record_io, `_deliver` (output file write, HTTP callback on daemon thread, broker publish
  on daemon thread), `_fire_callback`. Generic `except Exception` after any of these writes a token-fenced FAILED
  (no-op since token cleared) but STILL calls `_deliver(FAILED result)`.
- Cancel: get -> unconditional update_status(CANCELLED) -> unconditional set_result(CANCELLED).
- Poll: list QUEUED due rows ORDER BY priority DESC LIMIT 10 (50 when affinity_reorder [default True] / fairness /
  preload). Rows in "wait" (depends_on / label sequence) still occupy the window.
- All store + Galeed calls are synchronous on the worker event loop. Galeed MongoClient serverSelectionTimeoutMS=2000;
  Galeed failures logged at DEBUG only. Mongo store MongoClient default (30s server selection).
- OpenAICompatAdapter.run is `async` but calls blocking urllib on the loop (socket timeout = timeout_seconds or 120s).
- ollama.AsyncClient timeout=None. Ollama adapter auto-pulls a missing model inside the job.
- CliAdapter: create_subprocess_exec(argv, start_new_session=True), stdin NOT redirected, prompt as last positional
  argv element (no `--`), no default timeout (only optional job timeout_seconds), on cancel SIGTERM to process group
  then SIGKILL to leader only after 2s; `communicate()` buffers all output; error = f"{name} CLI exited {rc}:
  {(stderr or stdout)[:800]}"; exit 0 => success even with empty stdout; streaming path decodes each 256-byte read
  independently with errors="replace" and the stored output is the join of decoded chunks.
- `hoglah run`: only KeyboardInterrupt handled; no SIGTERM handler; never checks worker-thread liveness.
- close(): drain 2s then cancel in-flight (left PROCESSING for reclaim after lease expiry).
- Named callbacks redelivered at EVERY startup (any instance, incl. start_worker=False) for the first 200 rows of
  list() (ORDER BY priority DESC, created_at ASC = oldest), no delivered marker; exceptions swallowed.
- Direct callable registered in `_direct_callbacks` AFTER enqueue commit and after Galeed emit.
- Bridges: ingress enqueue(correlation_id UNIQUE) then ack. On handler exception: log "not acking (will retry)" and
  continue polling. Kafka commit(message) commits offset+1 (implicitly all earlier). Rabbit prefetch default 1, no
  consumer reconnect. Redis PEL drained only once at startup, no XAUTOCLAIM. Egress outbox (`result_published`)
  re-swept ONLY at process startup.
- Stream ChunkWriter.open/write_chunk raise on OSError -> fails the job.
- record_io (default on when Galeed enabled) stores full prompt/output; JobResult.metadata = request.metadata
  merged (per-job `metadata.api_key` for OpenAI adapter would flow into results/files/callbacks).
- RetryPolicy defaults: max_retries 2, base 1s, factor 2, max 10s, jitter 0; classify_error = status attr, then
  regex on str(exc) incl. `\b([45]\d{2})\b` anywhere, `\bconnection\b`, discard transient on `\binvalid\b`.
- Env dict/list settings (HOGLAH_CLI_BINARIES=claude=/p, HOGLAH_MODEL_SLOTS=a=1, HOGLAH_TAG_SLOTS, HOGLAH_OLLAMA_HOSTS=a,b)
  as documented crash pydantic-settings (only JSON works).

## Empirical evidence (all reproduced 2026-10-01; scripts exist)
E01 Dependency HOL deadlock: 60 priority-10 children depending on a priority-0 parent submitted last -> all 61 QUEUED
    forever, no log/event. (10 children: fine — window 50.)
E02 Cancel/complete race (window widened by stalling cancel between read and write): caller callback got
    `completed`; store final `cancelled`, stored output destroyed; output file `cancelled`.
E03 One injected heartbeat OperationalError -> heartbeat task dies silently -> lease expires -> reclaimed -> job
    executed TWICE; final `completed`; nothing logged at WARNING.
E04 One store.get failure right after successful set_result -> store `completed`, output file `failed`
    ("Unexpected worker error"), named callback never fired.
E05 Galeed enabled, Mongo unreachable: each submit() blocks 2.08s; job completed during that block and the direct
    callback was LOST; 3 stub jobs (30ms each) took 12.6s end-to-end (event loop blocked ~4s/job). No INFO log.
E06 OpenAI-compatible endpoint answering after 12s: worker event loop frozen 12.1s (no heartbeats/cancel/poll).
    With timeout_seconds=3 it is the socket timeout, not asyncio.wait_for, that ends it (loop frozen 3.1s).
E07 Fake CLI: streaming on + Hebrew output -> COMPLETED with 601 U+FFFD chars (stored output corrupted; intact
    without streaming). exit0+no stdout -> COMPLETED ''. Answer only on stderr exit0 -> COMPLETED ''.
    partial stdout + SIGSEGV -> FAILED, error text = the partial answer, signal unnamed.
E08 Real CLIs, prompt "--version": claude 2.1.287 and codex 0.157.1 exit 0, Hoglah would mark COMPLETED with the
    version string; grok 1.0.44 rc=2.
E09 Real CLIs with stdin an open pipe: codex exec prints "Reading additional input from stdin..." and blocks until
    killed (documented: piped stdin is appended); claude waits 3s then proceeds; grok fine.
E10 Real CLIs unauthenticated (isolated HOME): claude rc=1 "Not logged in" on STDOUT; grok rc=1 device-code hint on
    stderr; codex retries internally 5x then rc=1 401. Bad model: claude error on stdout (stderr held only a
    warning, so Hoglah's message would be the warning); grok stderr. Hoglah classify_error => [] for ALL of these.
E11 classify_error false positives: "num_ctx=512 exceeded" -> server/transient; "a connection between ideas" ->
    transient; "returned 503 tokens" -> transient.
E12 SIGKILL worker mid CLI job: CLI child still alive (own session); row PROCESSING; restarted worker reclaimed
    after ~16s (10s lease) and RAN AGAIN despite max_retries=0: 2 side effects; row shows nothing about prior run.
E13 Poison job (kills its worker): 4 worker runs -> 4 executions, max_retries=0 ignored, row still PROCESSING.
E14 SIGTERM to `hoglah run`: exit 143 immediately, no drain/close, CLI child orphaned & alive, row PROCESSING.
E15 Named callbacks: 205 jobs; each restart (even start_worker=False) re-fired 200; newest 5 never included.
E16 Stream ENOSPC on chunk -> job FAILED "[Errno 28]"; read-only stream dir -> FAILED "Unexpected worker error".
E17 Peer with clock +60s ran reclaim -> live freshly-heartbeated job reclaimed -> executed twice.
E18 Kafka-semantics fake transport: transient store error on msg c1 -> "not acking (will retry)", c2 committed
    offset 3 -> c1 permanently lost (not in store, not DLT, not redelivered after restart).
E19 Ollama 0.23.4 raw: missing model 404; invalid name 400; temperature:"hot" -> HTTP 500 (caller error as 5xx);
    empty prompt -> 200 done_reason "load", response ""; embed-only model generate -> 400; num_ctx=128 with
    1774-token prompt -> 200, prompt_eval_count=128 (silent input truncation), done_reason "length" (from num_predict).
E20 Ollama via Hoglah: missing model -> auto-pull attempted (registry), FAILED 1 attempt; bad option -> 3 attempts;
    host unreachable -> 3 attempts in 3.1s total then FAILED (retry budget << daemon restart time); empty prompt ->
    COMPLETED ''.
E21 200 KiB prompt to CLI backend -> FAILED "[Errno 7] Argument list too long" (100 KiB ok).
E22 Documented env formats for dict/list settings crash at startup.
Docs: Claude Code headless doc: failures inside the run print on stdout; SIGTERM -> 143, turn left unfinished;
`stream-json` emits system/api_retry with error category (authentication_failed, billing_error, rate_limit,
overloaded, invalid_request, model_not_found, server_error, ...); without --bare, -p runs workdir hooks/.mcp.json
even in untrusted folders; --permission-prompts none exists (>=2.1.259). Codex exec: no documented exit codes,
--json emits turn.failed/error, reads piped stdin, no timeout. Gemini CLI (not installed): exit 0,1,41 auth,42 input,
44 sandbox,52 config,53 turn limit; quota exhaustion can hang 3+ min (issue #10925); headless prints 429 text during
successful retries (#17906). Kiro (not installed): exit 0/1/3(MCP); --no-interactive denies tool approval -> tool
calls fail (#11594); 2.25.0 printed "Trust All Tools active" footer instead of reply (cao#832). Grok has
--prompt-file, --max-turns, json output. Ollama FAQ: overload -> 503 (OLLAMA_MAX_QUEUE 512); insufficient memory ->
requests queue until model loads; default ctx 4096. WSL2: clock lags minutes-hours after sleep then jumps forward
(microsoft/WSL#10006, #4179). Kafka: committing an offset marks all smaller offsets processed (Confluent).

## Pass-1 draft requirement set (to be attacked)
R1  ROB-STATE-001 Persist per-job attempt records (attempt_no, phase claimed|dispatched|returned|committed,
    started/ended, outcome succeeded|failed|unknown|cancelled, failure{category, retryable, provider_code, exit_code,
    signal, http_status, exception_class, excerpt}, decision{action, delay}); attempt_no incremented in the SAME
    atomic statement as claim.
R2  ROB-STATE-002 Retry count & max_retries enforced across process lifetimes (reclaim counts as an attempt when
    phase>=dispatched); poison bound: after max attempts -> FAILED category=attempts_exhausted.
R3  ROB-AMBIG-001 Reclaim of a job whose last attempt phase is dispatched/returned => attempt outcome=unknown.
    Auto re-run only if job is idempotent-class (default: ollama/openai/stub inference = rerunnable; CLI agent
    backends = NOT rerunnable by default) else FAILED category=outcome_unknown, requeue-able by operator.
R4  ROB-LEASE-001 Heartbeat loop must survive transient store errors (retry within remaining lease); if lease cannot
    be renewed before (expiry - margin, monotonic), abort provider work and record lease_lost.
R5  ROB-LEASE-002 Reclaim requires expiry observed on two polls separated by >= lease_seconds of monotonic time
    (defeats wall-clock forward jumps / skew); Mongo uses server time ($$NOW) for lease arithmetic.
R6  ROB-LOOP-001 No blocking I/O on the worker event loop: store and Galeed calls via a bounded executor;
    OpenAI adapter via asyncio.to_thread or async client; heartbeats on a path that cannot be starved.
R7  ROB-CORE-001 Post-commit path: after set_result succeeds nothing may change the delivered outcome; exceptions
    after commit are logged as delivery/observability failures, never converted to FAILED delivery.
R8  ROB-CORE-002 Cancel must be a single conditional write (status IN (QUEUED, PROCESSING)) that never overwrites a
    terminal row; cancel returns false if it lost.
R9  ROB-SCHED-001 Poll must not let waiting rows starve claimable ones: exclude/skip waiting rows in SQL or page
    through; detect jobs QUEUED-and-due older than threshold (stuck metric + Galeed event).
R10 ROB-DELIV-001 Durable delivery outbox (one table/record per sink: output_file, http_callback, named_callback,
    broker) with state pending|delivered|failed, attempts, delivery_id; swept periodically by the worker (not only
    at startup); HTTP POST carries Idempotency-Key/delivery id; direct callables registered BEFORE enqueue.
R11 ROB-DELIV-002 Output file: unique tmp name, fsync file + dir, then rename.
R12 ROB-BRIDGE-001 Ingress failure handling must not skip: Kafka seek back to failed offset + backoff (or stop
    consumer); Rabbit nack(requeue=True) with backoff + consumer reconnect; Redis XAUTOCLAIM idle pending periodically.
R13 ROB-CLI-001 Subprocess hygiene: stdin=DEVNULL (or prompt via stdin/file), prompt never parsed as option
    (`--` or --prompt-file/stdin), default per-backend timeout, kill process group SIGTERM->SIGKILL(group),
    persist pid/pgid/boot_id/start_time so a recovering worker can reap orphans on the same boot before rerun.
R14 ROB-CLI-002 CLI result validation: structured output modes (claude json/stream-json, codex --json, gemini json +
    exit codes, grok json) parsed by per-backend classifiers; exit0 + empty/invalid payload = protocol_error not
    success; error excerpt built from both streams, sanitized, tail-preserving; streaming uses incremental UTF-8 decoder.
R15 ROB-CLASS-001 Canonical taxonomy (cause) + action + job-state fields; classify from structured signals first;
    regex fallback only on provider-originated error text, never on whole message containing prompt/model; unknown
    => bounded conservative handling.
R16 ROB-RETRY-001 RetryPolicy extended: per-category defaults, max_elapsed budget, Retry-After / reset times, jitter
    default >0, retry state persisted (next_attempt_at via run_at so retries survive restart and do not hold slots).
R17 ROB-HEALTH-001 Per-backend circuit breaker: consecutive provider_unavailable/network/auth failures open the
    breaker; queued jobs for that backend deferred (not failed, attempt not consumed); half-open single probe job;
    state exported to metrics/Galeed.
R18 ROB-LOG-001 Galeed events for: attempt start/end with category, retry scheduled, lease reclaimed/lost,
    outcome_unknown, delivery attempt/failure, breaker transitions, recovery at startup; non-blocking bounded queue
    emitter; drops counted; local JSONL spool fallback replayed with event ids; Galeed failure logged at WARNING
    (rate-limited) not DEBUG.
R19 ROB-STREAM-001 Diagnostic stream failures never change job outcome (disable stream for that job, log).
R20 ROB-SHUT-001 SIGTERM/SIGINT handled: stop claiming, bounded drain, terminate CLI groups, close; worker-thread
    liveness checked by `hoglah run` (exit non-zero if dead).
R21 ROB-OLLAMA-001 Default client timeouts (connect/read-idle); detect input truncation via prompt_eval_count vs
    num_ctx; empty/`load` responses not success; auto-pull opt-in, not inside job attempts by default.
R22 ROB-SEC-001 Redact secrets (metadata.api_key etc.) from results/files/callbacks/Galeed/stderr excerpts; CLI
    workdir-trust (--bare for claude) and broker-selected CLI backends behind an allow-list.
R23 ROB-CONF-001 Documented env formats accepted (k=v, comma lists) or docs corrected; config validation at startup.
R24 ROB-TEST-001 Fault-injection suite (the E01–E22 scenarios as deterministic tests), gated real-provider smoke tests,
    restart tests with real process kill, state-machine property tests.

## Required output of each iteration
Revised requirement set (add/merge/delete with IDs), new failure modes found by passes 2-8 (with which pass found
them), and the score vector.
E23 (verified by parent after iteration 0): transient store error in the pre-commit race-guard get() after provider SUCCESS -> job persisted FAILED 'Unexpected worker error' (provider called once). Confirms iteration-0 finding C2/N10.
E25 (verified by parent after iteration 3): `hoglah clear` with 3 QUEUED jobs, answering 'n' at the prompt -> prints 'Clear cancelled.' yet all 3 rows deleted (delete runs before typer.confirm; no --status deletes non-terminal rows). Confirms C24/N32.
E26 (verified by parent during iteration 4): SQLite row with corrupt request_json + 3 healthy queued jobs -> healthy jobs never claimed for 5s, 'Error in worker loop' logged every second, stats() still reports 4 queued (looks merely slow). Confirms C18/N37 on SQLite. N42 confirmed in code: messaging_submitter.py:120 mints uuid4 per submit, :130 sets job timeout_seconds=int(wait timeout), :138 raises on no-result-within-timeout.
