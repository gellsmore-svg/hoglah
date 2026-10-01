# Failure catalogue

Deliverables **C** (comprehensive failure catalogue) and **E** (current-code
weaknesses) of the 2026-10-01 robustness analysis of Hoglah v0.13.0
(`f0d322a`). Index: [README.md](README.md).

The machine-readable catalogue, [`failure-catalog.yaml`](failure-catalog.yaml),
carries the full record schema for every entry: component, operation,
trigger, basis, preconditions, mechanism, current behaviour, persistent state
after failure, external visibility, duplicate/lost/corrupt/stuck flags,
category, retryability, ambiguity, current and required logging, current and
proposed recovery, risk vector, test method, evidence, and requirement IDs.
The tables below are generated from the same data, so the two cannot drift.

## How to read the tables

**Basis.**

| Label | Meaning |
|---|---|
| *observed (E##)* | Reproduced here; see [experiments/](experiments/README.md) |
| *code* | Confirmed by reading the cited lines |
| *documented* / *issue* | Primary source |
| *inference* | Strongly supported by code plus documented semantics |
| *theoretical* | Plausible, unreproduced |

**Effects.**

| Flag | Meaning |
|---|---|
| dup | duplicate execution or side effect |
| lost | lost result |
| corrupt | corrupted or contradictory state |
| stuck | job can remain non-terminal indefinitely |

A trailing `?` means possible rather than certain.

**Risk vector** (0–5 per axis). The vector is authoritative.

| Axis | Meaning |
|---|---|
| **P** | probability / frequency |
| **I** | impact |
| **S** | silent-failure risk |
| **C** | state-corruption risk |
| **D** | duplicate-side-effect risk |
| **R** | difficulty of recovery |
| **O** | observability deficiency |
| **X** | cross-component propagation |

**Agg** is a prioritisation aid only: `0.5P + I + 1.5S + 1.5C + D + 0.5R + 0.5O + 0.5X`. Silent failure and corruption are weighted up, as the brief requires.

Scores are the analyst's judgement, refined over the eight-iteration recursive
loop recorded in [convergence/](convergence/). A high aggregate on a
*theoretical* entry (FM-STORAGE-001) reflects impact if it happened, not
evidence that it does. FM-CLI-* rows apply to every CLI backend.
[PROVIDER-FAILURES.md](PROVIDER-FAILURES.md) keeps the provider-specific
detail, versions and sources.

## C. Catalogue

### Core execution and state machine

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-CORE-001 | more than poll_limit (50; 10 without affinity) higher-priority QUEUED rows waiting on depends_on/label order | observed (E01) | all jobs stay QUEUED forever | stuck | `2 5 5 1 0 2 5 3` | 20.0 | ROB-SCHED-001 |
| FM-CORE-002 | cancel() reads PROCESSING, worker commits COMPLETED, cancel writes | observed (E02) | COMPLETED result overwritten with CANCELLED; output destroyed | lost, corrupt | `1 4 4 5 0 4 4 2` | 23.0 | ROB-CORE-002, ROB-STATE-003 |
| FM-CORE-003 | one transient store exception in heartbeat (e.g. SQLite 'database is locked' > busy_timeout, Mongo blip) | observed (E03) | job executes twice; first result discarded (token mismatch) | dup, lost? | `3 4 5 2 5 2 5 3` | 26.0 | ROB-LEASE-001 |
| FM-CORE-004 | any exception after successful set_result (store.get, thread start, metrics) | observed (E04) | output file/HTTP/broker receive FAILED for a COMPLETED job; named callback never fires | corrupt | `2 4 4 4 0 3 3 4` | 22.0 | ROB-CORE-001, ROB-DELIV-001 |
| FM-CORE-005 | transient store error in store.get between provider success and set_result | observed (E23) | provider success persisted as FAILED 'Unexpected worker error'; side effect already happened | dup?, lost, corrupt | `2 4 3 4 4 3 3 2` | 23.5 | ROB-CORE-001 |
| FM-CORE-006 | lease reclaimed or cancelled while the provider ran; set_result returns False | code (C1) | computed output silently discarded | dup?, lost | `2 3 4 1 3 2 4 1` | 18.0 | ROB-LEASE-001 |
| FM-CORE-007 | worker dies after claim; job reclaimed | observed (E12) | re-executed regardless of max_retries; history erased | dup | `3 4 4 2 5 3 4 2` | 24.0 | ROB-STATE-001, ROB-AMBIG-001 |
| FM-CORE-008 | job whose execution kills the worker (OOM-kill, segfault in-process, deliberate) | observed (E13) | unbounded executions; every crash also interrupts and reruns other in-flight jobs | dup, lost?, stuck | `2 5 3 2 5 4 4 5` | 25.0 | ROB-STATE-001 |
| FM-CORE-009 | worker dies after dispatching to a provider (esp. CLI agent with side effects) | observed (E12, E14) | non-idempotent agent work silently re-run | dup | `3 5 5 2 5 3 5 2` | 27.0 | ROB-AMBIG-001, ROB-STATE-003 |
| FM-CORE-010 | service manager stop, terminal close, wsl --shutdown, Windows Update | observed (E14) | in-flight jobs left PROCESSING; CLI children orphaned and still running | dup?, lost?, stuck? | `4 4 3 2 4 3 3 3` | 22.0 | ROB-SHUT-001, ROB-CLI-001 |
| FM-CORE-011 | worker thread exits (unexpected BaseException, loop.close) | code | process looks healthy, processes nothing | stuck | `1 4 5 0 0 1 5 2` | 16.0 | ROB-SHUT-001 |
| FM-CORE-012 | slow store (SQLite busy, Mongo 30s server selection), Galeed down, OpenAI request | observed (E05, E06) | every in-flight job's heartbeat, poll and cancel freeze; leases can lapse fleet-wide | dup | `3 4 4 1 4 2 4 5` | 22.5 | ROB-LOOP-001 |
| FM-CORE-013 | peer clock skew > lease, or local clock jumps forward > lease (WSL2 after sleep) | observed (E17); documented (microsoft/WSL#10006) | live jobs reclaimed and executed twice | dup, lost? | `3 4 5 1 5 2 4 3` | 24.0 | ROB-LEASE-002 |
| FM-CORE-014 | store error while listing a label group | code (C11) | later label dispatched before earlier label completes | corrupt | `2 4 4 4 2 3 4 2` | 23.5 | ROB-SCHED-001 |
| FM-CORE-015 | store error reading the PROCESSING snapshot | code (C20) | slot oversubscription, possible GPU OOM failing other jobs | — | `2 3 4 1 1 2 4 4` | 17.5 | ROB-SCHED-001 |
| FM-CORE-016 | one row with undecodable request_json (manual edit, partial write, version skew) | observed (E26) | no job is ever claimed; stats() looks normal | stuck | `1 5 4 2 0 3 3 4` | 19.5 | ROB-SCHED-001 |
| FM-CORE-017 | one transient store error in _cancel_watch_loop | code (C22) | cancelled job keeps running to completion (result then dropped) | — | `2 3 4 0 2 1 4 1` | 15.0 | ROB-LEASE-001, ROB-CORE-002 |
| FM-CORE-018 | parent FAILED/CANCELLED/outcome_unknown, children already failed dependency_blocked; operator requeues parent | code (C8, C12) | pipeline silently half-revived | stuck | `2 3 3 1 0 2 3 2` | 13.5 | ROB-AMBIG-001, ROB-STATE-003 |
| FM-CORE-019 | operator requeue | code (C6) | previous failure evidence lost; COMPLETED can be reopened by library callers | — | `3 2 4 2 1 2 4 1` | 17.0 | ROB-STATE-001, ROB-STATE-003 |
| FM-CORE-020 | retryable failure; backoff in progress | code (C9) | slot occupied by a sleeping job; restart resets the retry budget | — | `4 2 2 1 1 1 2 2` | 12.0 | ROB-RETRY-001, ROB-STATE-001 |
| FM-CORE-021 | stream_dir full (ENOSPC), unwritable, or removed | observed (E16) | successful inference FAILED | lost | `2 3 2 2 0 1 2 3` | 13.0 | ROB-CORE-001 |
| FM-CORE-022 | any exception outside the adapter attempt loop | observed (E16b) | operator cannot tell why | — | `3 2 3 0 0 2 4 1` | 11.5 | ROB-CLASS-001, ROB-STATE-001 |
| FM-CORE-023 | store unavailable while recording a failure | code | job retried later with no record | dup? | `2 2 4 1 3 2 4 1` | 17.0 | ROB-LEASE-001, ROB-CORE-001 |
| FM-CORE-024 | store unavailable | code (C23) | monitoring blind when most needed | — | `2 3 4 0 0 1 5 2` | 14.0 | ROB-LOOP-001 |
| FM-CORE-025 | large queue; preload enabled | code (C19, C21) | loop time grows with queue size | — | `2 2 3 0 1 1 3 3` | 12.0 | ROB-LOOP-001, ROB-SCHED-001 |
| FM-CONF-001 | documented formats HOGLAH_CLI_BINARIES=claude=/p, HOGLAH_MODEL_SLOTS=a=1, HOGLAH_TAG_SLOTS, HOGLAH_OLLAMA_HOSTS=a,b | observed (E22) | startup crash (SettingsError) | — | `3 2 0 0 0 1 1 1` | 5.0 | ROB-CONF-001 |

### Retry and classification

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-RETRY-001 | error text containing numbers 400-599, 'connection', 'invalid', etc. (often prompt/model fragments) | observed (E11) | permanent errors retried / transient not retried depending on wording | dup? | `3 2 3 0 2 1 3 1` | 12.5 | ROB-CLASS-001 |
| FM-RETRY-002 | Ollama/OpenAI host down for longer than ~3 s (restart, model swap, WSL host change) | observed (E20) | every queued job for that backend fails permanently | — | `4 4 1 0 0 2 2 3` | 11.0 | ROB-RETRY-001 |
| FM-RETRY-003 | Ollama rejects an option type with HTTP 500 | observed (E19, E20) | permanent caller error retried 3x | dup | `3 1 1 0 1 0 2 1` | 6.5 | ROB-CLASS-001, ROB-BACKEND-001 |
| FM-RETRY-004 | any exception the classifier does not recognise (all CLI exits today) | observed (E10) | no cause recorded; transient CLI failures never retried | — | `4 2 3 0 0 2 4 1` | 12.0 | ROB-CLASS-001 |

### Delivery

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-DELIV-001 | any Hoglah construction with a callbacks registry | observed (E15) | already-delivered callbacks repeat every restart; recent missed ones are never redelivered | dup, lost | `4 3 4 1 4 2 4 2` | 20.5 | ROB-DELIV-001 |
| FM-DELIV-002 | job completes before submit() registers the callable (Galeed emit blocking, fast worker) | observed (E05) | callback never fires | lost | `2 3 5 0 0 2 5 1` | 15.5 | ROB-DELIV-001 |
| FM-DELIV-003 | process exits soon after completion; receiver slow; ack lost | code + inference | callback lost on exit; duplicate POST on ack loss with no idempotency key | dup, lost | `3 3 4 0 3 2 3 2` | 17.0 | ROB-DELIV-001 |
| FM-DELIV-004 | power loss / WSL termination after rename; concurrent writers | code + theoretical | zero-length or missing file after crash; tmp collision | lost, corrupt | `1 3 3 2 0 2 3 1` | 14.0 | ROB-DELIV-001 |
| FM-DELIV-005 | `hoglah cancel` (CLI), cascades, start_worker=False callers | code (N43) | cancel notifications lost | lost | `3 3 4 0 0 2 4 2` | 14.5 | ROB-DELIV-001 |

### Administration, batches and idempotency

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-ADMIN-001 | user runs `hoglah clear` and answers no | observed (E25) | jobs deleted while the tool says 'Clear cancelled.' | lost, corrupt | `3 5 5 5 0 5 4 3` | 27.5 | ROB-ADMIN-001 |
| FM-ADMIN-002 | clear without filter; remove(cancel=False); retention scripts (SQLite and Mongo) | code (C24) | running jobs lose their row; children blocked 'not found'; outbox results never published | lost, corrupt, stuck | `2 5 4 5 1 5 4 4` | 27.0 | ROB-ADMIN-001, ROB-STATE-003 |
| FM-ADMIN-003 | idempotency_key / correlation_id resubmitted after the row was deleted | code (N41) | duplicate job | dup | `2 3 3 0 4 2 3 2` | 16.0 | ROB-ADMIN-001 |
| FM-ADMIN-004 | operator or cron bulk requeue | code (N25, N47) | non-idempotent agents rerun; workers re-poisoned | dup | `2 4 3 1 5 2 3 3` | 20.0 | ROB-AMBIG-001 |
| FM-BATCH-001 | crash or error midway through submit_batch | code (C13, N26) | running prefix whose children never exist; compensation may cancel dispatched work | dup?, corrupt, stuck? | `2 3 4 3 2 3 3 2` | 20.5 | ROB-BATCH-001 |
| FM-BATCH-002 | batch item idempotency_key matches an existing job, then a later item fails | code (C25, N33) | pre-existing (even COMPLETED) jobs deleted; children point at never-persisted ids | lost, corrupt, stuck | `1 5 4 5 0 4 4 4` | 25.0 | ROB-BATCH-001 |

### Messaging bridges

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-BRIDGE-001 | transient store error while enqueuing message N | observed (E18, modelled transport); documented Kafka commit semantics | message silently lost (not in store, not DLT, not redelivered) | lost | `2 5 5 1 0 4 4 3` | 20.5 | ROB-BRIDGE-001 |
| FM-BRIDGE-002 | DLT produce fails for a poison message | inference (same mechanism as FM-BRIDGE-001) | poison message lost | lost | `2 3 4 0 0 3 4 2` | 14.5 | ROB-BRIDGE-001 |
| FM-BRIDGE-003 | one un-acked message with prefetch=1; broker connection drop | inference (code: no consumer reconnect; prefetch default 1) | ingress stalls until process restart | stuck | `2 4 3 0 0 2 3 2` | 13.0 | ROB-BRIDGE-001 |
| FM-BRIDGE-004 | handler failure at runtime; consumer renamed or dead | code | failed entries wait for a restart; a dead consumer's entries are never claimed | lost, stuck | `2 3 4 0 0 2 3 1` | 13.0 | ROB-BRIDGE-001 |
| FM-BRIDGE-005 | live publish fails while the process keeps running | code | result unpublished until the next restart (days for a daemon) | stuck | `3 4 4 0 0 2 3 2` | 15.0 | ROB-DELIV-001 |
| FM-BRIDGE-006 | submitter wait expires or publish unacknowledged; caller retries | code (N42) | job executed twice (no bridge dedupe possible) | dup | `3 4 3 0 5 2 3 2` | 18.5 | ROB-BRIDGE-001 |
| FM-BRIDGE-007 | retry with the same correlation id after an unknown outcome (Rabbit, Redis) | code (N48) | caller never receives a completed result | lost | `2 3 4 0 1 2 3 2` | 14.5 | ROB-BRIDGE-001, ROB-DELIV-001 |
| FM-BRIDGE-008 | results republished after switching cluster/URL | code (N53) | result delivered to the wrong cluster; caller never receives it | lost | `1 3 5 1 0 3 4 1` | 16.5 | ROB-DELIV-001 |

### Galeed, security and storage

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-GALEED-001 | galeed_enabled and its Mongo unreachable/slow | observed (E05) | submit() blocks 2 s; ~4 s loop block per job; heartbeat starvation; lost direct callbacks | dup?, lost | `3 4 4 0 3 1 4 5` | 19.5 | ROB-LOG-001, ROB-LOOP-001 |
| FM-GALEED-002 | any Galeed failure | code | central observability silently absent | — | `3 2 5 0 0 1 5 1` | 14.5 | ROB-LOG-001 |
| FM-GALEED-003 | any retry, reclaim, lease loss, delivery failure | code | retries, reclaims, ambiguous outcomes and delivery failures invisible centrally | — | `5 2 4 0 0 1 5 1` | 14.0 | ROB-LOG-001 |
| FM-GALEED-004 | cancel | code (N46) | cancels misread as completions | — | `5 1 3 0 0 0 2 1` | 9.5 | ROB-LOG-001 |
| FM-SEC-001 | per-job metadata.api_key (OpenAI adapter) or other secrets in metadata | observed (E24) | secret in result_json, output file (twice), callback POST, broker result, record_io | — | `3 4 4 0 0 3 3 3` | 16.0 | ROB-SEC-001 |
| FM-SEC-002 | job workdir contains .claude/settings.json hooks or .mcp.json; broker message selects a CLI backend | documented (Claude headless docs); code | workdir content executes; remote message triggers a local agent | dup | `2 4 2 2 3 3 3 3` | 18.5 | ROB-SEC-001 |
| FM-STORAGE-001 | db_path under /mnt/c (drvfs/9p) with WAL | theoretical (SQLite WAL needs shared-memory locking) | corruption or lock errors | dup?, lost?, corrupt | `1 5 3 5 2 4 3 3` | 24.5 | ROB-CONF-001 |
| FM-STORAGE-002 | network error after the primary applied set_result | inference | contradictory delivery (same shape as FM-CORE-004) | corrupt | `1 4 3 4 0 3 3 3` | 19.5 | ROB-CORE-001 |

### Provider adapters (summary; detail in PROVIDER-FAILURES.md)

| ID | Failure | Basis | Current behaviour | Effects | P I S C D R O X | Agg | Requirements |
|---|---|---|---|---|---|---|---|
| FM-CLI-001 | worker started with an open stdin pipe | observed (codex E09); documented | job PROCESSING forever, heartbeating | stuck | `3 5 5 0 0 2 5 2` | 18.5 | ROB-CLI-001 |
| FM-CLI-002 | interactive prompt, hung network, quota hang (gemini #10925) | code + issue | PROCESSING forever unless timeout_seconds set | stuck | `3 4 5 0 0 2 5 2` | 17.5 | ROB-CLI-001 |
| FM-CLI-003 | prompt beginning with '-' | observed (E08) | COMPLETED with wrong payload | — | `2 3 4 2 0 1 4 1` | 16.0 | ROB-CLI-001, ROB-BACKEND-001 |
| FM-CLI-004 | prompt > 128 KiB | observed (E21) | FAILED unclassified | — | `2 3 1 0 0 1 2 0` | 7.0 | ROB-CLI-001 |
| FM-CLI-005 | worker killed (SIGKILL/SIGTERM/WSL stop) | observed (E12, E14) | orphan keeps editing workdir; job rerun | dup | `3 5 4 3 5 3 4 3` | 27.0 | ROB-CLI-001, ROB-AMBIG-001 |
| FM-CLI-006 | grandchildren ignore SIGTERM on cancel/timeout | code + theoretical | MCP servers/shells survive | — | `2 3 4 1 2 2 4 2` | 17.5 | ROB-CLI-001 |
| FM-CLI-007 | exit 0 with empty stdout / answer only on stderr | observed (E07) | COMPLETED '' | lost | `3 4 5 2 0 2 5 2` | 20.5 | ROB-BACKEND-001 |
| FM-CLI-008 | failure printed on stdout while stderr has a warning | observed (E10); documented | recorded cause is the warning | — | `3 2 4 0 0 2 4 1` | 13.0 | ROB-BACKEND-001 |
| FM-CLI-009 | streaming on; multi-byte UTF-8 split across reads | observed (E07) | COMPLETED with U+FFFD corruption | lost | `4 4 5 4 0 3 5 2` | 24.5 | ROB-BACKEND-001 |
| FM-CLI-010 | killed by signal / segfault / OOM-kill | observed (E07) | FAILED; signal unnamed; partial answer used as error | — | `2 2 3 0 0 1 3 1` | 10.0 | ROB-BACKEND-001 |
| FM-CLI-011 | any non-zero exit | observed (E10) | never retried; cause not recorded | — | `5 2 3 0 0 2 4 1` | 12.5 | ROB-CLASS-001, ROB-BACKEND-001 |
| FM-CLI-012 | CLI upgrade changes flags or output | documented (frequent releases); issue (kiro 2.25.0) | unclassified failure or wrong success | — | `3 3 4 1 0 2 4 2` | 16.0 | ROB-BACKEND-001 |
| FM-CODEX-001 | missing/expired credentials | observed | long attempt, FAILED unclassified | — | `2 2 2 0 0 1 3 1` | 8.5 | ROB-BACKEND-001 |
| FM-CLAUDE-001 | SIGTERM mid-turn | documented | treated as cancel/killed; tool side effects unknown | — | `3 3 3 1 3 2 3 2` | 17.0 | ROB-AMBIG-001 |
| FM-CLAUDE-002 | workdir with project hooks / .mcp.json | documented | workdir content executes | — | `2 4 3 2 2 3 3 3` | 19.0 | ROB-SEC-001 |
| FM-GEMINI-001 | auth/input/sandbox/config/turn-limit failure | documented | all unclassified | — | `3 2 3 0 0 1 3 1` | 10.5 | ROB-BACKEND-001 |
| FM-GEMINI-002 | 429 text printed during a run that succeeds | issue #17906 | text classifiers would misfire | — | `3 1 2 0 0 0 2 0` | 6.5 | ROB-CLASS-001 |
| FM-KIRO-001 | tool approval needed in --no-interactive | issue #11594 | COMPLETED with work not done | — | `3 4 5 2 0 2 5 2` | 20.5 | ROB-BACKEND-001 |
| FM-KIRO-002 | kiro 2.25.0 output | issue (awslabs/cli-agent-orchestrator#832) | COMPLETED with wrong payload | — | `2 4 5 2 0 2 5 2` | 20.0 | ROB-BACKEND-001 |
| FM-OLLAMA-001 | daemon down / restarting / host address changed | observed (E20) | 3 attempts in 3.1 s then FAILED for every queued job | — | `4 4 1 0 0 2 2 3` | 11.0 | ROB-RETRY-001 |
| FM-OLLAMA-002 | model not installed | observed (E19, E20) | registry pull inside the job; offline pull retried as connection error | — | `3 3 2 0 0 2 3 2` | 11.0 | ROB-BACKEND-001 |
| FM-OLLAMA-003 | empty prompt | observed (E19) | COMPLETED '' | lost | `2 2 4 1 0 1 4 1` | 13.5 | ROB-BACKEND-001 |
| FM-OLLAMA-004 | prompt longer than num_ctx | observed (E19) | not flagged unless done_reason==length | — | `4 4 5 2 0 2 5 1` | 20.5 | ROB-BACKEND-001 |
| FM-OLLAMA-005 | server stalls (model loading queued for memory) | documented (FAQ) + code (AsyncClient timeout=None) | job hangs unless timeout_seconds set | stuck | `2 4 5 0 0 2 5 2` | 17.0 | ROB-CLI-001 |
| FM-OLLAMA-006 | stream ends without done:true | code + theoretical | partial text COMPLETED | lost | `1 4 5 2 0 2 5 1` | 19.0 | ROB-BACKEND-001 |
| FM-OPENAI-001 | slow or hung server | observed (E06) | whole worker frozen for the call | dup? | `4 5 4 1 4 2 4 5` | 24.0 | ROB-LOOP-001, ROB-CLI-001 |
| FM-OPENAI-002 | SSE without [DONE]/finish_reason; malformed chunk | code | COMPLETED partial | lost | `1 3 5 1 0 2 4 1` | 16.0 | ROB-BACKEND-001 |

### Top 15 by aggregate score

| Rank | ID | Agg | Vector P I S C D R O X | Why it ranks |
|---|---|---|---|---|
| 1 | FM-ADMIN-001 | 27.5 | `3 5 5 5 0 5 4 3` | jobs deleted while the tool says 'Clear cancelled.' |
| 2 | FM-CORE-009 | 27.0 | `3 5 5 2 5 3 5 2` | non-idempotent agent work silently re-run |
| 3 | FM-ADMIN-002 | 27.0 | `2 5 4 5 1 5 4 4` | running jobs lose their row; children blocked 'not found'; outbox results never published |
| 4 | FM-CLI-005 | 27.0 | `3 5 4 3 5 3 4 3` | orphan keeps editing workdir; job rerun |
| 5 | FM-CORE-003 | 26.0 | `3 4 5 2 5 2 5 3` | job executes twice; first result discarded (token mismatch) |
| 6 | FM-CORE-008 | 25.0 | `2 5 3 2 5 4 4 5` | unbounded executions; every crash also interrupts and reruns other in-flight jobs |
| 7 | FM-BATCH-002 | 25.0 | `1 5 4 5 0 4 4 4` | pre-existing (even COMPLETED) jobs deleted; children point at never-persisted ids |
| 8 | FM-STORAGE-001 | 24.5 | `1 5 3 5 2 4 3 3` | corruption or lock errors |
| 9 | FM-CLI-009 | 24.5 | `4 4 5 4 0 3 5 2` | COMPLETED with U+FFFD corruption |
| 10 | FM-CORE-007 | 24.0 | `3 4 4 2 5 3 4 2` | re-executed regardless of max_retries; history erased |
| 11 | FM-CORE-013 | 24.0 | `3 4 5 1 5 2 4 3` | live jobs reclaimed and executed twice |
| 12 | FM-OPENAI-001 | 24.0 | `4 5 4 1 4 2 4 5` | whole worker frozen for the call |
| 13 | FM-CORE-005 | 23.5 | `2 4 3 4 4 3 3 2` | provider success persisted as FAILED 'Unexpected worker error'; side effect already happened |
| 14 | FM-CORE-014 | 23.5 | `2 4 4 4 2 3 4 2` | later label dispatched before earlier label completes |
| 15 | FM-CORE-002 | 23.0 | `1 4 4 5 0 4 4 2` | COMPLETED result overwritten with CANCELLED; output destroyed |

## E. Confirmed current-code weaknesses

Only weaknesses confirmed by reading the implementation (and, where marked,
reproduced) are listed. Line numbers refer to `src/hoglah/` at `f0d322a`.

| # | File · function (lines) | Failure scenario | Why current behaviour is insufficient | FM / vector P I S C D R O X | Requirements |
|---|---|---|---|---|---|
| W1 | `client.py` · `_worker_loop` (1118–1185) | Waiting rows fill the 50-row poll window; parent never fetched (**E01**) | The window is a priority-ordered `LIMIT` over rows that may not be claimable; there is no paging, readiness cache or stuck signal | FM-CORE-001 · `2 5 5 1 0 2 5 3` | ROB-SCHED-001 |
| W2 | `client.py` · `_cancel_one` (932–969) | Cancel overwrites a just-completed job (**E02**) | Read-then-unconditional `update_status` + `set_result` | FM-CORE-002 · `1 4 4 5 0 4 4 2` | ROB-CORE-002 |
| W3 | `client.py` · `_heartbeat_loop` (1393–1415) | One store error kills the heartbeat and the job runs twice (**E03**) | Only `CancelledError` is handled; the `finally` in `_process_job` swallows the task's exception | FM-CORE-003 · `3 4 5 2 5 2 5 3` | ROB-LEASE-001 |
| W4 | `client.py` · `_process_job` (1286–1379) | Post-commit exception delivers FAILED (**E04**); pre-commit read error persists FAILED after success (**E23**); lost-lease result dropped at INFO | One generic `except Exception` covers pre-commit, commit and post-commit phases and ignores whether its own fenced write succeeded | FM-CORE-004/005/006 | ROB-CORE-001, ROB-LEASE-001 |
| W5 | `client.py` · `_execute_with_retries` (2022–2133); `store.py` · `reclaim_stale_leases` (636–674) | Kill after claim: rerun despite `max_retries=0` (**E12**); poison job runs forever (**E13**) | Attempt count lives only in a Python loop; reclaim does not count or mark the interrupted attempt | FM-CORE-007/008/009 · `2 5 3 2 5 4 4 5` | ROB-STATE-001, ROB-AMBIG-001 |
| W6 | `store.py` / `mongo_store.py` · `reclaim_stale_leases` | Peer clock +60 s reclaims a live job (**E17**) | Wall-clock ISO comparison across writers; no monotonic confirmation | FM-CORE-013 · `3 4 5 1 5 2 4 3` | ROB-LEASE-002 |
| W7 | `http_adapter.py` · `OpenAICompatAdapter.run` / `_post` / `_post_sse` | Event loop frozen for the whole request (**E06**) | `async def` wrapping blocking `urllib.request.urlopen` | FM-OPENAI-001 · `4 5 4 1 4 2 4 5` | ROB-LOOP-001 |
| W8 | `tracing.py` · `JobWitness.emit` / `record_io` | Galeed down: `submit()` blocks 2 s, worker loop blocks ~4 s/job, direct callback lost (**E05**) | Synchronous `insert_one` on caller threads; failures at DEBUG | FM-GALEED-001/002, FM-DELIV-002 | ROB-LOG-001, ROB-LOOP-001, ROB-DELIV-001 |
| W9 | `cli_adapters.py` · `CliAdapter.run` (252–331) | stdin hang (**E09**), prompt-as-flag (**E08**), E2BIG (**E21**), UTF-8 corruption when streaming (**E07**), empty success (**E07**), orphaned children (**E12/E14**) | No `stdin=DEVNULL`, no `--`/prompt file, no default timeout, per-chunk decode, exit code as sole success test, leader-only SIGKILL, `start_new_session` without reaping | FM-CLI-001…011 | ROB-CLI-001, ROB-BACKEND-001 |
| W10 | `models.py` · `classify_error` (155–249) | `"num_ctx=512"` retried as server error; every CLI failure → `[]` (**E10/E11**) | Regex over full message text; no provider-specific mapping; no category persisted | FM-RETRY-001/004, FM-CLI-011 | ROB-CLASS-001 |
| W11 | `client.py` · `_redeliver_restart_callbacks` (1043–1069) | 200 old callbacks re-fired every start, newest never (**E15**) | No delivered marker; `list(limit=200)` is oldest-first; runs on submitters; exceptions swallowed with `pass` | FM-DELIV-001 · `4 3 4 1 4 2 4 2` | ROB-DELIV-001 |
| W12 | `client.py` · `_deliver` / `_post_callback` (2208–2313) | HTTP callback lost on exit; duplicate on ack loss; output file not fsync'd | Daemon thread with no persistence or delivery id; fixed `.json.tmp` name | FM-DELIV-003/004 | ROB-DELIV-001 |
| W13 | `kafka_bridge.py` · `_consume_loop` (516–541); `ConfluentKafkaTransport.ack/nack` | Store error on message N, N+1 committed, N lost (**E18**) | "not acking (will retry)" has no seek-back; Kafka commits are cumulative | FM-BRIDGE-001/002 · `2 5 5 1 0 4 4 3` | ROB-BRIDGE-001 |
| W14 | `kafka_bridge.py` · `prime`/`start`/`republish_unpublished` | Live publish fails; result waits for the next process start | Outbox swept only at startup | FM-BRIDGE-005 | ROB-DELIV-001 |
| W15 | `rabbitmq.py` · `poll`; `redis_streams.py` · `poll` | Consumer dead after broker restart; failed entries wait for restart | No consumer reconnect; PEL read once; no `XAUTOCLAIM` | FM-BRIDGE-003/004 | ROB-BRIDGE-001 |
| W16 | `messaging_submitter.py` · `submit` (108–140) | Caller retries a timed-out submit, job runs twice | Fresh `uuid4` per call; timeout reported as failure; job timeout = wait timeout | FM-BRIDGE-006/007 | ROB-BRIDGE-001 |
| W17 | `cli.py` · `clear` (498–522); `store.py`/`mongo_store.py` · `delete_jobs`, `delete_job` | Answering "n" still deletes, including QUEUED jobs (**E25**) | Delete runs before `typer.confirm`; store deletes are unconditional | FM-ADMIN-001/002 · `3 5 5 5 0 5 4 3` | ROB-ADMIN-001 |
| W18 | `client.py` · `submit_batch` (764–807); `batch.py` | Crash mid-batch leaves a running prefix; compensation deletes pre-existing idempotent jobs | Per-item commits; ids assigned before idempotency resolution; compensation by returned id | FM-BATCH-001/002 | ROB-BATCH-001 |
| W19 | `client.py` · `_eval_label_sequence` (1530–1548), `_processing_snapshot`/`_dim_usage` (1787–1865) | Store error ⇒ later label runs early; slot caps ignored | Exceptions return "ready" / zero usage (fail-open) | FM-CORE-014/015 | ROB-SCHED-001 |
| W20 | `store.py` · `_row_to_dict` (333–344) | One corrupt row stops every poll (**E26**) | Unguarded `json.loads` inside `list()` | FM-CORE-016 · `1 5 4 2 0 3 3 4` | ROB-SCHED-001 |
| W21 | `streaming.py` · `ChunkWriter.open/write_chunk`; `client.py` · `_open_stream` | ENOSPC / unwritable stream dir fails the job (**E16**) | Diagnostic side channel raises into the provider call | FM-CORE-021 | ROB-CORE-001 |
| W22 | `cli.py` · `run` (1240–1253) | SIGTERM exits at once, no drain, orphaned children (**E14**) | Only `KeyboardInterrupt` handled; worker-thread liveness never checked | FM-CORE-010/011 | ROB-SHUT-001 |
| W23 | `config.py` · dict/list fields | Documented `k=v` env formats crash startup (**E22**) | pydantic-settings decodes JSON before the field validator | FM-CONF-001 | ROB-CONF-001 |
| W24 | `client.py` · `_execute_with_retries` result construction (2056–2089) | `metadata.api_key` copied into result, file, callback, broker payload (**E24**) | `metadata={**request.metadata, …}` and `parameters=asdict(request)` with no redaction | FM-SEC-001 | ROB-SEC-001 |
| W25 | `adapters.py` · `OllamaAdapter.run`/`_run_stream`/`pull_model` | Silent input truncation, empty `load` success, unbounded wait, in-job auto-pull (**E19/E20**) | Truncation keyed only on `done_reason`; no terminal-chunk check; `AsyncClient` timeout `None`; pull inside the attempt | FM-OLLAMA-002…006 | ROB-BACKEND-001, ROB-CLI-001 |
| W26 | `client.py` · `_cancel_one` (957–964) | Galeed records cancels as `status="completed"` | Literal string mismatch | FM-GALEED-004 | ROB-LOG-001 |

### Recursive-loop identifiers

The [convergence record](convergence/) uses working IDs N1–N58 and C1–C25.
Each confirmed one is folded into an FM entry above (for example N48 is
FM-BRIDGE-007, C24 is FM-ADMIN-002, C11 is FM-CORE-014). The loop's
requirement numbers R1–R27 map to ROB IDs as listed in
[ROBUSTNESS-REQUIREMENTS.md § F](ROBUSTNESS-REQUIREMENTS.md#f-robustness-requirements).
