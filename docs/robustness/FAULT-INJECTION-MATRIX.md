# Fault-injection test matrix

Deliverable **I** of the 2026-10-01 robustness analysis. Index:
[README.md](README.md).

This is the test design for ROB-TEST-001. Each row should become a
deterministic test. The **Repro** column names the script in
[experiments/](experiments/README.md) that demonstrates today's failing
behaviour, which is the starting point for the test. **Expected** columns
describe behaviour *after* the cited requirement is implemented.

**Injection techniques**, all safe and none touching real user data:

| Technique | How it works |
|---|---|
| Store patch | Wrap a `JobStore` method on a live instance to raise, stall, or raise N times. |
| Fake CLI | `fakecli.py` via `cli_binaries`; behaviour selected by prompt. |
| Process kill | `kill -9` / SIGTERM on a separate worker process sharing a temp DB. |
| Fake transport | `MessageTransport` with the target broker's commit semantics. |
| Fake server | `http.server` thread for OpenAI-compatible, SSE and callback receivers. |
| Clock injection | Patch `hoglah.store.datetime` / monotonic source. |
| Filesystem | `chmod` a temp dir; patch `ChunkWriter`/`Path.write_text` to raise `OSError(ENOSPC)`. No real disk is ever filled. |

| # | Failure | Injection | Repro | Expected Hoglah state | Expected retry / recovery | Expected Galeed events | Acceptance condition | Req |
|---|---|---|---|---|---|---|---|---|
| 1 | Waiting rows fill the poll window | 60 high-priority children `depends_on` a low-priority parent submitted last | `e04_hol.py` | parent → COMPLETED, then children | none needed | job.queued ×61, attempt.* | all 61 complete within 10 s | ROB-SCHED-001 |
| 2 | Cancel races completion | stall `cancel()` between read and write | `e05_cancel_race.py` | COMPLETED with output kept | `cancel()` returns False | job.completed only | row status, output and callback all agree on completed | ROB-CORE-002 |
| 3 | Heartbeat store error | `heartbeat` raises `OperationalError` once | `e06_hb.py` | COMPLETED | renewal retried within lease | lease.renew_failed (WARNING) | exactly 1 provider execution | ROB-LEASE-001 |
| 4 | Heartbeat impossible for > lease | `heartbeat` raises continuously | — | attempt `lease_lost` / `unknown`; provider work aborted | per ROB-AMBIG-001 | lease.lost | CLI process group killed; no commit | ROB-LEASE-001 |
| 5 | Store read error after commit | `get` raises once after successful `set_result` | `e07_postcommit.py` | COMPLETED | sink retried | delivery.failed (if any) | every sink says completed; callback fires | ROB-CORE-001 |
| 6 | Store read error before commit | `get` raises once after provider success | `e07b_precommit.py` | COMPLETED | commit retried within lease | — | status completed, 1 provider call | ROB-CORE-001 |
| 7 | Commit write fails transiently | `set_result` raises twice, then succeeds | — | COMPLETED | commit retried | — | 1 provider call, COMPLETED | ROB-CORE-001 |
| 8 | Ambiguous commit (applied, then error) | fake Mongo applies the update then raises | — | COMPLETED (resolved by re-read) | no FAILED delivery | — | sinks say completed | ROB-CORE-001 |
| 9 | Worker killed after claim, before dispatch | `kill -9` between claim and `phase=dispatched` | — | QUEUED → rerun | reclaim after window | lease.reclaimed | attempt[0] phase=claimed; 1 provider call total | ROB-LEASE-002, ROB-STATE-001 |
| 10 | Worker killed after dispatch (CLI) | `kill -9` while the fake CLI sleeps | `e10_kill.sh` | FAILED `outcome_unknown` | none automatic; orphan reaped | attempt.outcome_unknown, recovery.startup | side-effect file has 1 line; requeue refused while orphan alive | ROB-AMBIG-001, ROB-CLI-001 |
| 11 | Worker killed after dispatch (rerunnable) | same with the stub adapter | — | COMPLETED on rerun | 1 automatic rerun | attempt.outcome_unknown, attempt.started | attempts: [unknown, succeeded] | ROB-AMBIG-001 |
| 12 | Worker killed after provider returned, before commit | `kill -9` between adapter return and `set_result` | — | as rows 10/11 by class (`phase=returned`) | as rows 10/11 | attempt.outcome_unknown | no silent success or failure | ROB-AMBIG-001 |
| 13 | Poison job | the fake CLI SIGKILLs the worker | `e10b_poison.sh` | FAILED `attempts_exhausted` after `max_retries+1` | none | job.failed | worker restarts stop re-running it | ROB-STATE-001 |
| 14 | SIGTERM to `hoglah run` | `kill -TERM` during a CLI job | `e18_sigterm.sh` | drained or attempt `unknown` | bounded drain | shutdown events, attempt.outcome_unknown | "Shutting down" printed; no orphan process; exit 0 | ROB-SHUT-001 |
| 15 | Worker thread dies | raise `BaseException` in the loop | — | n/a | process exits non-zero | worker.degraded | `hoglah run` exit code ≠ 0 | ROB-SHUT-001 |
| 16 | Restart with PROCESSING rows (live peer) | two workers; restart one | — | peer's jobs untouched | none | — | no reclaim of a live peer within its window | ROB-LEASE-002 |
| 17 | Peer clock +60 s | reclaim from a skewed process | `e20_skew.py` | COMPLETED | none | — | 1 provider execution | ROB-LEASE-002 |
| 18 | Local clock jump +10 min | patch wall clock mid-job | — | COMPLETED | none | — | no reclaim | ROB-LEASE-002 |
| 19 | Galeed Mongo unreachable | `galeed_mongo_uri` points at a closed port | `e12_galeed_down.py` | jobs unaffected | n/a | dropped counter rises; WARNING rate-limited | `submit()` < 50 ms; direct callback fires; stub jobs < 1 s | ROB-LOG-001, ROB-LOOP-001 |
| 20 | Galeed slow (5 s per insert) | fake db with sleep | — | jobs unaffected | n/a | queue drains later | heartbeat never late | ROB-LOOP-001 |
| 21 | OpenAI-compatible endpoint hangs | fake server sleeps 12 s | `e11_openai_block.py` | per timeout policy | per policy | attempt.finished | loop lag < 0.5 s throughout | ROB-LOOP-001 |
| 22 | Ollama down then up | adapter host on a closed port for 30 s, then restored | `e_ollama_e2e.py` | all queued jobs COMPLETED | backoff within elapsed budget | retry.scheduled | 0 FAILED | ROB-RETRY-001 |
| 23 | Ollama bad option (500 with validation text) | `options={"temperature":"hot"}` | `e_ollama_e2e.py` | FAILED `caller_invalid` | at most 1 retry | attempt.finished | ≤ 2 attempts | ROB-CLASS-001 |
| 24 | Ollama empty prompt / `load` | `prompt=""` | `e_ollama_e2e.py` | FAILED `protocol_error` | none | attempt.finished | not COMPLETED | ROB-BACKEND-001 |
| 25 | Ollama silent input truncation | `num_ctx=128`, 1.7k-token prompt | curl probe | COMPLETED with `truncated=True`, reason `input` (or FAILED `context_limit` per job policy) | none | attempt.finished | flag set | ROB-BACKEND-001 |
| 26 | Ollama hang | fake server never answers | — | FAILED `timeout` at default | per policy | attempt.finished | finishes within the default total timeout | ROB-CLI-001 |
| 27 | Missing model, auto-pull off | `model="no-such:zz"` | `e_ollama_e2e.py` | FAILED `model_unavailable` | none | job.failed | < 1 s, no registry call | ROB-BACKEND-001 |
| 28 | CLI reads inherited stdin | fake CLI that blocks on stdin; worker stdin is an open pipe | E09 probe | COMPLETED | — | — | finishes promptly | ROB-CLI-001 |
| 29 | Prompt starts with `-` | prompt `--version` to the fake and real CLIs | E08 probe | prompt reaches the model as text | — | — | output is not a version string | ROB-CLI-001 |
| 30 | Prompt 1 MiB | fake CLI | E21 | COMPLETED | — | — | no E2BIG | ROB-CLI-001 |
| 31 | CLI exit 0, empty stdout | fake `empty` | `e13_fake.py` | FAILED `protocol_error` | once | attempt.finished | not COMPLETED | ROB-BACKEND-001 |
| 32 | Answer only on stderr | fake `stderr_only` | `e13_fake.py` | FAILED `protocol_error` | once | attempt.finished | not COMPLETED | ROB-BACKEND-001 |
| 33 | Streamed non-ASCII output | fake `utf8` with `stream_dir` | `e13_fake.py stream` | COMPLETED | — | — | output byte-identical to the non-streamed case | ROB-BACKEND-001 |
| 34 | Partial output then SIGSEGV | fake `partial_crash` | `e13_fake.py` | FAILED `process_failure`, signal SIGSEGV | once if rerunnable | attempt.finished | error names the signal; partial text not used as the error | ROB-BACKEND-001 |
| 35 | SIGTERM-immune grandchild | fake `orphan` + cancel | — | CANCELLED | — | job.cancelled | grandchild gone after the grace period | ROB-CLI-001 |
| 36 | Real CLI not logged in | isolated `HOME` (claude, grok) / empty `CODEX_HOME` | E10 probe (gated) | FAILED `authentication` | none | job.failed | category correct per CLI | ROB-BACKEND-001, ROB-CLASS-001 |
| 37 | Real CLI bad model | `-m no-such-model` (gated) | E10 probe | FAILED `model_unavailable` | none | job.failed | error text is the CLI's, not a stderr warning | ROB-BACKEND-001 |
| 38 | Gemini / Kiro documented codes | fake CLI exits 41/42/44/52/53 and 3; kiro footer-only stdout | — | mapped categories; `protocol_error` for footer | per category | attempt.finished | table-driven pass | ROB-BACKEND-001 |
| 39 | CLI version drift | fake CLI `--version` outside the tested range | — | jobs run | — | cli.version_drift | WARNING once at start | ROB-BACKEND-001 |
| 40 | Stream dir ENOSPC / read-only | patch `write_chunk`; `chmod 500` | `e14_stream_fail.py` | COMPLETED | — | stream disabled (WARNING) | not FAILED | ROB-CORE-001 |
| 41 | Output dir unwritable | `chmod 500` output_dir | — | COMPLETED; sink `failed` after bound | sink retries | delivery.failed, delivery.final | job unaffected; sink final state recorded | ROB-DELIV-001 |
| 42 | HTTP callback receiver down, then up | fake receiver 503 then 200 | — | COMPLETED | sink retried by sweeper | delivery.attempted ×N | delivered once per `delivery_id` (Idempotency-Key seen) | ROB-DELIV-001 |
| 43 | Callback ack lost | receiver processes and drops the connection | — | COMPLETED | resend with the same Idempotency-Key | delivery.attempted | receiver sees the same key twice | ROB-DELIV-001 |
| 44 | Process exits right after completion | `close()` / `kill -9` with an HTTP callback pending | — | COMPLETED; sink pending | next worker sweeps | delivery.attempted | POST arrives after restart | ROB-DELIV-001 |
| 45 | Named callbacks across restarts | 205 jobs, restart twice | `e09_redeliver.py` | — | none | — | 0 re-fires; interrupted deliveries complete | ROB-DELIV-001 |
| 46 | Direct callback race | Galeed blocking submit + fast job | `e12_galeed_down.py` | COMPLETED | — | — | callback fires exactly once | ROB-DELIV-001 |
| 47 | Kafka store error on one message | fake transport (Kafka semantics); `enqueue` raises once | `e19_kafka_skip.py` | message enqueued on retry | consumer seeks back / pauses | ingress.paused | `c1` present; offset not committed past it until handled | ROB-BRIDGE-001 |
| 48 | Kafka poison message | malformed body | existing tests | DLT copy | poison budget | ingress.dlt | committed only after the DLT ack | ROB-BRIDGE-001 |
| 49 | Rabbit broker restart | gated live broker (`RUN_RABBITMQ_TESTS`) | — | consumption resumes | reconnect | ingress.paused/resumed | no process restart needed | ROB-BRIDGE-001 |
| 50 | Redis handler failure | gated live Redis; `enqueue` raises once | — | entry handled | XAUTOCLAIM | — | handled without a restart | ROB-BRIDGE-001 |
| 51 | Broker egress down, then up | `produce_and_flush` raises for 30 s | — | result published | periodic sweeper | delivery.attempted | no restart required | ROB-DELIV-001 |
| 52 | Submitter timeout + retry | fake transport drops the ack; caller retries with the same id | — | one job | `SubmitOutcomeUnknown` then the result | — | 1 job row; result returned | ROB-BRIDGE-001 |
| 53 | Corrupt SQLite row | corrupt `request_json` | `e26_poison_row.py` | bad row FAILED `corrupt_row`; others COMPLETED | — | row.quarantined | healthy jobs finish | ROB-SCHED-001 |
| 54 | Label evaluation error | `list(label_group=…)` raises | — | later label stays QUEUED | next poll | readiness error counter | no out-of-order dispatch | ROB-SCHED-001 |
| 55 | PROCESSING snapshot error | `_processing_snapshot` source raises | — | no admission on capped dimensions | next poll | — | slot caps never exceeded | ROB-SCHED-001 |
| 56 | Persistent store outage | store raises everything for 2 min | — | unchanged rows | worker degraded | worker.degraded / recovered | `/metrics` returns 200 with gauges during the outage | ROB-LOOP-001 |
| 57 | `hoglah clear`, answer "n" | `echo n \| hoglah clear` | E25 | rows unchanged | — | — | count unchanged | ROB-ADMIN-001 |
| 58 | `clear` with no filter | QUEUED + PROCESSING + COMPLETED rows | — | only eligible terminal rows deleted | — | rows.deleted | non-terminal rows remain | ROB-ADMIN-001 |
| 59 | Delete a parent with live children | `remove(parent)` | — | refused ("deferred: live dependents") | — | — | child still runnable | ROB-ADMIN-001 |
| 60 | Batch idempotency + failure | an item matches an existing key; a later item invalid | — | existing job untouched | compensation scoped | — | pre-existing job still present | ROB-BATCH-001 |
| 61 | Kill mid-`submit_batch` | `kill -9` after k of n inserts | — | no item claimed | stale-token cleanup | batch.abandoned | 0 dispatches from the batch | ROB-BATCH-001 |
| 62 | Bulk requeue | FAILED rows of every category | — | ambiguous / poison / corrupt rows skipped | — | requeue | skipped ids printed | ROB-AMBIG-001 |
| 63 | Secret in metadata | `metadata={"api_key":…}` | E24 | — | — | — | 0 occurrences in any sink or Galeed record | ROB-SEC-001 |
| 64 | Documented env formats | `HOGLAH_CLI_BINARIES=claude=/x` etc. | E22 | — | — | — | settings load | ROB-CONF-001 |
| 65 | State-machine property | Hypothesis `RuleBasedStateMachine`, 2 simulated workers, random kill / cancel / requeue / delete | — | — | — | — | INV-1…14 hold over 10⁴ sequences | ROB-STATE-003 |
