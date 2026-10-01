# Iteration 6

## Answer

This pass was narrow, as iteration 5 asked. All checks were read-only against `src/hoglah` v0.13.0.

### (a) Attack on "ingress dedupe hit appends a broker sink for the new reply route" (R12/R10)

**A1. The fix does nothing on Redis.** This was found by reading the code.
- The Redis submitter's reply route is the shared results stream (`messaging_submitter.py` `reply_destination()` returns `results_stream`). So a retry has the same route as the first submit.
- That gives the same `delivery_id = job:attempt:broker:route`. Under R10's rule "append-only per id", no sink is appended.
- The original sink may already have been delivered, before the retry captured its new tail (`:268-270`). In that case the submitter never sees the result.
- N48 is therefore still open on Redis. Kafka works only because the submitter reads from `earliest`.
- The real fault is on the submitter side: a replayable log is read from the wrong position.

Fix, which is simpler than iteration 5's version:
- Log transports (Kafka, Redis) resolve a retry on the submitter. A retry that passes a `correlation_id` also passes `since` = the first attempt's wall-clock milliseconds.
  - Redis stream ids are millisecond timestamps, so the submitter reads `XREAD` from `<since>-0`. It no longer reads from the pre-add tail.
  - Kafka keeps reading from `earliest` in a fresh group, which works within retention.
- The bridge appends a sink only when the route is not one already recorded for that job. In practice that means Rabbit, where every new connection gets its own exclusive reply queue.

**A2. Unbounded sink growth under a client retry loop.**
- On Rabbit, each retry from a new connection adds a new route, so a loop adds one sink per retry. Every route except the newest is dead: exclusive, auto-delete, server-named queues.
- Fixes:
  - **Latest route wins.** Appending a broker sink marks every older *pending* broker sink of that job `failed`, reason `superseded`. This uses the existing `pending→failed` move, so no new sink state or transition is added.
  - **Cap.** At most `max_reply_routes` broker sinks per job (default 8). Past the cap, the dedupe hit is still acked, logged at WARNING and counted in `reply_route_cap_hits`. The caller keeps getting `SubmitOutcomeUnknown`. That is honest and bounded.
- Redelivery of the *same* ingress message (a crash between enqueue and ack) has the same route, so it appends nothing.

**A3. Interaction with R27 deletion.**
- A pending sink aimed at a dead Rabbit route would block deletion until its bounded attempts run out.
- Rabbit publishes with `mandatory=True` and confirms (`rabbitmq.py:174-179`), so `UnroutableError` is a definite signal. For a server-named exclusive queue it is permanent.
- Fold: an unroutable result on a server-named reply queue sets the sink `failed`, reason `route_gone`, at once. `superseded` and `route_gone` are counted in `reply_route_gone`, not in the `delivery_failed` alarm, because the caller simply went away.
- With supersede, the cap and `route_gone`, every broker sink reaches a terminal state in bounded time. R27's pending-sink block therefore always clears, except in the "no capable sweeper" case (see B1).

**A4. Interaction with INV-8 and INV-11.**
- INV-8 ("one sink record per configured sink") contradicts appending extra broker sinks. INV-11 ("a dedupe hit yields a deliverable sink for the new reply route") is false for log transports under the corrected design.
- Both are reworded; see the diffs below.

**A5. Simpler alternatives considered.**
- *Reply only on the results topic, keyed by correlation_id* (with Kafka compaction): this is adopted for the log transports. It cannot cover Rabbit, whose per-connection reply queues cannot be replayed.
- *A single broker sink whose route is overwritten in place* (latest route wins, with a route epoch): rejected. A sink that has already been delivered would have to reopen, which breaks INV-8. Appending with supersede gives the same effect with no reopen.

### (b) Attack on the capability-scoped sweeper (R10)

**B1. No capable worker ever runs.**
- The `delivery_backlog` gauge lives in a worker's memory. If no worker runs, nothing exports it, so iteration 5's mitigation is itself silent.
- Fold:
  - `stats()` and the CLI report pending sinks by kind with the oldest age, derived from the store.
  - A sink still pending past `sink_ttl` (default 7 days, configurable) goes to `failed`, reason `undeliverable`. Any process may apply this, including a CLI command, because it is a CAS on age and needs no delivery capability.
  - This also caps how long R27 deletion can be blocked.

**B2. Two capable workers.**
- R10 says "fenced sweeper" but never defines the fence. Adding a sink lease would repeat R5's clock problems.
- Simplification:
  - Only the state flip `pending→delivered/failed` and the attempt-counter increment are CAS writes.
  - Two sweepers may both send. Duplicate sends are allowed and declared, which is already within at-least-once per sink with an Idempotency-Key or delivery id.
  - Sweepers skip sinks younger than a grace period (2× that sink's delivery timeout). This avoids routinely duplicating the committing worker's live delivery.
- No sink lease is introduced.

**B3. Capability mismatch after a config change. This is a new silent mode (N53), and it exists in today's code.**
- Neither sink records nor today's `_kafka` metadata record *which* broker a route belongs to.
- `republish_unpublished` publishes through whatever transport is configured now (`kafka_bridge.py:637-660`). After a move from cluster A to cluster B, a result is produced to topic `reply_to` on B. The produce succeeds and the row is marked published, but the caller on A never receives it.
- Two other mismatches leave sinks with no capable sweeper (B1): a named callback dropped from the registry, and a changed output directory.
- Fold:
  - Each sink snapshots its target at write time: file path, URL, callback name, or `broker_id` plus route. `broker_id` is a cluster id or a normalized URL hash.
  - "Capable" means the worker can reach exactly that target.
  - A worker whose `broker_id` differs never publishes or marks that sink. Such sinks age out through `sink_ttl`.

### (c) Internal-consistency check of the iteration-5 set

**C1. The R16 breaker.**
- Iteration 1 merged R17 into R16 and kept only a metric gauge. The stated reason: E20 shows a retry budget much shorter than daemon restart time (3 attempts in 3.1s), not load or stampede harm.
- Iterations 2–4 kept that. Iteration 5's R16 brought back a full breaker (defer without consuming attempts, half-open probe) with no new evidence. That was drift.
- **Decision:** E20 supports only per-category elapsed budgets. The breaker is removed again and replaced by a per-backend health gauge (consecutive failures by category). A breaker MAY be added if load evidence appears.
- There is also a second reason. "Defer that backend's queued jobs" contradicts INV-10: a due, ready row must be admitted within a bounded number of polls. Removing the breaker resolves that.
- R18's "breaker transitions" event goes as well.

**C2. The R25 table versus R3 and INV-3.**
- R3 lets a *reclaimer* write FAILED `outcome_unknown`, and R1 lets it write `attempts_exhausted`. R25, however, allows PROCESSING→FAILED only through a fenced commit. INV-3 says only the lease holder may move a PROCESSING row.
- R3's behaviour is already unambiguous, so the table and the invariant are brought into line with it. This is a table omission, not a new ambiguity.

**C3. INV-12 versus R27.**
- INV-12 says any protected row can be deleted "with an explicit force flag". R27's `--force` overrides only the dedupe window, and `--force-nonterminal` cancels the row first.
- Unsealed rows and rows with live dependents have no force at all.
- INV-12 is reworded to follow R27.

**C4. INV-2 versus R27.** INV-2 says FAILED and CANCELLED rows leave only through operator requeue, but R27 can delete terminal rows. INV-2 is reworded to include R27 deletion.

**C5. INV-8 and INV-11 versus R10 and R12.** These were fixed in A4.

**C6. Other pairs checked, with no conflict found:**
- R7 (no pre-commit `get`) vs R3 (persist `dispatched` before the side effect)
- R4 (fenced read to classify a False renewal) vs R7
- R1 / INV-4 vs R15 (`unknown` retries at most once)
- R13 vs R14 (auto-pull outside the attempt)
- R26 / INV-9 vs R25
- R6 gauges vs B1: the fix makes the operator-facing view store-derived, while the R6 gauges stay in memory

### Requirement diffs

**R10 ROB-DELIV-001**
- `− a periodic, fenced sweeper in any worker completes it, claiming only sinks it is capable of delivering.`
- `+ Each sink snapshots its target at write time (path, URL, callback name, or broker_id + route). A sweeper in any worker completes sinks whose exact target it can reach; a worker whose broker_id differs never publishes or marks them. Only the state flip and attempt increment are CAS writes; concurrent duplicate sends are allowed (declared, idempotent by delivery id). Sweepers skip sinks younger than 2× their delivery timeout. A sink pending beyond sink_ttl (default 7d) becomes failed/undeliverable; any process may apply this. Older pending broker sinks of a job become failed/superseded when a newer route is appended; an unroutable server-named reply queue is failed/route_gone at once. superseded and route_gone count in reply_route_gone, not delivery_failed.`
- `− delivery_backlog exposes pending age;`
- `+ delivery_backlog (in-memory) plus store-derived pending-sink counts and oldest age in stats() and the CLI;`
- Covers: `+ N53`.

**R12 ROB-BRIDGE-001**
- `− On an ingress dedupe hit the bridge appends a broker sink for the message's reply route (delivered at once if the row is terminal).`
- `+ On an ingress dedupe hit whose reply route differs from every route recorded for the job, the bridge appends a broker sink for it (delivered at once if terminal), superseding older pending broker sinks; at most max_reply_routes (default 8) per job, beyond which the hit is acked, logged WARNING and counted. Log transports resolve retries on the submitter: a retry with a supplied correlation_id also supplies since = first-attempt time, and reads the results log from there (Redis XREAD from <since>-0; Kafka from earliest in a fresh group).`

**R16 ROB-RETRY-001**
- `− Per-backend breaker: repeated unavailable/auth failures defer that backend's queued jobs without consuming attempts; half-open single probe; state exported.`
- `+ Per-backend health gauge (consecutive failures by category), metric only. A breaker MAY be added if load evidence warrants it (E20 shows only an undersized budget).`
- Absorbs: `R17 (gauge only)`.

**R18 ROB-LOG-001**
- `− breaker transitions,`
- `+ reply route superseded/gone, sink undeliverable,`

**R25 ROB-STATE-003**
- `+ | PROCESSING→FAILED | reclaim-fail: CAS on the unchanged (token, expires) per R5, causes outcome_unknown (R3) or attempts_exhausted (R1) |`
- `QUEUED→FAILED` row: `− attempts_exhausted` (this is reached only through claim or reclaim of a PROCESSING row).

**R27 ROB-ADMIN-001**
- `+ Override map: --force = dedupe window only; --force-nonterminal = cancel then delete; pending sinks clear only by delivery, failure or sink_ttl; unsealed rows only by abandon GC; rows with live dependents are never deleted.`

**INV-2** `→ COMPLETED is absorbing; FAILED/CANCELLED rows leave only via operator requeue (new epoch) or R27 deletion.`

**INV-3** `+ …except reclaim-fail and conditional cancel, which act by CAS on the observed lease (R25).`

**INV-8** `→ Every terminal row has at least one sink record per configured sink, each with a fixed target snapshot; extra broker sinks arise only from an ingress route change; a terminal sink state never reverts; every sink reaches a terminal state within its bounded attempts or sink_ttl.`

**INV-11** `→ An accepted ingress message is acked/committed only after its job row (or a DLT copy) is durable; after a dedupe hit the result is obtainable on the new reply route (replayable log read from since, or an appended sink), subject to max_reply_routes.`

**INV-12** `→ Deletion follows R27's override map: no flag deletes unsealed rows or rows with live dependents; pending sinks block deletion until terminal.`

### New failure modes

| ID | Failure mode | Pass | Basis | Severity | Covered by |
|---|---|---|---|---|---|
| N48b | Iteration 5's dedupe-hit fix does nothing on Redis (same route, same delivery id), so N48 stays open there | 2/6 | code-backed (`messaging_submitter.py` Redis `reply_destination`, `:268-270`) | medium (residual of N48, not new) | R12 |
| N53 | After a broker config change, the startup or sweeper re-publish goes to a different cluster and is marked published, so the caller never receives it | 3/5 | code-backed (`kafka_bridge.py:637-660`; `_kafka` meta has no broker identity) | low-medium | R10 |
| N54 | A client retry loop on Rabbit adds unbounded dead-route sinks that block deletion and raise false `delivery_failed` alarms | 2 | design attack on R12 | low | R10, R12 |
| N55 | The backlog gauge is invisible when no worker runs | 5 | design attack on R10 | low | R10 |

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.97 | Redis retry routing, broker-identity drift and sink growth are now mapped. |
| restart_reconstructability | 0.89 | Unchanged. Sink target snapshots make re-delivery after a restart deterministic. Results lost in long store outages remain declared. |
| retry_idempotency_safety | 0.88 | Correlation-id retries now resolve on all three transports. Duplicate sweeper sends are declared and covered by delivery-id idempotency. |
| galeed_reconstructability | 0.75 | Unchanged. Galeed is lossy and is the only history left after deletion. |
| guarantee_honesty | 0.94 | The breaker, which had no evidence, is removed. Duplicate sends, the reply-route cap and `sink_ttl` loss are declared. |
| simplicity_compat | 0.84 | No breaker and no sink lease. The bridge appends only on a route change. One new submitter parameter (`since`) and two config knobs. |
| residual_high_risk | 0.15 | Nothing is implemented. N48b and N53 are code-read, not reproduced. |

## Changes From Previous
- CHANGED: R10 (target snapshot, `broker_id` capability, CAS-only fence, grace period, `sink_ttl`, supersede, `route_gone`, store-derived backlog), R12 (append only on route change, supersede, cap, submitter `since` for log transports), R16 (breaker reverted to a gauge, per E20), R18 (event list), R25 (reclaim-fail row), R27 (override map).
- Invariants reworded: INV-2, INV-3, INV-8, INV-11, INV-12.
- UNCHANGED: R1, R3, R4, R5, R6, R7, R8, R9, R13, R14, R15, R20, R22, R23, R24, R26.
- No IDs were added or deleted. No new job or sink state was added.

## Uncertainty
- `since` relies on submitter wall-clock time against Redis server stream ids. A skewed client clock could start the read late. A 60s lookback margin is advisable.
- How `broker_id` is defined for each transport (Kafka cluster id; Rabbit and Redis normalized URL) is not settled.
- The default `sink_ttl` trades lost late delivery against blocked deletion.
- Server-named Rabbit queues are told apart by the `amq.gen-` prefix. A client that uses a named reply queue would get bounded retries instead of `route_gone`.

## Stop-Relevant Notes
- **CONVERGENT (borderline, by the stated rule).**
  - No new high-risk mode was found. N53 is low-medium; N54 and N55 are low.
  - N48b is the medium residual of the already-listed N48. Its fix is a mechanism correction inside R12 that makes the existing INV-11 claim true. It is not a new obligation.
  - The R16 breaker reversion restores the evidence-backed text and removes a contradiction with INV-10.
  - R25, INV-2, INV-3 and INV-12 are consistency fixes to text that R3 and R27 already pinned down unambiguously.
  - No new job or sink state or transition was added: supersede and `route_gone` reuse `pending→failed`.
  - No risk judgement moved by more than one step.
- A stricter reader could count the submitter `since` parameter and the `broker_id` snapshot as new requirements. If so, run one more pass limited to them. Expect only folds.
- Do not reopen anything else. Four consecutive passes have found no high-severity mode.
