# Iteration 7

## Answer

This was the final narrow pass. All checks were read-only against `src/hoglah` v0.13.0 (`messaging_submitter.py:108-140, 264-300`; `redis_streams.py:128-132`; `config.py:626-633`; `kafka_bridge.py:187, 579, 612-680`).

### (a) Attack on R12 `since` and the R10 target snapshot / `broker_id`

**A1. Client clock skew against Redis ids (fold).** Redis stream ids use the *server's* milliseconds. A `since` built from the submitter's wall clock reads too late when the client clock runs ahead, which is exactly the WSL2 jump case. A late read misses the result and leaves the caller stuck on `SubmitOutcomeUnknown`. Nothing is lost silently, but the retry never resolves.

The code already has the right position. `publish_request` captures the results-stream tail with `xrevrange` *before* the `XADD` (`:282-283`). Fold: `since` becomes an opaque **resume token** from the server, not a time.
- On Redis the token is the tail id captured before the add.
- On Kafka it is ignored, and the read starts from `earliest`.

`SubmitOutcomeUnknown` carries `(correlation_id, resume_token)`. No skew margin is needed.

**A2. Caller loses `since` (fold).** The fallback reads from `0` on Redis or `earliest` on Kafka. This stays correct within retention. The cost is a full scan, since the bridge's `XADD` has no `MAXLEN` (`redis_streams.py:131`) and the stream grows without bound. That cost is declared.

**A3. Retention expiry or trimming (declared, low).** If the results log is trimmed or past retention, a retry with the same id is a dedupe hit on the same route. The bridge appends no sink and nothing is re-published, so the caller gets `SubmitOutcomeUnknown` forever. This is honest but does not resolve. Past `dedupe_window`, a retry creates a duplicate job, which is already declared in R12.

Fold: R12 declares that resolution beyond log retention is a store lookup by `correlation_id` (operator/CLI), and the docs state that results-log retention must be at least the caller's retry horizon. No new mechanism is added.

**A4. Target snapshot vs R22 (material defect in the iteration-6 wording; fixed by a definition).** Broker URLs carry credentials. The default is `amqp://guest:guest@…` (`config.py:94`), and `config.to_dict` deliberately excludes connection strings (`:629-631`). Iteration 6 defined `broker_id` as a "normalized URL hash". That causes two problems:
- **Leak.** A raw URL in a sink snapshot persists the secret into the store and into anything that renders sinks: `stats()`, CLI, Galeed `delivery` events.
- **Silent orphaning (N56).** If the hash includes userinfo, a routine **password rotation** changes `broker_id`. Every pending broker sink then becomes "not capable" for every worker. Nothing is delivered, and after `sink_ttl` (7d) all of them go to `failed/undeliverable`. The failure is counted but the loss is real, and it is caused by routine operations.

Fold (definition only): `broker_id` contains no credentials.
- Kafka: the cluster id.
- Rabbit: scheme, sorted host:port list, and vhost.
- Redis: host:port and db.

Userinfo, query and TLS parameters are excluded. HTTP-callback snapshots are the URL already stored in the request, and R22 redaction applies wherever they are rendered: events, stats, CLI.

**A5. `broker_id` definition changes on upgrade (fold).** Changing the normalization would orphan pending sinks just as A4 does. Fold:
- `broker_id` is versioned (`v1:kafka:<cluster_id>`). A worker matches any version it can compute.
- Legacy rows (v0.13 `_kafka` metadata with no snapshot) are treated as "the currently configured broker", which is today's behaviour, and each one logs a WARNING.

This also covers a reordered or DNS-aliased bootstrap list on Kafka, because the cluster id is stable across both.

### (b) Full read of the merged set (iteration 5 + iteration-6 diffs)

**Contradictions or priority inversions found (all one-line folds):**
1. **R1/INV-4 (P0) vs R16 (P2).** Under the full set, retries are fenced requeues, so attempt = claim = one dispatch, and there is no ambiguity. Under P0-only rollout, the in-claim retry loop (`_execute_with_retries`) still runs inside one claim. Total dispatches could then reach `(max_retries+1)²`, which breaks INV-4's intent. Fold: R16's first sentence (retry = fenced requeue with `run_at`) is **P0**, shipped with R1. Budgets, Retry-After, jitter and the health gauge stay P2.
2. **R12 (P0) and R27 (P0) rely on R10 (P1) sink records.** Fold: R12's sink-append clause and R27's "pending-sink" check land with R10. Before then, R27 checks today's `result_published` flag, and the P0 part of R12 is: never skip, `SubmitOutcomeUnknown`, caller `correlation_id`, resume token.
3. **R7 (P0) says "sink-recorded".** Fold: "(sink-recorded once R10 lands; logged at WARNING until then)".
4. **Guarantee statement vs `sink_ttl` and `max_reply_routes`.** The phrase "at-least-once per delivery sink" overstates. Fold: "at-least-once *attempted* per (job, attempt, sink, route) within bounded attempts or `sink_ttl`; otherwise recorded `failed` with a reason".
5. **INV-10 wording.** It lists only "dependencies are satisfied", although R9 also gates on label sequence and seal token. Fold: "…whose admission gates (R9) are satisfied…". This is not a transition change.
6. **R24 cites "E01–E26", but SOURCE has no E24.** Fold: "E01–E23, E25, E26".

**Pairs checked with no conflict:**
- R3 force-requeue vs INV-6, because each requeue starts a new epoch
- R25 reclaim-fail row vs R5 and INV-3 as amended
- R8 vs R10, because cancel writes sinks
- R26/INV-9 vs R27 unsealed-delete
- R4/R5 vs E06, where a long freeze leads to reclaim and a rerun only for the rerunnable class (declared at-least-once)
- R14 auto-pull vs R13 timeouts
- R16 health gauge vs INV-10, with no deferral left

No state or transition is added or changed.

### Requirement diffs (wording folds only)
- **R12**: `− since = first-attempt time … Redis XREAD from <since>-0` → `+ resume_token returned in SubmitOutcomeUnknown (Redis: results-stream tail id captured before XADD; Kafka: earliest); if lost, read from 0/earliest (full scan, declared); beyond log retention, resolve by store lookup on correlation_id — declared.` `+ P0 scope excludes the sink-append clause, which lands with R10.`
- **R10**: `+ broker_id is versioned and credential-free (Kafka cluster id; Rabbit scheme+sorted hosts+vhost; Redis host:port/db); legacy sinks without a snapshot use the current broker with a WARNING; snapshots are rendered redacted (R22).`
- **R16**: `+ Priority split: "retry = fenced requeue with run_at" is P0 (with R1); remainder P2.`
- **R7**: `+ (sink-recorded once R10 lands; WARNING log before)`.
- **R27**: `+ pre-R10: the pending-sink check uses result_published.`
- **R24**: `E01–E26` → `E01–E23, E25, E26`.
- **INV-10**: `dependencies are satisfied` → `admission gates (R9) are satisfied`.
- **Guarantee line**: `at-least-once per delivery sink` → `at-least-once attempted per (job, attempt, sink, route), bounded by attempts/sink_ttl, else recorded failed`.

### New failure modes
| ID | Failure mode | Pass | Basis | Severity | Covered by |
|---|---|---|---|---|---|
| N56 | `broker_id` that includes URL credentials: a secret is persisted in sink snapshots, and a password rotation orphans every pending broker sink until `sink_ttl` | 2/3 | code-backed (`config.py:94, 629-631`) attack on the iteration-6 definition | medium (design-of-fix; nothing implemented) | R10 definition fold, R22 |
| N57 | Wall-clock `since` against Redis server ids: a skewed client misses its result and the retry never resolves | 2/6 | code-backed (`messaging_submitter.py:282-297`) | low (raises, not silent) | R12 resume-token fold |
| N58 | P0-only rollout with the in-claim retry loop allows up to `(max_retries+1)²` dispatches | 8 (consistency) | code fact (`_execute_with_retries`) | low-medium | R16 priority split |

### E01–E26 coverage
| E | Requirements | E | Requirements |
|---|---|---|---|
| E01 | R9, INV-10 | E14 | R3, R13, R20 |
| E02 | R8, R25 | E15 | R10 |
| E03 | R4, R18 | E16 | R7 (ex-R19) |
| E04 | R7, R10 | E17 | R5 |
| E05 | R6, R10, R18 | E18 | R12, INV-11 |
| E06 | R6, R13 | E19 | R14, R15 |
| E07 | R14 | E20 | R14, R16 |
| E08 | R13, R14 | E21 | R13 |
| E09 | R13 | E22 | R23 |
| E10 | R14, R15 | E23 | R7 |
| E11 | R15 | E24 | does not exist in SOURCE |
| E12 | R1, R3, R13 | E25 | R25, R27 |
| E13 | R1, R3 | E26 | R9 |

Every E-scenario in SOURCE is covered, and R24 tests each one.

## Score Vector
| name | value | rationale |
|---|---|---|
| failure_mode_coverage | 0.97 | Unchanged. The new items are attacks on iteration-6 wording, and all are mapped. Every E-id is covered. |
| restart_reconstructability | 0.89 | Unchanged. Credential-free, versioned `broker_id` keeps pending sinks deliverable across restarts and password rotations. |
| retry_idempotency_safety | 0.89 | +0.01. Server-side resume tokens remove clock skew from submitter retries, and the R16 P0 split closes the partial-rollout dispatch blow-up. |
| galeed_reconstructability | 0.75 | Unchanged. Galeed is lossy and is the only history left after deletion. |
| guarantee_honesty | 0.95 | +0.01. The delivery guarantee is reworded to "attempted, bounded". Retention-limited resolution is declared. |
| simplicity_compat | 0.84 | Unchanged. No IDs, states or knobs are added. The resume token reuses a position the code already captures. |
| residual_high_risk | 0.15 | Unchanged. Nothing is implemented. N48b, N53 and N56 are code-read, not reproduced. |

## Changes From Previous
- CHANGED (wording folds only): R7, R10 (`broker_id` definition), R12 (resume token, P0 scope, retention declaration), R16 (priority split), R24 (E range), R27 (pre-R10 check), INV-10, guarantee line.
- UNCHANGED: R1, R3, R4, R5, R6, R8, R9, R13, R14, R15, R18, R20, R22, R23, R25, R26, INV-1…9 and INV-11…14.
- No IDs added or deleted. No job state, sink state or transition added.

## Uncertainty
- Kafka cluster-id retrieval needs an admin/metadata call at startup. If that call is unavailable, the fallback is the sorted bootstrap list, which is weaker against DNS aliasing.
- The legacy "current broker" rule for v0.13 rows keeps today's N53 exposure for rows written before the upgrade.
- The Redis full-scan fallback cost is unbounded, because nothing trims the results stream. A `MAXLEN` would trade that cost against A3 retention loss.
- Severities are still code-read. Nothing in this pass was reproduced.

## Stop-Relevant Notes
- **CONVERGENT.** This is the second consecutive non-material pass.
  - **No new high-risk mode.** N56 is medium, but it is a defect in iteration 6's not-yet-implemented *definition* of `broker_id`, and the fix is one definitional sentence. N57 and N58 are low or low-medium.
  - **No new requirement.** Every change is a wording or priority fold inside an existing ID.
  - **No new state-transition ambiguity.** The R1/R16 issue is about counting under partial rollout, and no transition changes.
  - **No risk judgement moved more than one step.** No score moved more than 0.01.
- The merged set is internally consistent after the six folds above, and every SOURCE E-scenario maps to at least one requirement.
- Recommend stopping. Any further work belongs in implementation and the reproduction of N42–N58, not in another review pass.
