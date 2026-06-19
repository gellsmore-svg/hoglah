---
type: Concept
title: Decoupled submitter/worker topology
description: Pure submitters feed a shared queue and a separate worker daemon executes; only worker instances recover interrupted jobs, which makes the separate-submitter topology safe.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/docs/architecture-decisions.md
tags: [hoglah, topology, concurrency, adr-016]
timestamp: 2026-06-19T00:00:00Z
---

# Decoupled submitter/worker topology

Hoglah separates **submitting** work from **executing** it:

- A **pure submitter** constructs the client with `start_worker=False`, calls
  [`submit()`](../modules/client.md), and collects the result — it never runs a
  model. Tirzah and Milcah are submitters.
- A **worker daemon** (`hoglah run --real`, see [run](../cli/daemons.md)) owns
  execution: it claims queued jobs from the shared [store](../modules/storage.md)
  and runs them through an [adapter](../modules/adapters.md).

**Interrupted-job recovery is a worker responsibility (ADR-016):** only
`start_worker=True` instances run recovery. Previously every client re-queued any
in-flight job on construction — with a live daemon, a submitter constructing a
client would re-queue the daemon's running jobs and cause double processing.
Recovery now happens only in the instance that executes, so a submitter is safe to
come and go.

This is what makes the **shared-queue** pattern work: one (or more) worker daemons
plus any number of submitters against the same SQLite/Mongo store, or against a
broker via the [messaging bridges](../modules/messaging-bridges.md). Two *workers*
on one SQLite file still need care (a process-level lock); MongoDB's atomic claim
makes multi-worker safe server-side.

Related: [result delivery](result-delivery.md) (how a detached submitter learns a
job finished), [messaging submitter](../modules/messaging-submitter.md).
