---
type: Module
title: Storage backends
description: The JobStore seam — a single-file SQLite store by default (zero setup) and an optional MongoDB store for multi-worker/multi-machine queues, selected at construction time.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/store.py
tags: [hoglah, storage, sqlite, mongodb, adr-002, adr-017]
timestamp: 2026-06-19T00:00:00Z
---

# Storage backends (`store.py`, `mongo_store.py`)

The store is the **source of truth** for jobs and results, behind a `JobStore`
seam (ADR-002). Two drop-in backends, chosen at `Hoglah(config={"backend": ...})`
construction (or `HOGLAH_BACKEND`):

- **`SQLiteJobStore`** (default) — a single-file `hoglah.db`, zero dependencies.
  Concurrency safety uses WAL + `busy_timeout`; `claim_for_processing` flips
  `QUEUED → PROCESSING` so each job runs once.
- **`MongoJobStore`** (ADR-017, `pip install "hoglah[mongo]"`) — for shared,
  multi-worker / multi-machine queues. The atomic claim is server-side
  (`find_one_and_update({_id, status: QUEUED} → PROCESSING)`), so concurrent
  workers (even cross-machine) execute each job once with no file lock. Jobs are
  native documents (inspectable from `mongosh`/Compass). Returned rows mirror the
  SQLite store exactly.

Both keep the `correlation_id` UNIQUE index that the [messaging bridges](messaging-bridges.md)
rely on for [idempotent enqueue](../concepts/crash-safety.md). Note: the
[messaging bridges](messaging-bridges.md) are **transport, not storage** (ADR-018) —
the store remains authoritative even when jobs arrive over a broker.
