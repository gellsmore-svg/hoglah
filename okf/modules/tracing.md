---
type: Module
title: tracing
description: JobWitness — the best-effort Galeed emitter: job.* lifecycle events onto the spine and full-I/O llm_calls capture, gated by galeed_enabled / galeed_capture_io.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/tracing.py
tags: [hoglah, module, tracing, galeed]
timestamp: 2026-07-05T00:00:00Z
---

# tracing

`JobWitness(config)` lives on the client. `emit(type, job_id=…, …)` sends one
`job.*` spine event through a per-event Galeed `Tracer` (trace_id = job id);
`record_io(job_id=…, request=…, result=…)` writes the full In→Out document
into `llm_calls` (see the [observability concept](../concepts/observability.md)).

Config (`HoglahSettings`): `galeed_enabled` (off by default),
`galeed_mongo_uri` / `galeed_mongo_db` (the database the family trace API
reads — `mnemosyne_dev` by default), `galeed_capture_io` (on when the spine
is on). The lazy Mongo handle is created under a lock with the resolved flag
set last — the submitter and worker threads can both race the first emission,
and an unlocked check-then-act here silently dropped events once.

Call sites in the client: submit (queued), `_process_job` (started,
completed/failed + `record_io`), `cancel` (cancelled).
