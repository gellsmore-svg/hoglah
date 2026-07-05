---
type: Concept
title: Observability — the spine witness and full-I/O capture
description: Hoglah testifies its job lifecycle onto the family Galeed spine and records every generate job's complete prompt/messages and output into the llm_calls debugging store — opt-in, best-effort, never touching the queue.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/tracing.py
tags: [hoglah, concepts, observability, galeed, debugging]
timestamp: 2026-07-05T00:00:00Z
---

# Observability

Hoglah is infrastructure: its work happens in a background worker, invisible
unless it testifies. With `galeed_enabled` (env `HOGLAH_GALEED_ENABLED=1`, the
`galeed` extra) the client emits one spine event per lifecycle transition —
`job.queued/started/completed/failed/cancelled` — with the job id as
`trace_id` and mirrored into `metadata.job_id` (Galeed's correlation key).

With `galeed_capture_io` (default on alongside the spine) every **generate**
job additionally records its COMPLETE prompt/messages and output into Galeed's
`llm_calls` store: `call_id` = job id, `parent_call_id` = parent job id, and a
caller-threaded `metadata.trace_id` joins a multi-step flow into one tree.
Label calls with `submit(step_name="initial_research")` / `--step`. Embedding
jobs are skipped (vectors are not debugging reading).

Both layers are strictly **best-effort**: lazy imports, a locked lazy Mongo
handle (a thread race here once silently dropped events — hence the lock), and
every failure swallowed. The queue behaves identically with tracing on, off,
or broken. Viewers: `hoglah debug`, `galeed trace`, `hoglah serve`, Mizpah.
