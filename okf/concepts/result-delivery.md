---
type: Concept
title: Result delivery
description: Five ways a caller collects a finished job — poll/wait, in-process callback (with a restart-surviving named registry), per-job HTTP callback, an output-folder file, and a broker reply.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/docs/architecture-decisions.md
tags: [hoglah, delivery, callback, adr-014, adr-015]
timestamp: 2026-06-19T00:00:00Z
---

# Result delivery

A job runs detached from its submitter, so Hoglah offers several ways to collect
the terminal [`JobResult`](../modules/models.md):

1. **Poll / `wait()`** — `get(job_id)` reads the store; `wait(job_id, timeout)`
   blocks until terminal. Works for any submitter against the shared store.
2. **In-process callback** — pass `callback=callable`. Best-effort re-delivery of
   recent completions on restart; durable cases use a **named registry**
   (`callback_key="..."` + a key→callable map supplied on startup) so a callback
   survives a process restart (ADR-006 / DQ-008).
3. **HTTP callback (ADR-015)** — pass `callback_url`; the worker POSTs the result
   JSON to it the instant the job finishes. Runs on a daemon thread (a slow
   endpoint never blocks the worker), retries with backoff, stdlib `urllib` only.
4. **Output file (ADR-014)** — set `output_dir`; the worker atomically writes
   `<output_dir>/<job_id>.json` on terminal status. A poller never reads a partial
   file.
5. **Broker reply** — with a [messaging bridge](../modules/messaging-bridges.md),
   the result is published to the request's `reply_to` (or the results topic),
   keyed by `correlation_id`; the [messaging submitter](../modules/messaging-submitter.md)
   awaits it.

Mechanisms 3–5 exist so a **detached** submitter (see [decoupled topology](decoupled-topology.md))
can be notified without sharing the worker's in-process callbacks. Delivery is
always best-effort and never alters the persisted terminal status.
