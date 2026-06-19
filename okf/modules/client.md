---
type: Module
title: Hoglah client
description: The Hoglah class — the public API (submit, get, wait, list, cancel, submit_embedding) and, when start_worker=True, the asyncio worker loop; configured by HoglahConfig.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/client.py
tags: [hoglah, client, api]
timestamp: 2026-06-19T00:00:00Z
---

# Hoglah client (`client.py`)

The `Hoglah` class is the entry point. Construct it with `Hoglah(config=...)`
(or env), then:

- `submit(prompt=... | messages=..., model=..., tags=..., metadata=..., callback=...,
  callback_url=..., timeout_seconds=..., priority=...) -> job_id` — enqueue work,
  return a [job id](models.md) immediately (ADR-007: UUID4).
- `submit_embedding(text, model=...) -> job_id` — convenience for `kind="embed"`
  (see [job kinds](../concepts/job-kinds.md)).
- `get(job_id) -> JobResult` — current state from the [store](storage.md).
- `wait(job_id, timeout=...) -> JobResult` — block until terminal (polling).
- `list(...)`, `cancel(job_id)`, queue stats.

**`HoglahConfig`** carries the backend selection, `db_path`/Mongo settings,
`output_dir`, concurrency, callback timeouts/retries, and the broker flags
(`kafka_enabled` / `rabbitmq_enabled` / `redis_enabled` + connection settings).

**`start_worker`** decides the topology (see [decoupled topology](../concepts/decoupled-topology.md)):
`True` runs the internal asyncio worker loop (ADR-003: a semaphore enforces
`concurrency`, default 1) and interrupted-job recovery; `False` is a pure
submitter. The synchronous-feeling `submit` returns instantly while execution is
async internally (ADR-012).

Inference goes through an [adapter](adapters.md); results are collected by any of
the [delivery paths](../concepts/result-delivery.md).
