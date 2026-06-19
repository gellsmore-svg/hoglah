---
type: Module
title: Job model
description: The data records — JobRequest (the submitted work), JobResult (the terminal outcome, including output or embedding and truncation metadata), and the JobStatus lifecycle.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/models.py
tags: [hoglah, models, jobresult, jobstatus]
timestamp: 2026-06-19T00:00:00Z
---

# Job model (`models.py`)

- **`JobRequest`** — the submitted work: `kind` (`generate` | `embed` | chat via
  `messages`), `prompt`/`messages`, `model`, `tags`, `metadata`, `priority`,
  `timeout_seconds`, `max_retries`, `callback_url`, `parent_job_id` (stored for
  provenance; no dependency orchestration in V1, ADR-008), and `correlation_id`
  (used by the [bridges](messaging-bridges.md) for idempotent enqueue).
- **`JobResult`** (ADR-010) — the terminal outcome: `id`, `status`, `output`
  (text; `None` for embeddings), `model`, `error`, `tags`, `metadata`,
  `parent_job_id`, timings, and **truncation metadata** (`truncated`,
  `truncation_reason`) per ADR-009 (a job succeeds on truncation and reports it
  rather than failing). For [embed jobs](../concepts/job-kinds.md): `embedding:
  list[float]` + `embedding_dim`.
- **`JobStatus`** — the lifecycle enum: `queued` → `processing` → terminal
  (`completed` | `failed` | `cancelled`). Retries use exponential backoff on
  *transient* errors only (ADR-011); `timeout_seconds` marks a job failed.

These records are what the [client](client.md) returns, the [store](storage.md)
persists, and the [messaging](messaging-bridges.md) wire formats serialise.
