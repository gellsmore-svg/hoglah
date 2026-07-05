---
type: Module
title: web
description: The read-only web queue monitor behind `hoglah serve` — status cards that double as filters, a live-polling jobs table with step names, and a job detail page showing the full In→Out.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/web.py
tags: [hoglah, module, web, monitor]
timestamp: 2026-07-05T00:00:00Z
---

# web

`create_app(db_path=None)` builds a single-file FastAPI app over the same
JobStore as the CLI, opened through a non-working client
(`Hoglah(start_worker=False)`) — it observes the queue, never executes.
`hoglah serve` runs it (default `127.0.0.1:8781`; `web` extra: fastapi +
uvicorn). Strictly read-only; maintenance stays in the CLI.

Pages: the dashboard (`/`) with status-count cards that double as filters and
a jobs table (job/status/model/**step**/tags/parent/age/preview) refreshed by
4s polling; the job detail (`/jobs/{id}`) with status, step, timings, usage,
truncation, the **full input** (prompt or role-tagged messages, read from the
persisted request) and full output/error, plus a `galeed trace --call` pointer.
JSON API: `/api/summary`, `/api/jobs/{id}`.
