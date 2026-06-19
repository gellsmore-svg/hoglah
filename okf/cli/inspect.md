---
type: CLI Command
title: hoglah inspect & maintenance commands
description: Read and prune the queue — list/ps, stats, status, wait, info, show, cancel, clear/rm, doctor, and models.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/cli.py
tags: [hoglah, cli, inspect, maintenance]
timestamp: 2026-06-19T00:00:00Z
---

# Inspect & maintenance

Read and tend the [store](../modules/storage.md):

- **`list`** (alias **`ps`**) — recent [jobs](../modules/models.md), filterable by
  `--status`.
- **`stats`** — queue counts by status and totals.
- **`status <job_id>`** / **`show <job_id>`** — one job's state / full result.
- **`wait <job_id>`** — block until the job is terminal (the CLI form of the
  client's [`wait`](../modules/client.md)).
- **`info`** — instance config, adapter in use, and queue stats.
- **`cancel <job_id>`** — best-effort cancellation (ADR-011).
- **`clear`** / **`rm <job_id>`** — prune completed jobs / delete one.
- **`doctor`** — environment + connectivity diagnostics.
- **`models`** — list models known to the adapter (`--real` queries Ollama).

> A live, auto-refreshing **queue monitor** (a watch view over `stats`/`list`) is
> planned but not yet built — these are the current point-in-time inspection
> commands.
