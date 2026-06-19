---
type: Concept Index
title: Hoglah Concepts
description: The load-bearing design ideas behind Hoglah — the decoupled submitter/worker topology, the result-delivery paths, end-to-end crash safety, and the job kinds.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/docs/architecture-decisions.md
tags: [hoglah, concepts, architecture]
timestamp: 2026-06-19T00:00:00Z
---

# Concepts

The design decisions are recorded in
[`docs/architecture-decisions.md`](https://github.com/gellsmore-svg/hoglah/blob/main/docs/architecture-decisions.md)
(ADR-001 … ADR-020). These docs distil the load-bearing ones.

- **[Decoupled topology](decoupled-topology.md)** — pure submitters feed a shared
  store; a separate worker daemon executes (ADR-016).
- **[Result delivery](result-delivery.md)** — poll/`wait`, in-process callback,
  HTTP callback, output file, and broker reply (ADR-006/014/015).
- **[Crash safety](crash-safety.md)** — idempotent consumer + transactional outbox
  give exactly-once *effect* across the messaging bridges (ADR-018/019/020).
- **[Job kinds](job-kinds.md)** — one queue for `generate`, chat, and `embed`
  (ADR-004/013).
