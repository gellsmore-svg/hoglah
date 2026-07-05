---
type: Module Index
title: Hoglah Modules
description: The code that makes up Hoglah — the client, the job model, pluggable storage, inference adapters, the worker-side messaging bridges, and the submitter-side messaging client.
resource: https://github.com/gellsmore-svg/hoglah/tree/main/src/hoglah
tags: [hoglah, modules, code]
timestamp: 2026-06-19T00:00:00Z
---

# Modules

- **[Client](client.md)** (`client.py`) — the `Hoglah` class + `HoglahConfig`: the
  public API and (optionally) the worker loop.
- **[Job model](models.md)** (`models.py`) — `JobRequest`, `JobResult`, `JobStatus`.
- **[Storage](storage.md)** (`store.py`, `mongo_store.py`) — the `JobStore` seam:
- **[web](web.md)** — the read-only queue monitor behind `hoglah serve`.
- **[tracing](tracing.md)** — the Galeed witness: spine events + llm_calls capture.
  SQLite (default) and MongoDB backends.
- **[Adapters](adapters.md)** (`adapters.py`) — `OllamaAdapter` (real) and
  `StubAdapter` (deterministic default) behind `BaseAdapter`.
- **[Messaging bridges](messaging-bridges.md)** (`kafka_bridge.py`, `rabbitmq.py`,
  `redis_streams.py`) — the worker-side `MessageBridge` + `MessageTransport`
  implementations.
- **[Messaging submitter](messaging-submitter.md)** (`messaging_submitter.py`) —
  the submitter-side client that publishes a request and awaits the result over a
  broker.
