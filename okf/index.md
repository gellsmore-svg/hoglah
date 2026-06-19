---
type: Project
title: Hoglah
description: A lightweight, local-first job queue for Ollama — submit generate/chat/embedding work asynchronously, run it on a durable background queue, and collect results by poll, callback, HTTP push, output file, or a messaging broker.
resource: https://github.com/gellsmore-svg/hoglah
tags: [hoglah, job-queue, ollama, local-first, llm]
timestamp: 2026-06-19T00:00:00Z
---

# Hoglah

Hoglah is a lightweight, local-first **job queue for [Ollama](https://ollama.com)**.
An application submits LLM inference work — text generation, chat, or embeddings —
asynchronously: `submit()` returns a job id immediately, the work runs on a durable
background worker, and the result is collected by polling, an in-process callback,
an HTTP callback, a written output file, or a messaging broker.

It targets resource-constrained single-machine setups (where only one or a few
inferences can run at once) but scales out to multiple workers and machines when
pointed at a shared backend.

This bundle is an [Open Knowledge Format](https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing)
description of Hoglah's concepts, modules, and CLI.

## Map

- **[Concepts](concepts/index.md)** — the load-bearing design ideas: the decoupled
  submitter/worker topology, result delivery, crash safety, and job kinds.
- **[Modules](modules/index.md)** — the code: the client, the job model, storage
  backends, inference adapters, and the messaging bridges + submitter.
- **[CLI](cli/index.md)** — the `hoglah` commands: submit, run a worker, run a
  bridge, and inspect the queue.

## At a glance

- Storage: **SQLite by default** (zero setup), MongoDB optional — see [storage](modules/storage.md).
- Inference: a deterministic **stub by default**; real Ollama is opt-in — see [adapters](modules/adapters.md).
- Brokers: **Kafka, RabbitMQ, Redis Streams** bridges, all crash-safe — see [messaging bridges](modules/messaging-bridges.md).
- Current version: 0.7.0 (Apache-2.0).
