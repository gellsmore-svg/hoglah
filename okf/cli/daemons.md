---
type: CLI Command
title: hoglah run / kafka-bridge / rabbitmq-bridge / redis-bridge
description: The long-running worker and bridge daemons — `run` executes queued jobs against Ollama; the `*-bridge` commands additionally consume job requests from Kafka, RabbitMQ, or Redis Streams.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/cli.py
tags: [hoglah, cli, daemon, worker, bridge]
timestamp: 2026-06-19T00:00:00Z
---

# Worker and bridge daemons

These run in the foreground until interrupted. All take `--real` (use real
[Ollama](../modules/adapters.md), default is the stub), `--ollama-host`, and `--db`
(the shared [store](../modules/storage.md)).

- **`hoglah run`** — the worker daemon: claim queued [jobs](../modules/models.md)
  and execute them (`--concurrency` / `-c`). This is the executor in the
  [decoupled topology](../concepts/decoupled-topology.md); only it recovers
  interrupted jobs (ADR-016).
- **`hoglah kafka-bridge`** — run the worker **plus** the Kafka
  [messaging bridge](../modules/messaging-bridges.md): `--bootstrap-servers`,
  `--input-topic`, `--results-topic`.
- **`hoglah rabbitmq-bridge`** — same, for RabbitMQ: `--url`, `--input-queue`,
  `--results-queue`, `--prefetch`.
- **`hoglah redis-bridge`** — same, for Redis Streams: `--url`, `--input-stream`,
  `--results-stream`, `--group`, `--consumer-name`.

A bridge daemon both executes jobs and serves requests arriving over its broker;
the matching [messaging submitter](../modules/messaging-submitter.md) transport
must publish to the same topics/queues/streams (defaults `hoglah-jobs` /
`hoglah-results`).
