---
type: CLI Index
title: Hoglah CLI
description: The `hoglah` command-line interface — submit work, run a worker daemon, run a messaging bridge, and inspect or prune the queue.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/cli.py
tags: [hoglah, cli]
timestamp: 2026-06-19T00:00:00Z
---

# CLI (`hoglah`)

A typer app (`pip install "hoglah[cli]"`). Most commands take `--db` to select the
SQLite store; daemon/bridge commands take `--real` + `--ollama-host` to use real
Ollama (default is the safe [stub](../modules/adapters.md)).

- **[Submit](submit.md)** — `submit` work onto the queue; `pull` a model first.
- **[Daemons](daemons.md)** — `run` a worker; `kafka-bridge` / `rabbitmq-bridge` /
  `redis-bridge` to consume from a broker.
- **[Inspect](inspect.md)** — `list`/`ps`, `stats`, `status`, `wait`, `info`,
  `cancel`, `clear`/`rm`, `doctor`, `models`, `show`.

The CLI mirrors the [client API](../modules/client.md); the running model is the
shared queue, so a `submit` from one process and a `run` worker in another
cooperate via the [decoupled topology](../concepts/decoupled-topology.md).
