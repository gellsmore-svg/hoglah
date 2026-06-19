---
type: CLI Command
title: hoglah submit / pull
description: Enqueue a generation, chat, or embedding job (returns a job id immediately), and pre-pull a model into Ollama before submitting with --real.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/cli.py
tags: [hoglah, cli, submit]
timestamp: 2026-06-19T00:00:00Z
---

# `hoglah submit` / `hoglah pull`

- **`hoglah submit`** — enqueue a [job](../modules/models.md) and print its id (and,
  with `--wait`, its result). Selects the [store](../modules/storage.md) with
  `--db`, the model with `--model`, and may attach `--tags` / metadata. The job is
  executed by a separate [`run` worker](daemons.md) (or, with no daemon, can be
  processed inline by some flows). See [job kinds](../concepts/job-kinds.md) for
  generate vs chat vs embed.
- **`hoglah pull`** — ensure a model is pulled into Ollama before submitting with
  `--real` (`--ollama-host` selects the server). Without `--real` the
  [stub](../modules/adapters.md) does nothing.

For programmatic submission, use the [client](../modules/client.md) `submit()` /
`submit_embedding()`; to submit over a broker, the
[messaging submitter](../modules/messaging-submitter.md).
