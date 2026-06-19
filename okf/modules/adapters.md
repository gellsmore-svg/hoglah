---
type: Module
title: Inference adapters
description: The seam to the model runtime — OllamaAdapter performs real generate/chat/embed calls, StubAdapter is the deterministic default that needs no server; real inference is explicitly opt-in.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/adapters.py
tags: [hoglah, adapters, ollama, stub, safe-by-default]
timestamp: 2026-06-19T00:00:00Z
---

# Inference adapters (`adapters.py`)

The worker executes a [job](models.md) through an adapter behind `BaseAdapter`
(generate / chat / embed):

- **`StubAdapter`** — the **default**. Deterministic, no server required: returns
  canned text and a deterministic finite embedding vector. This is why Hoglah is
  "safe by default" and why tests need no Ollama.
- **`OllamaAdapter`** — real inference against an Ollama server (generate, chat,
  `/api/embed`, model listing). **Opt-in**: enabled with `use_real=True` /
  `HOGLAH_USE_REAL_ADAPTER=1` / the CLI `--real` flag, with `--ollama-host` /
  `ollama_host` selecting the server.

The adapter also handles context limits: it auto-detects a model's window and lets
truncation surface as [`JobResult`](models.md) metadata rather than failing the
job (ADR-009). The official `ollama` package is preferred with an httpx-style
fallback (ADR-005).

Selecting the adapter is orthogonal to [storage](storage.md) and to the
[delivery](../concepts/result-delivery.md) path.
