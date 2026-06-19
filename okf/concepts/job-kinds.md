---
type: Concept
title: Job kinds — generate, chat, embed
description: One durable queue serves three job kinds — text generation (prompt), chat (messages), and embeddings — each normalised to the right Ollama endpoint behind a unified job model.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/docs/architecture-decisions.md
tags: [hoglah, jobs, generate, chat, embeddings, adr-013]
timestamp: 2026-06-19T00:00:00Z
---

# Job kinds

A single queue handles three kinds of work, all carried by the same
[`JobRequest`](../modules/models.md) and executed through the same
[adapter](../modules/adapters.md):

- **`generate`** — text generation from a `prompt` (Ollama `/api/generate`). The
  default kind.
- **chat** — a multi-turn `messages` list (Ollama `/api/chat`). Submitted via the
  `messages=` argument; ADR-004 normalises `prompt` vs `messages` to the right
  endpoint behind one job model.
- **`embed` (ADR-013)** — embeddings as a first-class kind. The input text rides in
  `prompt`; the worker routes it to `adapter.embed()` (Ollama `/api/embed`); the
  result carries `embedding: list[float]` + `embedding_dim` with `output=None`.
  Non-finite vectors (NaN/Inf — a known instability in some models like bge-m3)
  raise rather than returning a bogus vector. Convenience: `submit_embedding(text, model=...)`.

Routing embeddings through the same durable queue/daemon (rather than calling
Ollama directly) is what lets a separate app — e.g. Tirzah's embedding adapter —
share one execution path for generation and embeddings. See
[result delivery](result-delivery.md) for how the vector comes back.
