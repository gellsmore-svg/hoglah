# AGENTS.md — working with Hoglah

Short orientation for coding agents editing or calling this repository.

## What this package is

**Hoglah** = durable **Ollama job queue** (generate / chat / embed).  
Default adapter is a **safe stub**; real Ollama is opt-in (`use_real=True` / `--real`).

## Docs to read

| Need | File |
|---|---|
| Full human guide | [docs/user-guide.md](docs/user-guide.md) |
| Agent contracts / invariants | [docs/ai-user-guide.md](docs/ai-user-guide.md) |
| Docs index | [docs/README.md](docs/README.md) |
| Living roadmap / gaps | [docs/feature-comparison-and-gaps.md](docs/feature-comparison-and-gaps.md) |
| ADRs | [docs/architecture-decisions.md](docs/architecture-decisions.md) |
| Product overview | [README.md](README.md) |
| Structured knowledge (OKF) | [okf/index.md](okf/index.md) |

## Layout

```text
src/hoglah/     # library (client, store, adapters, bridges, web, metrics)
tests/          # pytest; gated integration via RUN_*_TESTS=1
docs/           # guides + design history
okf/            # Open Knowledge Format bundle
examples/       # runnable samples
```

## Dev commands

```bash
pip install -e ".[dev,cli]"
pytest
ruff check src tests
```

## Hard rules when changing code

1. Keep **stub default**; do not make real Ollama the default path.  
2. Preserve **JobStore** protocol parity (SQLite + Mongo) for new fields.  
3. **Leases / cancel / claim** must stay multi-worker safe.  
4. Public API (`submit` kwargs, `JobResult`, status enum) stays typed and documented.  
5. Update **CHANGELOG**, gap list, and both user guides when shipping user-visible behaviour.  
6. Unit tests must pass **without** external services.

## Version

See `pyproject.toml` (`0.10.2` at time of writing).
