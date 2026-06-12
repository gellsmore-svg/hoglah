# Contributing to Hoglah

This file records the working conventions for collaboration between the operator, codex, and other agents on the Hoglah repository. It is intentionally short. If something is unclear, prefer adding to this file over repeating the same discussion.

## Roles

- **Operator** — the human owner of the project. Makes architectural and scope decisions.
- **Codex** — typically runs with elevated privileges in the environment and handles `git push` to the remote when needed.
- **Claude / other agents** — implement, document, and propose within the operator's stated direction. Commit locally; do not assume push rights.

## Restart-doc discipline (highly recommended for long-running work)

Two files carry project state across sessions:

- `.restart.md` — *canonical current state*. Tight, human-readable summary of status, next step, open decisions, and key files. Surgical edits preferred. Save pre-edit snapshots (`.restart.md.<context>-<date>.bak`) before large rewrites.
- `.session-log.md` — *how we got here*. Append-only chronological narrative. Use dated agent headings. Never rewrite history; add corrections as new entries.

## Cadence & chunking

Break multi-step work into visible, named chunks. Typical pattern in responses:

> "Chunk N — short description. Doing X."
> [tool calls]
> "Chunk N done. Next: chunk N+1."

## Standing rules (derived from requirements and family conventions)

- V1 scope is deliberately narrow: reliable local Ollama job queue + Python API + persistence + basic callbacks. Ask before expanding beyond the Non-Goals in `docs/requirements-v1.0.md`.
- Keep the implementation lightweight. Prefer stdlib + a small number of well-chosen dependencies (ollama client, pydantic, typer or argparse for CLI, etc.).
- Source preservation and auditability matter: job inputs, parameters, and final outputs should remain inspectable.
- Callbacks and side effects must be isolated; a failing callback must never lose the job result.
- Document decisions in `docs/architecture-decisions.md` (append-only table style) or as new `.session-log.md` entries.
- Match the style and quality bar of sister domains (Mahalath, Tirzah) for docs, tests, and packaging.

## Code conventions (initial)

- Python 3.11+
- Source under `src/hoglah/`
- Tests under `tests/`
- Use `pyproject.toml` (setuptools) for packaging and CLI entry points
- Configuration: support constructor overrides + environment variables + small config file
- Runtime state (default): `~/.hoglah/` for the SQLite DB and logs (gitignored)
- All public surfaces (submit parameters, JobResult, status enum, etc.) should be clearly typed

## Working with the domains family

Hoglah lives alongside AMS, context, healing, Mahalath, Tirzah, Relational-Substrate, etc. under `~/domains/`. The projects share a local-first, privacy-focused, restart-resilient philosophy but are intentionally **runtime independent**. Cross-pollination via reading each other's code and docs is encouraged; direct imports between projects should be a deliberate later decision recorded in architecture docs.

Hoglah's job queue may eventually be useful to other domains (e.g. background work in Tirzah or Mahalath REM jobs), but any integration is future work and must respect each project's independence.
