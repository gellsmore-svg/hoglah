# Contributing to Hoglah

Thanks for your interest in Hoglah. This guide covers local development.

## Development setup

```bash
git clone https://github.com/gellsmore-svg/hoglah
cd hoglah
python -m venv .venv
.venv/bin/pip install -e ".[dev,cli]"
```

Optional backends/transports add extras: `.[mongo]`, `.[kafka]`, `.[rabbitmq]`,
`.[redis]`.

## Running tests

```bash
pytest                 # full suite; the default adapter needs no Ollama
ruff check src tests   # lint
```

Integration tests that need external services are gated behind environment flags
and skipped by default:

- `RUN_OLLAMA_TESTS=1` — a local Ollama with a small model (e.g. `gemma3:1b`).
- `RUN_MONGO_TESTS=1` — a MongoDB at `mongodb://localhost:27017`.
- `RUN_KAFKA_TESTS=1` / `RUN_RABBITMQ_TESTS=1` / `RUN_REDIS_TESTS=1` — a local
  broker for the corresponding bridge.

Please keep the default (unflagged) suite green and ruff-clean, and add a test for
any behaviour change.

## Code conventions

- Python 3.11+
- Source under `src/hoglah/`, tests under `tests/`
- Public surfaces (submit parameters, `JobResult`, status enum, config) are typed
- Configuration via constructor overrides + `HOGLAH_*` environment variables
- Keep dependencies light; new runtime deps should be optional extras where possible
- Architecture decisions are recorded (append-only) in
  `docs/architecture-decisions.md`

## Releasing (maintainers)

Releases are automated. Bump the version in `pyproject.toml`, update
`CHANGELOG.md`, commit, then push a tag:

```bash
git tag vX.Y.Z && git push origin vX.Y.Z
```

The tag triggers `.github/workflows/release.yml`, which builds the wheel + sdist,
creates the GitHub Release, and publishes to PyPI via OIDC trusted publishing.

## Reporting issues

Bugs and questions: <https://github.com/gellsmore-svg/hoglah/issues>. Security
issues: please report privately — see [SECURITY.md](SECURITY.md).
