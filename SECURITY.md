# Security Policy

## Supported versions

Hoglah is pre-1.0 and ships from a single line of development. Security fixes are
made against the latest released version on PyPI; please upgrade to the latest
`0.x` before reporting.

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately via GitHub's
[private vulnerability reporting](https://github.com/gellsmore-svg/hoglah/security/advisories/new)
("Report a vulnerability" under the repository's **Security** tab). This keeps the
details private until a fix is available.

When reporting, please include:

- the Hoglah version (`hoglah --version`),
- the storage backend and messaging transport in use (see `hoglah doctor`),
- a description and, ideally, a minimal reproduction.

You can expect an acknowledgement; once confirmed and fixed, a patched release is
cut to PyPI and the advisory is published with credit (unless you prefer to remain
anonymous).

## Handling credentials

Several optional backends and transports take connection strings that may embed
credentials (`mongo_uri`, `redis_url`, Kafka/RabbitMQ URLs). Hoglah keeps these
out of its diagnostic surfaces on purpose: `hoglah doctor` and the config view
embedded in result metadata report the **backend name and transport flags only**,
never the connection URLs. Still, scrub your own logs before attaching them to any
report.
