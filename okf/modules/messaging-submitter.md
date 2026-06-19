---
type: Module
title: Messaging submitter (submitter side)
description: The mirror of the worker bridge — MessagingSubmitter publishes a job-request over a broker and blocks for the matching result by correlation_id, reusing the bridge's exact wire formats, with Kafka/RabbitMQ/Redis transports.
resource: https://github.com/gellsmore-svg/hoglah/blob/main/src/hoglah/messaging_submitter.py
tags: [hoglah, messaging, submitter, kafka, rabbitmq, redis]
timestamp: 2026-06-19T00:00:00Z
---

# Messaging submitter (`messaging_submitter.py`)

The **submitter side** — the mirror of the [worker bridge](messaging-bridges.md).
A client (e.g. Tirzah, Milcah) uses it to dispatch a job **over a broker** instead
of writing to the shared SQLite store: publish a job-request message, then block
until the matching result comes back.

- **`MessagingSubmitter.submit(kind, prompt, model, timeout, ...)`** — builds the
  request, publishes it, awaits the result by `correlation_id`, returns the result
  dict (raises on timeout).
- It uses the bridge's **exact wire formats** (`build_request_message` is accepted
  by the bridge's `parse_input_message`; results decode via `build_result_message`),
  so a running `*-bridge` serves these requests unchanged.
- A tiny `SubmitterTransport` interface (publish request + await result) has
  Kafka / RabbitMQ / Redis implementations (import-guarded). Build one with
  `make_submitter_transport(transport, ...)`.

Each transport handles request/reply correlation natively: Kafka uses a throwaway
consumer group scanning the results topic; RabbitMQ uses an exclusive reply queue
(classic RPC); Redis captures the results-stream tail before `XADD` then `XREAD`s
from it. The orchestration is broker-neutral and unit-tested with an in-memory
fake. Downstream consumers: Tirzah's `hoglah_transport` and Milcah's
`HoglahExtractor` both submit through this.
