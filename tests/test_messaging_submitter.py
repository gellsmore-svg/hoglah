"""Submitter-side messaging client tests (broker-free).

The orchestration is exercised with an in-memory fake transport, and the wire
formats are cross-checked against the *worker* bridge's own parser/builder so the
two sides provably agree without standing up a broker.
"""

from __future__ import annotations

import pytest

from hoglah.kafka_bridge import build_result_message, parse_input_message
from hoglah.messaging_submitter import (
    MessagingSubmitter,
    MessagingSubmitError,
    build_request_message,
    parse_result_message,
)


class _FakeTransport:
    """Records the published request and replies with a canned result, echoing
    the correlation_id the way a real bridge would."""

    def __init__(self, *, result: dict | None = None, reply_to: str | None = "results"):
        self._reply_to = reply_to
        self._result = result
        self.published: list[tuple[bytes, str]] = []

    def reply_destination(self):
        return self._reply_to

    def publish_request(self, body, *, correlation_id):
        self.published.append((body, correlation_id))

    def await_result(self, correlation_id, timeout):
        if self._result is None:
            return None
        return {**self._result, "correlation_id": correlation_id}

    def close(self):
        pass


def test_submit_builds_request_and_returns_result() -> None:
    transport = _FakeTransport(result={"status": "completed", "output": "hi", "job_id": "j1"})
    sub = MessagingSubmitter(transport)
    result = sub.submit(kind="generate", prompt="hello", model="gemma3:1b", timeout=5)
    assert result["status"] == "completed"
    assert result["output"] == "hi"
    # exactly one request published, carrying the correlation id returned in the result
    assert len(transport.published) == 1
    body, corr = transport.published[0]
    assert result["correlation_id"] == corr


def test_submit_raises_on_timeout() -> None:
    sub = MessagingSubmitter(_FakeTransport(result=None))
    with pytest.raises(MessagingSubmitError, match="no result"):
        sub.submit(kind="generate", prompt="hello", model="m", timeout=0.2)


def test_request_message_is_accepted_by_the_worker_bridge_parser() -> None:
    # The submitter's request must be parseable by the bridge that will serve it.
    body = build_request_message(
        kind="generate",
        prompt="what is a vorton?",
        model="gemma3:1b",
        correlation_id="abc123",
        reply_to="hoglah-results",
        tags=["tirzah"],
        metadata={"source": "tirzah"},
    )
    parsed = parse_input_message(body)
    assert parsed.correlation_id == "abc123"
    assert parsed.reply_to == "hoglah-results"
    assert parsed.request.model == "gemma3:1b"
    assert parsed.request.prompt == "what is a vorton?"
    assert parsed.request.kind == "generate"


def test_embed_request_is_accepted_by_the_bridge_parser() -> None:
    body = build_request_message(
        kind="embed", prompt="vorton", model="nomic-embed-text:latest", correlation_id="e1"
    )
    parsed = parse_input_message(body)
    assert parsed.request.kind == "embed"
    assert parsed.request.prompt == "vorton"


def test_result_message_roundtrips_with_the_bridge_builder() -> None:
    # What the bridge produces, the submitter must decode.
    raw = build_result_message(
        {"job_id": "j9", "status": "completed", "output": "ok", "embedding": None}, "corr-9"
    )
    decoded = parse_result_message(raw)
    assert decoded["correlation_id"] == "corr-9"
    assert decoded["job_id"] == "j9"
    assert decoded["status"] == "completed"
    assert decoded["output"] == "ok"


def test_parse_result_message_tolerates_garbage() -> None:
    assert parse_result_message(b"not json") is None
    assert parse_result_message(b"[1,2,3]") is None
