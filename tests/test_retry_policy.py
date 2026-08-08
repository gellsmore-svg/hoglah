"""G2 — richer RetryPolicy (max, backoff, jitter, retry_on)."""

from __future__ import annotations

import random
import tempfile
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus, RetryPolicy
from hoglah.models import (
    JobRequest,
    classify_error,
    effective_retry_policy,
)


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-retry-")
    return Path(td) / "test.db"


def test_retry_policy_defaults_and_validation():
    p = RetryPolicy()
    assert p.max_retries == 2
    assert p.retry_on == ("transient",)
    assert p.delay_for_attempt(0) == 1.0
    assert p.delay_for_attempt(1) == 2.0
    assert p.delay_for_attempt(2) == 4.0
    assert p.delay_for_attempt(10) == 10.0  # capped at max_delay

    with pytest.raises(ValueError, match="max_retries"):
        RetryPolicy.from_any({"max_retries": -1})
    with pytest.raises(ValueError, match="jitter"):
        RetryPolicy.from_any({"jitter": 1.5})
    with pytest.raises(ValueError, match="unknown retry_on"):
        RetryPolicy.from_any({"retry_on": ["nope"]})


def test_jitter_uses_equal_jitter_range():
    p = RetryPolicy(base_delay=10.0, backoff_factor=1.0, max_delay=100.0, jitter=0.5)
    rng = random.Random(0)
    samples = [p.delay_for_attempt(0, rng=rng) for _ in range(50)]
    # With j=0.5, range is [5, 15]
    assert min(samples) >= 5.0 - 1e-9
    assert max(samples) <= 15.0 + 1e-9
    assert min(samples) < 10.0 < max(samples)


def test_classify_error_classes():
    assert "connection" in classify_error(ConnectionError("connection refused"))
    assert "transient" in classify_error(ConnectionError("connection refused"))
    assert "oom" in classify_error(RuntimeError("CUDA out of memory"))
    assert "transient" not in classify_error(RuntimeError("CUDA out of memory"))
    assert "server" in classify_error(RuntimeError("503 service unavailable"))
    assert "rate_limit" in classify_error(RuntimeError("HTTP 429 too many requests"))
    # Context errors are not transient
    classes = classify_error(ValueError("context length exceeded"))
    assert "transient" not in classes


def test_classify_error_negative_cases_review_h1():
    """False-positive / false-negative cases from the 2026-08-08 review."""
    # Permanent model pull failure must not be retried as transient, even with 500.
    permanent = classify_error(
        RuntimeError("pull model manifest: file does not exist (status code: 500)")
    )
    assert "transient" not in permanent
    assert "server" not in permanent

    # Byte count containing "4004" must not strip transient via a bare "400" match.
    reset = classify_error(ConnectionError("connection reset by peer after 4004 bytes"))
    assert "connection" in reset
    assert "transient" in reset

    # Real Ollama OOM phrasing.
    assert "oom" in classify_error(RuntimeError("memory layout cannot be allocated"))

    # "oom" inside a model name must not classify as OOM.
    assert "oom" not in classify_error(RuntimeError("model not found: mushroom-classifier:7b"))


def test_should_retry_respects_retry_on():
    default = RetryPolicy()
    assert default.should_retry(ConnectionError("connection reset"))
    assert not default.should_retry(RuntimeError("CUDA out of memory"))
    assert not default.should_retry(TimeoutError("job"), job_timeout=True)

    oom_only = RetryPolicy(retry_on=("oom",))
    assert oom_only.should_retry(RuntimeError("out of memory"))
    assert not oom_only.should_retry(ConnectionError("connection refused"))

    timeout_opt_in = RetryPolicy(retry_on=("timeout",))
    assert timeout_opt_in.should_retry(TimeoutError("x"), job_timeout=True)

    never = RetryPolicy(retry_on=("none",))
    assert not never.should_retry(ConnectionError("connection refused"))


def test_effective_retry_policy_from_legacy_max_retries_zero():
    """max_retries=0 must mean one attempt — not falsy-coerced to 2."""
    req = JobRequest(prompt="x", model="m", max_retries=0)
    p = effective_retry_policy(req)
    assert p.max_retries == 0


def test_submit_persists_retry_policy():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    jid = h.submit(
        prompt="hi",
        model="m",
        retry_policy=RetryPolicy(
            max_retries=4,
            base_delay=0.5,
            jitter=0.2,
            retry_on=("connection", "oom"),
        ),
    )
    row = h._store.get(jid)
    assert row is not None
    pol = row["request"]["retry_policy"]
    assert pol["max_retries"] == 4
    assert pol["base_delay"] == 0.5
    assert pol["jitter"] == 0.2
    assert set(pol["retry_on"]) == {"connection", "oom"}
    assert row["request"]["max_retries"] == 4
    h.close()


def test_worker_retries_transient_then_succeeds():
    from hoglah.adapters import StubAdapter

    class Flaky(StubAdapter):
        attempts = 0

        async def run(self, request):
            type(self).attempts += 1
            if type(self).attempts < 3:
                raise ConnectionError("connection reset by peer")
            return await super().run(request)

    db = _temp_db()
    h = Hoglah(
        config={"db_path": db},
        adapter=Flaky(),
        start_worker=True,
    )
    try:
        jid = h.submit(
            prompt="hi",
            model="m",
            retry_policy={
                "max_retries": 3,
                "base_delay": 0.01,
                "max_delay": 0.05,
                "backoff_factor": 1.0,
                "retry_on": ["transient"],
            },
        )
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        assert Flaky.attempts == 3
    finally:
        h.close()


def test_worker_does_not_retry_oom_by_default():
    from hoglah.adapters import StubAdapter

    class OomOnce(StubAdapter):
        attempts = 0

        async def run(self, request):
            type(self).attempts += 1
            raise RuntimeError("CUDA out of memory")

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, adapter=OomOnce(), start_worker=True)
    try:
        jid = h.submit(prompt="hi", model="m", max_retries=5)
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.FAILED
        assert "out of memory" in (res.error or "").lower()
        assert OomOnce.attempts == 1
    finally:
        h.close()


def test_worker_retries_oom_when_opted_in():
    from hoglah.adapters import StubAdapter

    class OomThenOk(StubAdapter):
        attempts = 0

        async def run(self, request):
            type(self).attempts += 1
            if type(self).attempts == 1:
                raise RuntimeError("CUDA out of memory")
            return await super().run(request)

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, adapter=OomThenOk(), start_worker=True)
    try:
        jid = h.submit(
            prompt="hi",
            model="m",
            retry_policy={
                "max_retries": 2,
                "base_delay": 0.01,
                "max_delay": 0.05,
                "retry_on": ["oom"],
            },
        )
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        assert OomThenOk.attempts == 2
    finally:
        h.close()


def test_max_retries_zero_is_one_attempt():
    from hoglah.adapters import StubAdapter

    class AlwaysFail(StubAdapter):
        attempts = 0

        async def run(self, request):
            type(self).attempts += 1
            raise ConnectionError("connection refused")

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, adapter=AlwaysFail(), start_worker=True)
    try:
        jid = h.submit(prompt="hi", model="m", max_retries=0)
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.FAILED
        assert AlwaysFail.attempts == 1
    finally:
        h.close()
