"""Basic tests for the Hoglah client + SQLite store (Chunk 1).

These tests exercise persistence, restart behavior (new Hoglah instances),
callback registry re-delivery, submit/list/get/cancel/wait, etc.

They do NOT require a running Ollama instance.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.models import JobResult


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-test-")
    return Path(td) / "test.db"


def test_submit_and_get_basic():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    job_id = h.submit(
        prompt="Hello world",
        model="gemma:2b",
        tags=["test"],
        priority=5,
    )

    assert job_id
    res = h.get(job_id)
    assert isinstance(res, JobResult)
    assert res.status == JobStatus.QUEUED
    assert res.model == "gemma:2b"
    assert "test" in res.tags

    # New instance (simulates restart) should still see the job
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    res2 = h2.get(job_id)
    assert res2.status == JobStatus.QUEUED


def test_list_and_filter():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    j1 = h.submit(prompt="one", model="gemma:2b", tags=["a"])
    j2 = h.submit(prompt="two", model="gemma:2b", tags=["b"])

    all_jobs = h.list(limit=10)
    assert len(all_jobs) == 2

    a_jobs = h.list(tags=["a"])
    assert len(a_jobs) == 1
    assert a_jobs[0].job_id == j1


def test_cancel_and_wait_timeout():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    job_id = h.submit(prompt="will be cancelled", model="gemma:2b")

    assert h.cancel(job_id) is True
    res = h.get(job_id)
    assert res.status == JobStatus.CANCELLED

    # Second cancel should be no-op
    assert h.cancel(job_id) is False

    # wait should return immediately for terminal job
    res2 = h.wait(job_id)
    assert res2.status == JobStatus.CANCELLED

    # Timeout on a still-queued job (we never start a worker in this test)
    job2 = h.submit(prompt="still queued", model="gemma:2b")
    with pytest.raises(TimeoutError):
        h.wait(job2, timeout=0.1)


def test_named_callback_registry_restart_delivery():
    """Test that named callbacks are re-delivered on new Hoglah() instance
    for jobs that reached terminal state while "down".
    """
    db = _temp_db()

    calls: list[JobResult] = []

    def my_handler(result: JobResult):
        calls.append(result)

    # First instance: submit with named key + mark it completed manually
    h1 = Hoglah(config={"db_path": db}, callbacks={"my_handler": my_handler})
    job_id = h1.submit(
        prompt="test callback",
        model="gemma:2b",
        callback="my_handler",  # str means named registry key
    )

    # Simulate the job having completed while the process was "down"
    # (we directly poke the store for the test)
    fake_result = JobResult(
        job_id=job_id,
        status=JobStatus.COMPLETED,
        output="done",
        model="gemma:2b",
    )
    h1._store.set_result(job_id, fake_result)  # type: ignore[attr-defined]

    # New instance with the same registry should re-deliver
    calls.clear()
    Hoglah(config={"db_path": db}, callbacks={"my_handler": my_handler})

    assert len(calls) == 1
    assert calls[0].job_id == job_id
    assert calls[0].status == JobStatus.COMPLETED


def test_direct_callback_not_redelivered_after_restart():
    """Direct callables should not be re-fired after restart (they can't be)."""
    db = _temp_db()

    calls: list[JobResult] = []

    def direct_cb(result: JobResult):
        calls.append(result)

    h1 = Hoglah(config={"db_path": db})
    job_id = h1.submit(prompt="direct", model="gemma:2b", callback=direct_cb)

    # Manually complete
    fake = JobResult(job_id=job_id, status=JobStatus.COMPLETED, output="x")
    h1._store.set_result(job_id, fake)  # type: ignore[attr-defined]

    # New instance has no knowledge of the direct callable
    Hoglah(config={"db_path": db})

    # The callback should NOT have been called by the new instance
    # (it was only remembered inside h1)
    assert len(calls) == 0  # because we didn't call it ourselves in this test