"""Tests for the background worker + execution path (Chunk 2).

Real Ollama calls are currently paused. These tests exercise the worker,
claiming, retries, callbacks, truncation reporting, and recovery using the
default StubAdapter (which never touches the Ollama server).

Written as plain sync tests (asyncio.run inside) to avoid extra pytest plugins.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.models import JobResult


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-worker-test-")
    return Path(td) / "worker.db"


def test_worker_executes_generate_and_fires_callback():
    """Submit a prompt job; the worker (using StubAdapter) should process it and fire the callback."""
    db = _temp_db()
    results: list[JobResult] = []

    def cb(res: JobResult):
        results.append(res)

    async def _run():
        h = Hoglah(
            config={"db_path": db, "concurrency": 1},
            callbacks={"cb": cb},
            start_worker=True,
        )

        job_id = h.submit(
            prompt="Say hello",
            model="gemma:2b",
            callback="cb",
            max_retries=0,
        )

        # Wait for the worker to pick it up and complete
        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            res = h.get(job_id)
            if res.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Worker did not complete the job in time")
            await asyncio.sleep(0.1)

        h.close()
        return results, job_id

    results, job_id = asyncio.run(_run())

    assert len(results) == 1
    final = results[0]
    assert final.job_id == job_id
    assert final.status == JobStatus.COMPLETED
    # StubAdapter output format
    assert "[STUB]" in (final.output or "")
    assert "Say hello" in (final.output or "")
    assert final.model == "gemma:2b"


def test_worker_executes_chat_and_reports_truncation():
    """Chat-style request that should trigger the stub's truncation simulation."""
    db = _temp_db()

    async def _run():
        h = Hoglah(config={"db_path": db}, start_worker=True)

        job_id = h.submit(
            messages=[{"role": "user", "content": "word " * 120}],  # ~120 tokens, > 0.9 * 100
            model="gemma:2b",
            num_ctx=100,
            max_retries=0,
        )

        deadline = asyncio.get_event_loop().time() + 5.0
        final = None
        while True:
            res = h.get(job_id)
            if res.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                final = res
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Worker did not finish")
            await asyncio.sleep(0.1)

        h.close()
        return final

    final = asyncio.run(_run())

    assert final is not None
    assert final.status == JobStatus.COMPLETED
    assert "[STUB-CHAT]" in (final.output or "")
    assert final.truncated is True
    assert final.truncation_reason is not None


def test_startup_recovery_marks_interrupted_as_queued():
    """If jobs are left in PROCESSING, the next Hoglah() should recover them to QUEUED."""
    db = _temp_db()

    h1 = Hoglah(config={"db_path": db}, start_worker=False)
    job_id = h1.submit(prompt="will be interrupted", model="gemma:2b")

    # Simulate the job having been claimed but the process died
    h1._store.update_status(job_id, JobStatus.PROCESSING)

    # New instance should recover it
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    status = h2.status(job_id)
    assert status == JobStatus.QUEUED

    h1.close()
    h2.close()