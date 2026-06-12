"""Tests for the background worker + Ollama execution path (Chunk 2).

These use mocks so they run without a real Ollama server.
They are written as plain sync tests (using asyncio.run) to avoid requiring
pytest-asyncio in the minimal dev environment.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.models import JobResult


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-worker-test-")
    return Path(td) / "worker.db"


def test_worker_executes_generate_and_fires_callback():
    """Submit a prompt job; the worker should execute it and fire the callback."""
    db = _temp_db()
    results: list[JobResult] = []

    def cb(res: JobResult):
        results.append(res)

    # Mock the async ollama call
    mock_resp = {
        "response": "Hello from the model!",
        "prompt_eval_count": 12,
        "eval_count": 7,
    }

    async def _run():
        with patch("hoglah.client.ollama.AsyncClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.generate = AsyncMock(return_value=mock_resp)

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

            # Wait for the worker to pick it up and complete (polling)
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
    assert "Hello from the model" in (final.output or "")
    assert final.model == "gemma:2b"
    assert final.usage.get("prompt_tokens") == 12


def test_worker_executes_chat_and_reports_truncation():
    db = _temp_db()

    mock_resp = {
        "message": {"content": "Partial answer because context was tight"},
        "prompt_eval_count": 95,
        "eval_count": 3,
    }

    async def _run():
        with patch("hoglah.client.ollama.AsyncClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.chat = AsyncMock(return_value=mock_resp)

            h = Hoglah(config={"db_path": db}, start_worker=True)

            job_id = h.submit(
                messages=[{"role": "user", "content": "A very long prompt that would exceed context..."}],
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
    assert "Partial answer" in (final.output or "")
    # Heuristic in _call_ollama should have flagged truncation because prompt tokens ~95 vs num_ctx=100
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