"""Tests for the background worker + execution path (Chunk 2).

Real Ollama calls are currently paused. These tests exercise the worker,
claiming, retries, callbacks, truncation reporting, and recovery using the
default StubAdapter (which never touches the Ollama server).

Written as plain sync tests (asyncio.run inside) to avoid extra pytest plugins.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.adapters import BaseAdapter, OllamaAdapter, StubAdapter
from hoglah.models import JobRequest, JobResult

# Marker for real Ollama integration tests.
# Run with: RUN_OLLAMA_TESTS=1 pytest -m ollama  (or just the tests when server is up)
requires_ollama = pytest.mark.skipif(
    os.environ.get("RUN_OLLAMA_TESTS") != "1",
    reason="Real Ollama integration tests require RUN_OLLAMA_TESTS=1 and a running Ollama server reachable on the configured host."
)


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


# ------------------------------------------------------------------ #
# Adapter tests (Chunk 3)
# ------------------------------------------------------------------ #


def test_stub_adapter_run_and_list_models():
    """StubAdapter fulfills the contract and provides canned models."""
    import asyncio

    async def _go():
        ad = StubAdapter()
        req = JobRequest(prompt="hello stub", model="x", num_ctx=128)
        out, usage, meta = await ad.run(req)
        assert "STUB" in out
        assert "prompt_tokens" in usage
        assert isinstance(meta, dict)

        models = await ad.list_models()
        assert len(models) >= 2
        assert "stub-model" in models[0]["name"]

    asyncio.run(_go())


def test_ollama_adapter_instantiable_and_has_list_models():
    """OllamaAdapter can be constructed (real calls only happen on use)."""
    ad = OllamaAdapter(host="http://127.0.0.1:11434")
    assert isinstance(ad, BaseAdapter)
    # list_models will be exercised only if server present; do not call here
    # to keep test hermetic.
    # We can at least confirm the method exists
    assert hasattr(ad, "list_models")
    assert hasattr(ad, "run")


@requires_ollama
def test_real_ollama_adapter_end_to_end():
    """Basic execution path against a real Ollama server.

    This test is skipped by default. Enable with RUN_OLLAMA_TESTS=1
    when a local Ollama instance with at least one model (e.g. gemma3:1b or tiny) is available.
    It exercises the full worker + OllamaAdapter path.
    """
    db = _temp_db()

    async def _run():
        # Use real adapter explicitly (or rely on env)
        h = Hoglah(
            config={"db_path": db},
            use_real=True,
            start_worker=True,
        )

        # Use a very small prompt; model name is up to the tester's Ollama
        # We pick a common small one; if not present the call will fail (acceptable for this gated test)
        job_id = h.submit(
            prompt="Reply with exactly the word: PONG",
            model="gemma3:1b",  # change if your Ollama has a different small model
            max_retries=0,
        )

        deadline = asyncio.get_event_loop().time() + 60.0
        final = None
        while True:
            res = h.get(job_id)
            if res.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                final = res
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Real Ollama job did not complete in time")
            await asyncio.sleep(0.2)

        h.close()
        return final

    final = asyncio.run(_run())

    assert final is not None
    assert final.status == JobStatus.COMPLETED
    # We don't assert exact output because it depends on the actual model,
    # but we can at least ensure we got *something* back.
    assert final.output is not None and len(final.output) > 0


def test_ollama_adapter_build_options_maps_params():
    """Verify that generation params and options dict are merged correctly for the real adapter."""
    ad = OllamaAdapter()

    req = JobRequest(
        prompt="x",
        model="test",
        temperature=0.7,
        top_p=0.9,
        num_ctx=8192,
        seed=42,
        num_predict=128,
        options={"foo": "bar", "temperature": 0.1},  # explicit options should be base, then overrides
    )

    opts = ad._build_options(req)  # test the internal builder (safe, no network)

    # options dict provides starting point
    assert opts.get("foo") == "bar"
    # scalar fields on the request take precedence / are applied
    assert opts["temperature"] == 0.7
    assert opts["top_p"] == 0.9
    assert opts["num_ctx"] == 8192
    assert opts["seed"] == 42
    assert opts["num_predict"] == 128