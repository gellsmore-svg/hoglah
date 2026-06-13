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

from unittest.mock import AsyncMock, MagicMock, patch

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


def test_startup_recovery_is_a_worker_responsibility():
    """Recovery of interrupted (PROCESSING) jobs belongs to the worker (ADR-016).

    A pure submitter (start_worker=False) sharing the queue with a separate
    worker daemon must NOT re-queue PROCESSING jobs, or it would clobber the
    daemon's in-flight work. The worker path still recovers them.
    """
    db = _temp_db()

    h1 = Hoglah(config={"db_path": db}, start_worker=False)
    job_id = h1.submit(prompt="will be interrupted", model="gemma:2b")
    # Simulate the job having been claimed but the process died
    h1._store.update_status(job_id, JobStatus.PROCESSING)

    # Submitter mode: constructing another client must leave it PROCESSING.
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    assert h2.status(job_id) == JobStatus.PROCESSING

    # The worker's recovery routine moves it back to QUEUED.
    h2._recover_interrupted_jobs()
    assert h2.status(job_id) == JobStatus.QUEUED

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
    when a local Ollama instance with at least one small model (e.g. gemma3:1b or phi3:mini) is available.
    It exercises the full worker + OllamaAdapter path, including show_model (for context),
    pull if needed, submit, wait, truncation reporting, and result metadata.
    Also exercises h.show_model and h.pull_model directly.
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
        model = "gemma3:1b"  # change if your Ollama has a different small model
        # Exercise direct show and pull (will pull if missing)
        details = await h.adapter.show_model(model)
        await h.adapter.pull_model(model)
        # Also via client convenience
        details2 = h.show_model(model)
        h.pull_model(model)

        job_id = h.submit(
            prompt="Reply with exactly the word: PONG",
            model=model,
            max_retries=0,
            # Do not specify num_ctx to test auto from model details
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
        return final, details, details2

    final, details, details2 = asyncio.run(_run())

    assert final is not None
    assert final.status == JobStatus.COMPLETED
    # We don't assert exact output because it depends on the actual model,
    # but we can at least ensure we got *something* back.
    assert final.output is not None and len(final.output) > 0

    # Verify model details were retrievable (context calibration)
    assert details is not None
    assert "parameters" in details or "details" in details
    assert details2 is not None

    # If the model provided num_ctx and we didn't specify, effective should be set from it
    if final.effective_num_ctx:
        assert final.effective_num_ctx > 0


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


def test_ollama_adapter_show_model_mocked():
    """OllamaAdapter.show_model uses client.show and returns details."""
    with patch("hoglah.adapters.ollama.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.show = AsyncMock(return_value={"name": "test:1b", "details": {"family": "test"}, "parameters": "num_ctx 4096"})
        mock_client_class.return_value = mock_client

        ad = OllamaAdapter(host="http://example")
        details = asyncio.run(ad.show_model("test:1b"))
        assert details["name"] == "test:1b"
        mock_client.show.assert_called_once_with(model="test:1b")


def test_ollama_adapter_pull_model_mocked():
    """OllamaAdapter.pull_model calls show (if present) or pull."""
    with patch("hoglah.adapters.ollama.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.show = AsyncMock()  # succeeds -> no pull
        mock_client.pull = AsyncMock()
        mock_client_class.return_value = mock_client

        ad = OllamaAdapter()
        asyncio.run(ad.pull_model("test:1b"))
        mock_client.show.assert_called()
        mock_client.pull.assert_not_called()


def test_ollama_adapter_run_uses_model_context_mocked():
    """OllamaAdapter.run calls show_model to get num_ctx and passes to generate."""
    with patch("hoglah.adapters.ollama.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        # show returns model with num_ctx
        mock_client.show = AsyncMock(return_value={"parameters": "num_ctx 8192"})
        # generate returns response
        mock_resp = MagicMock()
        mock_resp.response = "ok"
        mock_resp.prompt_eval_count = 10
        mock_resp.eval_count = 5
        mock_resp.done_reason = "stop"
        mock_client.generate = AsyncMock(return_value=mock_resp)
        mock_client_class.return_value = mock_client

        ad = OllamaAdapter()
        req = JobRequest(prompt="hi", model="test:1b", num_ctx=None)
        output, usage, meta = asyncio.run(ad.run(req))

        assert output == "ok"
        # should have used 8192 from model
        call_kwargs = mock_client.generate.call_args.kwargs
        assert call_kwargs["options"]["num_ctx"] == 8192
        assert meta.get("effective_num_ctx") == 8192
        assert meta.get("done_reason") == "stop"

def test_worker_executes_embedding_job_via_stub():
    """submit_embedding routes through the worker to StubAdapter.embed and the
    JobResult carries a finite vector with embedding_dim set (output is None)."""
    db = _temp_db()

    async def _run():
        h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
        job_id = h.submit_embedding("hello world", model="bge-m3", max_retries=0)

        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            res = h.get(job_id)
            if res.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Worker did not complete the embedding job in time")
            await asyncio.sleep(0.1)
        h.close()
        return res

    res = asyncio.run(_run())
    assert res.status == JobStatus.COMPLETED
    assert res.output is None
    assert isinstance(res.embedding, list) and len(res.embedding) == 8
    assert res.embedding_dim == 8
    assert all(isinstance(x, float) for x in res.embedding)
    assert res.model == "bge-m3"


def test_stub_adapter_embed_is_deterministic():
    """Same input -> same vector; different input -> different vector."""
    ad = StubAdapter()
    v1, _, meta = asyncio.run(ad.embed(JobRequest(kind="embed", prompt="apple", model="bge-m3")))
    v2, _, _ = asyncio.run(ad.embed(JobRequest(kind="embed", prompt="apple", model="bge-m3")))
    v3, _, _ = asyncio.run(ad.embed(JobRequest(kind="embed", prompt="orange", model="bge-m3")))
    assert v1 == v2
    assert v1 != v3
    assert meta["embedding_dim"] == len(v1)


def test_ollama_adapter_embed_mocked():
    """OllamaAdapter.embed pulls the model, calls client.embed, and returns the
    first vector with usage + embedding_dim metadata."""
    with patch("hoglah.adapters.ollama.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.show = AsyncMock()  # model present -> no pull
        mock_resp = MagicMock()
        mock_resp.embeddings = [[0.1, 0.2, 0.3]]
        mock_resp.prompt_eval_count = 4
        mock_client.embed = AsyncMock(return_value=mock_resp)
        mock_client_class.return_value = mock_client

        ad = OllamaAdapter()
        vector, usage, meta = asyncio.run(
            ad.embed(JobRequest(kind="embed", prompt="hi", model="bge-m3"))
        )
        assert vector == [0.1, 0.2, 0.3]
        assert meta["embedding_dim"] == 3
        assert usage["prompt_tokens"] == 4
        assert mock_client.embed.call_args.kwargs["input"] == "hi"


def test_ollama_adapter_embed_rejects_non_finite():
    """A NaN/Inf component raises rather than returning a bogus vector."""
    with patch("hoglah.adapters.ollama.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.show = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.embeddings = [[0.1, float("nan"), 0.3]]
        mock_client.embed = AsyncMock(return_value=mock_resp)
        mock_client_class.return_value = mock_client

        ad = OllamaAdapter()
        with pytest.raises(RuntimeError):
            asyncio.run(ad.embed(JobRequest(kind="embed", prompt="hi", model="bge-m3")))


def test_worker_writes_output_file_when_output_dir_set():
    """With output_dir configured, the worker writes <output_dir>/<job_id>.json
    on completion with the full serialized result; embedding jobs include the
    vector. With output_dir unset, no file is written."""
    import json as _json

    db = _temp_db()
    out_dir = db.parent / "outbox"

    async def _run():
        h = Hoglah(
            config={"db_path": db, "concurrency": 1, "output_dir": out_dir},
            start_worker=True,
        )
        gen_id = h.submit(prompt="Say hi", model="gemma:2b", max_retries=0)
        emb_id = h.submit_embedding("hello", model="bge-m3", max_retries=0)

        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            done = all(
                h.get(j).status in (JobStatus.COMPLETED, JobStatus.FAILED)
                for j in (gen_id, emb_id)
            )
            if done:
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Worker did not complete jobs in time")
            await asyncio.sleep(0.1)
        h.close()
        return gen_id, emb_id

    gen_id, emb_id = asyncio.run(_run())

    # output_dir is auto-created by ensure_dirs()
    assert out_dir.is_dir()
    gen_file = out_dir / f"{gen_id}.json"
    emb_file = out_dir / f"{emb_id}.json"
    assert gen_file.is_file() and emb_file.is_file()
    # no leftover temp files
    assert not list(out_dir.glob("*.tmp"))

    gen_doc = _json.loads(gen_file.read_text())
    assert gen_doc["job_id"] == gen_id
    assert gen_doc["status"] == "completed"
    assert "[STUB]" in (gen_doc["output"] or "")

    emb_doc = _json.loads(emb_file.read_text())
    assert emb_doc["status"] == "completed"
    assert isinstance(emb_doc["embedding"], list) and len(emb_doc["embedding"]) == 8
    assert emb_doc["embedding_dim"] == 8


def test_worker_writes_no_output_file_when_output_dir_unset():
    """Default config (no output_dir) writes no files and stays backward-compatible."""
    db = _temp_db()

    async def _run():
        h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
        job_id = h.submit(prompt="hi", model="gemma:2b", max_retries=0)
        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            if h.get(job_id).status in (JobStatus.COMPLETED, JobStatus.FAILED):
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Worker did not complete the job in time")
            await asyncio.sleep(0.1)
        h.close()
        return job_id

    asyncio.run(_run())
    # No sibling outbox dir created next to the db
    assert not (db.parent / "outbox").exists()


def test_worker_posts_result_to_callback_url():
    """With a per-job callback_url, the worker POSTs the terminal JobResult JSON
    to that endpoint (proving the outbound 'push' path, ADR-015)."""
    import json as _json
    import threading as _threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    received: list[dict] = []
    got = _threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            received.append(_json.loads(body.decode("utf-8")))
            self.send_response(200)
            self.end_headers()
            got.set()

        def log_message(self, *args):  # silence test server logging
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    server_thread = _threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    db = _temp_db()
    try:
        async def _run():
            h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
            job_id = h.submit(
                prompt="ping",
                model="gemma:2b",
                callback_url=f"http://127.0.0.1:{port}/hook",
                max_retries=0,
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while h.get(job_id).status not in (JobStatus.COMPLETED, JobStatus.FAILED):
                if asyncio.get_event_loop().time() > deadline:
                    pytest.fail("Worker did not complete the job in time")
                await asyncio.sleep(0.1)
            h.close()
            return job_id

        job_id = asyncio.run(_run())
        assert got.wait(timeout=5.0), "callback endpoint never received a POST"
        assert len(received) == 1
        doc = received[0]
        assert doc["job_id"] == job_id
        assert doc["status"] == "completed"
        assert "[STUB]" in (doc["output"] or "")
    finally:
        server.shutdown()


def test_post_callback_gives_up_after_retries(caplog):
    """A persistently failing endpoint is retried then abandoned with a warning;
    the failure never raises into the worker."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "callback_max_retries": 2}, start_worker=False)
    try:
        with patch("hoglah.client.urllib.request.urlopen", side_effect=OSError("refused")):
            with patch("hoglah.client.time.sleep"):  # no real backoff in tests
                h._post_callback("http://127.0.0.1:9/hook", "job-123", "{}")
        assert any("Callback POST" in r.message for r in caplog.records)
    finally:
        h.close()
