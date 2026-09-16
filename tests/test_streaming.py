"""G8 token streaming: chunk files, store-stays-final, SSE."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.adapters import OllamaAdapter, StubAdapter
from hoglah.http_adapter import OpenAICompatAdapter
from hoglah.models import JobRequest
from hoglah.streaming import (
    ChunkWriter,
    follow_job_events,
    safe_job_id,
    stream_path,
)


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-stream-")
    return Path(td) / "test.db"


def test_safe_job_id_rejects_traversal():
    with pytest.raises(ValueError):
        safe_job_id("../etc/passwd")
    with pytest.raises(ValueError):
        safe_job_id("a/b")
    with pytest.raises(ValueError):
        safe_job_id("")
    assert safe_job_id("abc-def_01") == "abc-def_01"


def test_chunk_writer_and_retry_truncate(tmp_path):
    path = tmp_path / "job.ndjson"
    w = ChunkWriter(path)
    w.open()
    w.write_chunk("Hel")
    w.write_chunk("lo")
    w.reset()
    w.write_chunk("Hi")
    w.finish("completed")
    lines = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
    assert lines[0] == {"i": 0, "text": "Hi"}
    assert lines[-1]["done"] is True
    assert lines[-1]["status"] == "completed"
    w.finish("failed")  # idempotent
    lines2 = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
    assert lines2[-1]["status"] == "completed"


def test_worker_writes_chunks_store_stays_final():
    db = _temp_db()
    stream_dir = db.parent / "streams"

    class Slow(StubAdapter):
        async def run(self, request, *, on_chunk=None):
            if on_chunk:
                on_chunk("Hel")
            await asyncio.sleep(0.15)
            if on_chunk:
                on_chunk("lo")
            return "Hello", {"prompt_tokens": 1, "completion_tokens": 1, "total": 2}, {}

    async def _run():
        h = Hoglah(
            config={"db_path": db, "concurrency": 1, "stream_dir": stream_dir},
            adapter=Slow(),
            start_worker=True,
        )
        jid = h.submit(prompt="x", model="stub:1", max_retries=0)
        saw_processing_without_output = False
        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            row = h._store.get(jid)
            status = row["status"] if row else None
            result = (row or {}).get("result")
            if status == "processing":
                assert not result or not result.get("output")
                path = stream_path(stream_dir, jid)
                if path.is_file() and "Hel" in path.read_text():
                    saw_processing_without_output = True
            if status in ("completed", "failed"):
                break
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("job did not finish")
            await asyncio.sleep(0.02)
        res = h.get(jid)
        h.close()
        return jid, res, saw_processing_without_output

    jid, res, saw = asyncio.run(_run())
    assert res.status == JobStatus.COMPLETED
    assert res.output == "Hello"
    path = stream_path(stream_dir, jid)
    text = path.read_text()
    assert '"text": "Hel"' in text
    assert '"text": "lo"' in text
    assert '"done": true' in text
    assert saw is True


def test_default_off_writes_no_stream_file():
    db = _temp_db()

    async def _run():
        h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
        jid = h.submit(prompt="hi", model="stub:1", max_retries=0)
        h.wait(jid, timeout=10)
        h.close()
        return jid

    jid = asyncio.run(_run())
    assert not (db.parent / "streams" / f"{jid}.ndjson").exists()
    assert jid


def test_embed_does_not_write_stream():
    db = _temp_db()
    stream_dir = db.parent / "streams"

    async def _run():
        h = Hoglah(
            config={"db_path": db, "concurrency": 1, "stream_dir": stream_dir},
            start_worker=True,
        )
        jid = h.submit_embedding("hello", model="stub-emb", max_retries=0)
        h.wait(jid, timeout=10)
        h.close()
        return jid

    jid = asyncio.run(_run())
    assert not stream_path(stream_dir, jid).exists()


def test_follow_synthesises_from_store_when_no_file():
    class Job:
        status = JobStatus.COMPLETED
        output = "full text"
        error = None

    async def _run():
        events = []
        async for ev in follow_job_events(
            job_id="abc-def_01",
            stream_dir=None,
            get_job=lambda _i: Job(),
        ):
            events.append(ev)
        return events

    events = asyncio.run(_run())
    assert events[0]["type"] == "chunk"
    assert events[0]["text"] == "full text"
    assert events[-1]["type"] == "done"
    assert events[-1]["status"] == "completed"


def test_sse_replays_completed_job():
    pytest.importorskip("fastapi", reason="web extra not installed")
    from fastapi.testclient import TestClient

    from hoglah.web import create_app

    db = _temp_db()
    stream_dir = db.parent / "streams"
    h = Hoglah(
        config={"db_path": db, "concurrency": 1, "stream_dir": stream_dir},
        start_worker=True,
    )
    jid = h.submit(prompt="hello world", model="stub:1", max_retries=0)
    h.wait(jid, timeout=10)
    h.close()

    with TestClient(create_app(db_path=db, stream_dir=stream_dir)) as client:
        r = client.get(f"/api/jobs/{jid}/stream")
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]
        assert "data:" in r.text
        assert "done" in r.text
        page = client.get(f"/jobs/{jid}")
        assert page.status_code == 200


def test_sse_unknown_and_unsafe_job():
    pytest.importorskip("fastapi", reason="web extra not installed")
    from fastapi.testclient import TestClient

    from hoglah.web import create_app

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    h.close()
    with TestClient(create_app(db_path=db)) as client:
        assert client.get("/api/jobs/nope-nope/stream").status_code == 404
        assert client.get("/api/jobs/../etc/passwd/stream").status_code == 404


def test_stream_enabled_defaults_dir(tmp_path):
    from hoglah.config import HoglahSettings

    s = HoglahSettings(db_path=tmp_path / "x.db", stream_enabled=True)
    assert s.stream_dir is not None
    assert s.stream_dir.name == "streams"


def test_openai_sse_chunks():
    adapter = OpenAICompatAdapter(base_url="http://localhost:8000/v1")
    chunks: list[str] = []

    class _Stream:
        def __init__(self):
            self._lines = [
                b'data: {"choices":[{"delta":{"content":"Hel"}}]}\n',
                b'data: {"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}\n',
                b"data: [DONE]\n",
            ]
            self._i = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self._i >= len(self._lines):
                raise StopIteration
            line = self._lines[self._i]
            self._i += 1
            return line

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    with patch("hoglah.http_adapter.urllib.request.urlopen", lambda req, timeout=None: _Stream()):
        out, usage, meta = asyncio.run(
            adapter.run(JobRequest(prompt="ping", model="gpt-4o"), on_chunk=chunks.append)
        )
    assert out == "Hello"
    assert chunks == ["Hel", "lo"]
    assert meta.get("stream") is True
    assert meta.get("done_reason") == "stop"


def test_ollama_stream_chunks():
    parts = [
        MagicMock(response="Hel", prompt_eval_count=0, eval_count=0, done_reason=None),
        MagicMock(response="lo", prompt_eval_count=2, eval_count=1, done_reason="stop"),
    ]

    async def _gen():
        for p in parts:
            yield p

    mock_client = MagicMock()
    mock_client.generate = AsyncMock(return_value=_gen())
    mock_client.show = AsyncMock(return_value={"parameters": "num_ctx 4096"})
    chunks: list[str] = []

    ad = OllamaAdapter()
    ad._pulled.add("m")
    ad._show_cache["m"] = {"parameters": "num_ctx 4096"}
    ad._get_client = lambda: mock_client  # type: ignore[method-assign]
    out, usage, meta = asyncio.run(
        ad.run(JobRequest(prompt="x", model="m"), on_chunk=chunks.append)
    )
    assert out == "Hello"
    assert chunks == ["Hel", "lo"]
    assert usage["completion_tokens"] == 1
    assert meta.get("done_reason") == "stop"


def test_retry_truncates_stream_file():
    db = _temp_db()
    stream_dir = db.parent / "streams"
    hits = {"n": 0}

    class Flaky(StubAdapter):
        async def run(self, request, *, on_chunk=None):
            hits["n"] += 1
            if on_chunk:
                on_chunk(f"try{hits['n']}")
            if hits["n"] == 1:
                raise ConnectionError("blip")
            return "ok", {"prompt_tokens": 1, "completion_tokens": 1, "total": 2}, {}

    async def _run():
        h = Hoglah(
            config={"db_path": db, "concurrency": 1, "stream_dir": stream_dir},
            adapter=Flaky(),
            start_worker=True,
        )
        jid = h.submit(
            prompt="x",
            model="stub:1",
            max_retries=2,
            retry_policy={"max_retries": 1, "base_delay": 0.01, "retry_on": ["transient"]},
        )
        h.wait(jid, timeout=10)
        h.close()
        return jid

    jid = asyncio.run(_run())
    text = stream_path(stream_dir, jid).read_text()
    assert "try2" in text
    assert "try1" not in text
