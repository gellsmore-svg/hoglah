"""G9 — failed-job DLQ view + requeue."""

from __future__ import annotations

import tempfile
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.models import JobRequest, JobResult
from hoglah.store import SQLiteJobStore


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-requeue-")
    return Path(td) / "test.db"


def test_store_requeue_failed_clears_result():
    store = SQLiteJobStore(_temp_db())
    jid = store.enqueue(JobRequest(prompt="x", model="m"))
    store.set_result(
        jid,
        JobResult(job_id=jid, status=JobStatus.FAILED, error="boom", model="m"),
    )
    assert store.get(jid)["status"] == "failed"
    assert store.get(jid)["result"] is not None

    assert store.requeue(jid) is True
    row = store.get(jid)
    assert row["status"] == "queued"
    assert row.get("result") is None
    assert row.get("result_json") is None
    assert row.get("error") is None
    # Second requeue of a queued job fails.
    assert store.requeue(jid) is False
    store.close()


def test_requeue_rejects_completed_by_default():
    store = SQLiteJobStore(_temp_db())
    jid = store.enqueue(JobRequest(prompt="x", model="m"))
    store.set_result(
        jid,
        JobResult(job_id=jid, status=JobStatus.COMPLETED, output="ok", model="m"),
    )
    assert store.requeue(jid) is False
    assert store.get(jid)["status"] == "completed"
    store.close()


def test_client_requeue_and_worker_reruns():
    from hoglah.adapters import StubAdapter

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    jid = h.submit(prompt="retry me", model="m", max_retries=0)
    h._store.set_result(
        jid,
        JobResult(job_id=jid, status=JobStatus.FAILED, error="transient blip", model="m"),
    )
    assert h.status(jid) == JobStatus.FAILED
    assert h.requeue(jid) is True
    assert h.status(jid) == JobStatus.QUEUED
    h.close()

    # Fresh worker completes the requeued job.
    h2 = Hoglah(config={"db_path": db}, adapter=StubAdapter(), start_worker=True)
    try:
        res = h2.wait(jid, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        assert "[STUB]" in (res.output or "")
    finally:
        h2.close()


def test_requeue_failed_bulk():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    ids = []
    for i in range(3):
        jid = h.submit(prompt=f"f{i}", model="m")
        h._store.set_result(
            jid,
            JobResult(job_id=jid, status=JobStatus.FAILED, error=f"e{i}", model="m"),
        )
        ids.append(jid)
    # One completed — must not be bulk-requeued.
    ok = h.submit(prompt="ok", model="m")
    h._store.set_result(
        ok,
        JobResult(job_id=ok, status=JobStatus.COMPLETED, output="y", model="m"),
    )

    requeued = h.requeue_failed(limit=10)
    assert set(requeued) == set(ids)
    assert h.status(ok) == JobStatus.COMPLETED
    for jid in ids:
        assert h.status(jid) == JobStatus.QUEUED
    h.close()


def test_requeue_cancelled_requires_flag():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    jid = h.submit(prompt="c", model="m")
    h.cancel(jid)
    assert h.requeue(jid) is False
    assert h.requeue(jid, allow_cancelled=True) is True
    assert h.status(jid) == JobStatus.QUEUED
    h.close()
