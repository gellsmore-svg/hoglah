"""G6 — idempotent submit via idempotency_key."""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.models import JobRequest
from hoglah.store import SQLiteJobStore


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-idemp-")
    return Path(td) / "test.db"


def test_submit_same_idempotency_key_returns_same_job():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    a = h.submit(prompt="once", model="m", idempotency_key="agent-step-7")
    b = h.submit(prompt="different text ignored", model="other", idempotency_key="agent-step-7")
    assert a == b
    assert len(h.list(limit=50)) == 1
    assert h.status(a) == JobStatus.QUEUED
    h.close()


def test_different_keys_create_different_jobs():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    a = h.submit(prompt="a", model="m", idempotency_key="k1")
    b = h.submit(prompt="b", model="m", idempotency_key="k2")
    assert a != b
    assert len(h.list(limit=50)) == 2
    h.close()


def test_blank_idempotency_key_is_ignored():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    a = h.submit(prompt="a", model="m", idempotency_key="  ")
    b = h.submit(prompt="b", model="m", idempotency_key="")
    assert a != b
    h.close()


def test_store_enqueue_idempotency_race():
    """Concurrent enqueues with the same key: exactly one row."""
    store = SQLiteJobStore(_temp_db())
    key = "race-key"
    ids: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        jid = store.enqueue(
            JobRequest(prompt="x", model="m", idempotency_key=key),
            idempotency_key=key,
        )
        with lock:
            ids.append(jid)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(ids)) == 1
    assert len(store.list(limit=100)) == 1
    row = store.find_by_idempotency_key(key)
    assert row is not None
    assert row["id"] == ids[0]
    store.close()


def test_idempotent_hit_registers_direct_callback():
    """A re-submit with the same key re-binds the in-process callback before the
    worker runs (agent crash-retry while the job is still queued)."""
    db = _temp_db()
    seen: list[str] = []
    # start_worker=False so the second submit can register the callback first.
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=False)
    try:
        jid1 = h.submit(prompt="hi", model="m", idempotency_key="cb-key", max_retries=0)
        jid2 = h.submit(
            prompt="hi",
            model="m",
            idempotency_key="cb-key",
            max_retries=0,
            callback=lambda r: seen.append(r.job_id),
        )
        assert jid1 == jid2
        assert jid2 in h._direct_callbacks

        # Now run a worker against the same store to complete the job.
        h2 = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
        try:
            # Callback is process-local — fire via wait + manual delivery is not
            # expected here; just prove the job still completes under the same id.
            res = h2.wait(jid2, timeout=5.0)
            assert res.status == JobStatus.COMPLETED
        finally:
            h2.close()
    finally:
        h.close()
