"""MongoJobStore tests (backend='mongo'). Gated like the real-Ollama tests:
they need a running MongoDB and RUN_MONGO_TESTS=1. They use a throwaway
collection (dropped after) on the local server.

Coverage mirrors the SQLite-backed client/worker contract so the Mongo drop-in
is exercised on the same behaviours: filters (status/tags/parent), priority
ordering, offset/limit, the COALESCE semantics of update_status(error=None),
delete_jobs(before=), the embedding round-trip, and a genuinely concurrent
(multi-thread) claim race.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

requires_mongo = pytest.mark.skipif(
    os.environ.get("RUN_MONGO_TESTS") != "1",
    reason="Mongo tests require RUN_MONGO_TESTS=1 and a running MongoDB on mongodb://localhost:27017.",
)

MONGO_URI = os.environ.get("HOGLAH_MONGO_URI", "mongodb://localhost:27017")


def _fresh_store():
    """A MongoJobStore on a unique throwaway collection. Caller drops + closes."""
    from hoglah.mongo_store import create_mongo_store

    coll = "hoglah_test_" + uuid.uuid4().hex[:8]
    return create_mongo_store(MONGO_URI, "hoglah_test", coll)


@requires_mongo
def test_mongo_store_contract():
    from hoglah.models import JobRequest, JobResult, JobStatus

    store = _fresh_store()
    try:
        jid = store.enqueue(JobRequest(prompt="hi", model="m", tags=["t"]))
        row = store.get(jid)
        assert row["id"] == jid
        assert row["status"] == "queued"
        assert row["request"]["model"] == "m"          # native sub-document
        assert row["result"] is None and row["result_json"] is None
        assert row["tags"] == ["t"]

        # filters
        assert any(r["id"] == jid for r in store.list(status=JobStatus.QUEUED))
        assert all(r["id"] != jid for r in store.list(status=JobStatus.COMPLETED))

        # atomic claim: first wins, second fails (single-execution guarantee)
        assert store.claim_for_processing(jid) is True
        assert store.claim_for_processing(jid) is False

        # set_result mirrors SQLite shape (parsed + *_json)
        store.set_result(jid, JobResult(job_id=jid, status=JobStatus.COMPLETED, output="ok", model="m"))
        row = store.get(jid)
        assert row["status"] == "completed"
        assert row["result"]["output"] == "ok"
        assert row["result_json"]  # truthy JSON string (client reads this)

        assert store.get_status_counts().get("completed", 0) >= 1
        assert store.delete_job(jid) is True
        assert store.get(jid) is None
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_mongo_list_filters_ordering_and_paging():
    from hoglah.models import JobRequest, JobStatus

    store = _fresh_store()
    try:
        # priority 0 (low) and 9 (high); high must sort first.
        low = store.enqueue(JobRequest(prompt="low", model="m", priority=0, tags=["a"]))
        high = store.enqueue(JobRequest(prompt="high", model="m", priority=9, tags=["a", "b"]))
        child = store.enqueue(JobRequest(prompt="child", model="m", parent_job_id=low, tags=["c"]))

        # priority ordering (priority desc, created_at asc)
        ids = [r["id"] for r in store.list(status=JobStatus.QUEUED)]
        assert ids.index(high) < ids.index(low)

        # tag filter ($all): both a and b -> only `high`
        assert [r["id"] for r in store.list(tags=["a", "b"])] == [high]
        # tag a -> low + high
        assert {r["id"] for r in store.list(tags=["a"])} == {low, high}

        # parent filter
        assert [r["id"] for r in store.list(parent_job_id=low)] == [child]

        # offset/limit paging over the full queued set (3 jobs)
        page1 = store.list(limit=2, offset=0)
        page2 = store.list(limit=2, offset=2)
        assert len(page1) == 2 and len(page2) == 1
        assert {r["id"] for r in page1} | {r["id"] for r in page2} == {low, high, child}
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_mongo_update_status_error_coalesce():
    """update_status(error=None) must NOT clobber a previously-set error
    (COALESCE semantics, matching SQLiteJobStore)."""
    from hoglah.models import JobRequest, JobStatus

    store = _fresh_store()
    try:
        jid = store.enqueue(JobRequest(prompt="x", model="m"))
        store.update_status(jid, JobStatus.PROCESSING, error="boom")
        assert store.get(jid)["error"] == "boom"
        # a later status change without an error must preserve "boom"
        store.update_status(jid, JobStatus.QUEUED)
        assert store.get(jid)["error"] == "boom"
        # an explicit new error overwrites
        store.update_status(jid, JobStatus.FAILED, error="boom2")
        assert store.get(jid)["error"] == "boom2"
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_mongo_delete_jobs_by_status_and_before():
    from hoglah.models import JobRequest, JobResult, JobStatus

    store = _fresh_store()
    try:
        done = store.enqueue(JobRequest(prompt="done", model="m"))
        store.set_result(done, JobResult(job_id=done, status=JobStatus.COMPLETED, output="ok", model="m"))
        store.enqueue(JobRequest(prompt="keep", model="m"))  # stays QUEUED

        # delete only COMPLETED -> removes 1, leaves the queued one
        assert store.delete_jobs(status=JobStatus.COMPLETED) == 1
        assert store.get(done) is None
        assert store.get_status_counts().get("queued", 0) == 1

        # delete_jobs(before=<future>) removes everything updated before then
        future = "2999-01-01T00:00:00+00:00"
        assert store.delete_jobs(before=future) == 1
        assert store.get_status_counts() == {}
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_mongo_embedding_round_trip():
    """Embedding results carry a float vector through Mongo intact (BSON + the
    re-synthesized result_json the client reads)."""
    import json

    from hoglah.models import JobRequest, JobResult, JobStatus

    store = _fresh_store()
    try:
        jid = store.enqueue(JobRequest(kind="embed", prompt="vectorize me", model="bge-m3"))
        vec = [0.1, -0.2, 0.3, 0.4]
        store.set_result(
            jid,
            JobResult(
                job_id=jid,
                status=JobStatus.COMPLETED,
                model="bge-m3",
                embedding=vec,
                embedding_dim=len(vec),
            ),
        )
        row = store.get(jid)
        assert row["result"]["embedding"] == vec
        assert row["result"]["embedding_dim"] == 4
        assert row["result"]["output"] is None
        # result_json (what the client serializes/reads) must also round-trip
        assert json.loads(row["result_json"])["embedding"] == vec
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_mongo_concurrent_claim_exactly_once():
    """Many threads racing to claim the SAME job: exactly one wins. This is the
    property that lets multiple workers (even cross-machine) share one queue."""
    from hoglah.models import JobRequest

    store = _fresh_store()
    try:
        jid = store.enqueue(JobRequest(prompt="contended", model="m"))

        n = 16
        results: list[bool] = []
        lock = threading.Lock()
        barrier = threading.Barrier(n)

        def worker():
            barrier.wait()  # release all threads at once to maximise contention
            won = store.claim_for_processing(jid)
            with lock:
                results.append(won)

        threads = [threading.Thread(target=worker) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(results) == 1, f"expected exactly one winner, got {sum(results)}"
        assert store.get(jid)["status"] == "processing"
    finally:
        store._col.drop()
        store.close()


@requires_mongo
def test_hoglah_end_to_end_on_mongo():
    """Full client on the Mongo backend (StubAdapter — no Ollama needed)."""
    from hoglah import Hoglah, JobStatus

    coll = "hoglah_e2e_" + uuid.uuid4().hex[:8]
    h = Hoglah(
        config={"backend": "mongo", "mongo_uri": MONGO_URI, "mongo_db": "hoglah_test", "mongo_collection": coll},
        start_worker=True,
    )
    try:
        jid = h.submit(prompt="hi", model="stub")
        deadline = time.time() + 10
        while h.get(jid).status not in (JobStatus.COMPLETED, JobStatus.FAILED):
            if time.time() > deadline:
                raise AssertionError("job never completed on mongo backend")
            time.sleep(0.1)
        assert h.get(jid).status == JobStatus.COMPLETED
    finally:
        h._store._col.drop()
        h.close()
