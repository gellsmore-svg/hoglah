"""MongoJobStore tests (backend='mongo'). Gated like the real-Ollama tests:
they need a running MongoDB and RUN_MONGO_TESTS=1. They use a throwaway
collection (dropped after) on the local server.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

requires_mongo = pytest.mark.skipif(
    os.environ.get("RUN_MONGO_TESTS") != "1",
    reason="Mongo tests require RUN_MONGO_TESTS=1 and a running MongoDB on mongodb://localhost:27017.",
)

MONGO_URI = os.environ.get("HOGLAH_MONGO_URI", "mongodb://localhost:27017")


@requires_mongo
def test_mongo_store_contract():
    from hoglah.models import JobRequest, JobResult, JobStatus
    from hoglah.mongo_store import create_mongo_store

    coll = "hoglah_test_" + uuid.uuid4().hex[:8]
    store = create_mongo_store(MONGO_URI, "hoglah_test", coll)
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
